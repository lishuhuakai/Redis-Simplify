#include "rio.h"
#include "util.h"
#include "db.h"
#include "object.h"
#include "t_string.h"
#include "t_zset.h"
#include "t_list.h"
#include "t_hash.h"
#include "t_set.h"
#include "redis.h"
#include "ziplist.h"
#include "intset.h"
#include "networking.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include "aof.h"
#include "rdb.h"

/*============================ Variable and Function Declaration ======================== */
extern struct redisServer server;
void expireCommand(redisClient *c);
void pexpireCommand(redisClient *c);
void setexCommand(redisClient *c);
void psetexCommand(redisClient *c);
void decrRefCountVoid(void *o);
void *dupClientReplyValue(void *o);
/* 
* 将 obj 所指向的整数对象或字符串对象的值写入到 r 当中。
*/
int rioWriteBulkObject(rio *r, robj *obj) {
	/* Avoid using getDecodedObject to help copy-on-write (we are often
	* in a child process when this function is called). */
	if (obj->encoding == REDIS_ENCODING_INT) {
		return rioWriteBulkLongLong(r, (long)obj->ptr);
	}
	else if (sdsEncodedObject(obj)) {
		return rioWriteBulkString(r, obj->ptr, sdslen(obj->ptr));
	}
	else {
		mylog("%s", "Unknown string encoding");
	}
}

/* 
* 将重建列表对象所需的命令写入到 r 。
*
* 出错返回 0 ，成功返回 1 。
*
* 命令的形式如下：  RPUSH item1 item2 ... itemN
*/
int rewriteListObject(rio *r, robj *key, robj *o) {
	long long count = 0, items = listTypeLength(o);

	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *zl = o->ptr;
		unsigned char *p = ziplistIndex(zl, 0);
		unsigned char *vstr;
		unsigned int vlen;
		long long vlong;

		/* 先构建一个 RPUSH key 
		 * 然后从 ZIPLIST 中取出最多 REDIS_AOF_REWRITE_ITEMS_PER_CMD 个元素
		 * 之后重复第一步，直到 ZIPLIST 为空 */
		while (ziplistGet(p, &vstr, &vlen, &vlong)) {
			if (count == 0) {
				int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
					REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

				if (rioWriteBulkCount(r, '*', 2 + cmd_items) == 0) return 0;
				if (rioWriteBulkString(r, "RPUSH", 5) == 0) return 0;
				if (rioWriteBulkObject(r, key) == 0) return 0;
			}
			if (vstr) { /* 取出值 */
				if (rioWriteBulkString(r, (char*)vstr, vlen) == 0) return 0;
			}
			else {
				if (rioWriteBulkLongLong(r, vlong) == 0) return 0;
			}
			/* 移动指针，并计算被取出元素的数量 */
			p = ziplistNext(zl, p);
			if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
			items--;
		}
	}
	else if (o->encoding == REDIS_ENCODING_LINKEDLIST) { /* 双向链表 */
		list *list = o->ptr;
		listNode *ln;
		listIter li;

		/* 先构建一个 RPUSH key 
		 * 然后从双端链表中取出最多 REDIS_AOF_REWRITE_ITEMS_PER_CMD 个元素
		 * 之后重复第一步，直到链表为空 */
		listRewind(list, &li);
		while ((ln = listNext(&li))) {
			robj *eleobj = listNodeValue(ln);

			if (count == 0) {
				int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
					REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

				if (rioWriteBulkCount(r, '*', 2 + cmd_items) == 0) return 0;
				if (rioWriteBulkString(r, "RPUSH", 5) == 0) return 0;
				if (rioWriteBulkObject(r, key) == 0) return 0;
			}

			/* 取出值 */
			if (rioWriteBulkObject(r, eleobj) == 0) return 0;

			/* 元素计数 */
			if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;

			items--;
		}
	}
	else {
		mylog("%s", "Unknown list encoding");
	}
	return 1;
}

/*
* 将重建集合对象所需的命令写入到 r 。
*
* 出错返回 0 ，成功返回 1 。
*
* 命令的形式如下：  SADD item1 item2 ... itemN
*/
int rewriteSetObject(rio *r, robj *key, robj *o) {
	long long count = 0, items = setTypeSize(o);

	if (o->encoding == REDIS_ENCODING_INTSET) { /* 底层由 intset */
		int ii = 0;
		int64_t llval;

		while (intsetGet(o->ptr, ii++, &llval)) {
			if (count == 0) {
				int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
					REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

				if (rioWriteBulkCount(r, '*', 2 + cmd_items) == 0) return 0;
				if (rioWriteBulkString(r, "SADD", 4) == 0) return 0;
				if (rioWriteBulkObject(r, key) == 0) return 0;
			}
			if (rioWriteBulkLongLong(r, llval) == 0) return 0;
			if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
			items--;
		}
	}
	else if (o->encoding == REDIS_ENCODING_HT) {
		dictIterator *di = dictGetIterator(o->ptr);
		dictEntry *de;

		while ((de = dictNext(di)) != NULL) {
			robj *eleobj = dictGetKey(de);
			if (count == 0) {
				int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
					REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

				if (rioWriteBulkCount(r, '*', 2 + cmd_items) == 0) return 0;
				if (rioWriteBulkString(r, "SADD", 4) == 0) return 0;
				if (rioWriteBulkObject(r, key) == 0) return 0;
			}
			if (rioWriteBulkObject(r, eleobj) == 0) return 0;
			if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
			items--;
		}
		dictReleaseIterator(di);
	}
	else {
		mylog("%s", "Unknown set encoding");
	}
	return 1;
}

/* 
* 将重建有序集合对象所需的命令写入到 r 。
*
* 出错返回 0 ，成功返回 1 。
*
* 命令的形式如下：  ZADD score1 member1 score2 member2 ... scoreN memberN
*/
int rewriteSortedSetObject(rio *r, robj *key, robj *o) {
	long long count = 0, items = zsetLength(o);

	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *zl = o->ptr;
		unsigned char *eptr, *sptr;
		unsigned char *vstr;
		unsigned int vlen;
		long long vll;
		double score;

		eptr = ziplistIndex(zl, 0);
		assert(eptr != NULL);
		sptr = ziplistNext(zl, eptr);
		assert(sptr != NULL);

		while (eptr != NULL) {
			assert(ziplistGet(eptr, &vstr, &vlen, &vll));
			score = zzlGetScore(sptr);

			if (count == 0) {
				int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
					REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

				if (rioWriteBulkCount(r, '*', 2 + cmd_items * 2) == 0) return 0;
				if (rioWriteBulkString(r, "ZADD", 4) == 0) return 0;
				if (rioWriteBulkObject(r, key) == 0) return 0;
			}
			if (rioWriteBulkDouble(r, score) == 0) return 0;
			if (vstr != NULL) {
				if (rioWriteBulkString(r, (char*)vstr, vlen) == 0) return 0;
			}
			else {
				if (rioWriteBulkLongLong(r, vll) == 0) return 0;
			}
			zzlNext(zl, &eptr, &sptr);
			if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
			items--;
		}
	}
	else if (o->encoding == REDIS_ENCODING_SKIPLIST) {
		zset *zs = o->ptr;
		dictIterator *di = dictGetIterator(zs->dict);
		dictEntry *de;

		while ((de = dictNext(di)) != NULL) {
			robj *eleobj = dictGetKey(de);
			double *score = dictGetVal(de);

			if (count == 0) {
				int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
					REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

				if (rioWriteBulkCount(r, '*', 2 + cmd_items * 2) == 0) return 0;
				if (rioWriteBulkString(r, "ZADD", 4) == 0) return 0;
				if (rioWriteBulkObject(r, key) == 0) return 0;
			}
			if (rioWriteBulkDouble(r, *score) == 0) return 0;
			if (rioWriteBulkObject(r, eleobj) == 0) return 0;
			if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
			items--;
		}
		dictReleaseIterator(di);
	}
	else {
		mylog("%s", "Unknown sorted zset encoding");
	}
	return 1;
}


/* 
* 选择写入哈希的 key 或者 value 到 r 中。
*
* hi 为 Redis 哈希迭代器
*
* what 决定了要写入的部分，可以是 REDIS_HASH_KEY 或 REDIS_HASH_VALUE
*
* 出错返回 0 ，成功返回非 0 。
*/
static int rioWriteHashIteratorCursor(rio *r, hashTypeIterator *hi, int what) {

	if (hi->encoding == REDIS_ENCODING_ZIPLIST) { /* 压缩链表 */
		unsigned char *vstr = NULL;
		unsigned int vlen = UINT_MAX;
		long long vll = LLONG_MAX;

		hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
		if (vstr) {
			return rioWriteBulkString(r, (char*)vstr, vlen);
		}
		else {
			return rioWriteBulkLongLong(r, vll);
		}

	}
	else if (hi->encoding == REDIS_ENCODING_HT) { /* 哈希表 */
		robj *value;

		hashTypeCurrentFromHashTable(hi, what, &value);
		return rioWriteBulkObject(r, value);
	}

	mylog("%s", "Unknown hash encoding");
	return 0;
}

/* 
* 将重建哈希对象所需的命令写入到 r 。
*
* 出错返回 0 ，成功返回 1 。
*
* 命令的形式如下：HMSET field1 value1 field2 value2 ... fieldN valueN
*/
int rewriteHashObject(rio *r, robj *key, robj *o) {
	hashTypeIterator *hi;
	long long count = 0, items = hashTypeLength(o);

	hi = hashTypeInitIterator(o);
	while (hashTypeNext(hi) != REDIS_ERR) {
		if (count == 0) {
			int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
				REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

			if (rioWriteBulkCount(r, '*', 2 + cmd_items * 2) == 0) return 0;
			if (rioWriteBulkString(r, "HMSET", 5) == 0) return 0;
			if (rioWriteBulkObject(r, key) == 0) return 0;
		}

		if (rioWriteHashIteratorCursor(r, hi, REDIS_HASH_KEY) == 0) return 0;
		if (rioWriteHashIteratorCursor(r, hi, REDIS_HASH_VALUE) == 0) return 0;
		if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
		items--;
	}

	hashTypeReleaseIterator(hi);
	return 1;
}



/* 
* 将一切足以还原当前数据集的命令写入到 filename 指定的文件中。
*
* 这个函数被 REWRITEAOF 和 BGREWRITEAOF 两个命令调用。
* （REWRITEAOF 似乎已经是一个废弃的命令）
*
* 为了最小化重建数据集所需执行的命令数量，
* Redis 会尽可能地使用接受可变参数数量的命令，比如 RPUSH 、SADD 和 ZADD 等。
*
* 不过单个命令每次处理的元素数量不能超过 REDIS_AOF_REWRITE_ITEMS_PER_CMD 。
*/
int rewriteAppendOnlyFile(char *filename) { 
	/* 这里指的是一切,也就是要将数据库里的东西全部写一遍 */
	dictIterator *di = NULL;
	dictEntry *de;
	rio aof;
	FILE *fp;
	char tmpfile[256];
	int j;
	long long now = mstime();

	/* 
	* 创建临时文件
	*
	* 注意这里创建的文件名和 rewriteAppendOnlyFileBackground() 创建的文件名稍有不同
	*/
	snprintf(tmpfile, 256, "temp-rewriteaof-%d.aof", (int)getpid());
	fp = fopen(tmpfile, "w");
	if (!fp) {
		mylog("Opening the temp file for AOF rewrite in rewriteAppendOnlyFile(): %s", strerror(errno));
		return REDIS_ERR;
	}

	/* 初始化文件 io */
	rioInitWithFile(&aof, fp);

	/* 设置每写入 REDIS_AOF_AUTOSYNC_BYTES 字节
	 * 就执行一次 FSYNC 
	 * 防止缓存中积累太多命令内容，造成 I/O 阻塞时间过长 */
	if (server.aof_rewrite_incremental_fsync)
		rioSetAutoSync(&aof, REDIS_AOF_AUTOSYNC_BYTES);

	/* 遍历所有数据库 */
	for (j = 0; j < server.dbnum; j++) {
		char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
		redisDb *db = server.db + j;
		/* 指向键空间 */
		dict *d = db->dict;
		if (dictSize(d) == 0) continue;

		/* 创建键空间迭代器 */
		di = dictGetSafeIterator(d);
		if (!di) {
			fclose(fp);
			return REDIS_ERR;
		}

		/* 
		* 首先写入 SELECT 命令，确保之后的数据会被插入到正确的数据库上
		*/
		if (rioWrite(&aof, selectcmd, sizeof(selectcmd) - 1) == 0) goto werr;
		if (rioWriteBulkLongLong(&aof, j) == 0) goto werr;

		/* 
		* 遍历数据库所有键，并通过命令将它们的当前状态（值）记录到新 AOF 文件中
		*/
		while ((de = dictNext(di)) != NULL) {
			sds keystr;
			robj key, *o;
			long long expiretime;
			/* 取出键 */
			keystr = dictGetKey(de);
			/* 取出值 */
			o = dictGetVal(de);
			initStaticStringObject(key, keystr);
			/* 取出过期时间 */
			expiretime = getExpire(db, &key);

			/* 
			* 如果键已经过期，那么跳过它，不保存
			*/
			if (expiretime != -1 && expiretime < now) continue;

			/* 
			* 根据值的类型，选择适当的命令来保存值
			*/
			if (o->type == REDIS_STRING) {
				/* Emit a SET command */
				char cmd[] = "*3\r\n$3\r\nSET\r\n";
				if (rioWrite(&aof, cmd, sizeof(cmd) - 1) == 0) goto werr;
				/* Key and value */
				if (rioWriteBulkObject(&aof, &key) == 0) goto werr;
				if (rioWriteBulkObject(&aof, o) == 0) goto werr;
			}
			else if (o->type == REDIS_LIST) {
				if (rewriteListObject(&aof, &key, o) == 0) goto werr;
			}
			else if (o->type == REDIS_SET) {
				if (rewriteSetObject(&aof, &key, o) == 0) goto werr;
			}
			else if (o->type == REDIS_ZSET) {
				if (rewriteSortedSetObject(&aof, &key, o) == 0) goto werr;
			}
			else if (o->type == REDIS_HASH) {
				if (rewriteHashObject(&aof, &key, o) == 0) goto werr;
			}
			else {
				mylog("%s", "Unknown object type");
			}

			/* 
			* 保存键的过期时间
			*/
			if (expiretime != -1) {
				char cmd[] = "*3\r\n$9\r\nPEXPIREAT\r\n";

				/* 写入 PEXPIREAT expiretime 命令 */
				if (rioWrite(&aof, cmd, sizeof(cmd) - 1) == 0) goto werr;
				if (rioWriteBulkObject(&aof, &key) == 0) goto werr;
				if (rioWriteBulkLongLong(&aof, expiretime) == 0) goto werr;
			}
		}
		/* 释放迭代器 */
		dictReleaseIterator(di);
	}

	/* 冲洗并关闭新 AOF 文件 */
	if (fflush(fp) == EOF) goto werr;
	if (aof_fsync(fileno(fp)) == -1) goto werr;
	if (fclose(fp) == EOF) goto werr;

	/* 
	* 原子地改名，用重写后的新 AOF 文件覆盖旧 AOF 文件
	*/
	if (rename(tmpfile, filename) == -1) {
		mylog("Error moving temp append only file on the final destination: %s", strerror(errno));
		unlink(tmpfile);
		return REDIS_ERR;
	}
	mylog("%s", "SYNC append only file rewrite performed");
	return REDIS_OK;

werr:
	fclose(fp);
	unlink(tmpfile);
	mylog("Write error writing append only file on disk: %s", strerror(errno));
	if (di) dictReleaseIterator(di);
	return REDIS_ERR;
}


/* 
* 以下是后台重写 AOF 文件（BGREWRITEAOF）的工作步骤：
*
* 1) 用户调用 BGREWRITEAOF
*
* 2) Redis 调用这个函数，它执行 fork() ：
*
*    2a) 子进程在临时文件中对 AOF 文件进行重写
*
*    2b) 父进程将新输入的写命令追加到 server.aof_rewrite_buf 中
*
* 3) 当步骤 2a 执行完之后，子进程结束
*
* 4) 父进程会捕捉子进程的退出信号，
*    如果子进程的退出状态是 OK 的话，
*    那么父进程将新输入命令的缓存追加到临时文件，
*    然后使用 rename(2) 对临时文件改名，用它代替旧的 AOF 文件，
*    至此，后台 AOF 重写完成。
*/

int rewriteAppendOnlyFileBackground(void) {
	pid_t childpid;
	long long start;

	/* 已经有进程在进行 AOF 重写了 */
	if (server.aof_child_pid != -1) return REDIS_ERR;

	/* 记录 fork 开始前的时间，计算 fork 耗时用 */
	start = ustime();

	if ((childpid = fork()) == 0) {
		char tmpfile[256];

		/* Child */

		/* 关闭网络连接 fd */
		closeListeningSockets(0);

		/*
		* 我突然想到了一件特别有意思的事情,那就是父进程一旦执行了fork()函数,那么基本上
		* 生成的子进程便拥有了父进程的一切数据,同时,自此以后,父进程和子进程除了父子关系,
		* 再也没有任何瓜葛,这意味着,父进程的更新不会对子进程的数据造成任何影响,请将父子进程
		* 和多线程区分开来.
		*/

		/* 创建临时文件，并进行 AOF 重写 */
		snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int)getpid());
		if (rewriteAppendOnlyFile(tmpfile) == REDIS_OK) {
			size_t private_dirty = zmalloc_get_private_dirty();

			if (private_dirty) {
				mylog("AOF rewrite: %zu MB of memory used by copy-on-write",
					private_dirty / (1024 * 1024));
			}
			/* 发送重写成功信号 */
			exitFromChild(0);
		}
		else {
			/* 发送重写失败信号 */
			exitFromChild(1);
		}
	}
	else { /* Parent */	

		if (childpid == -1) {
			mylog("Can't rewrite append only file in background: fork: %s",
				strerror(errno));
			return REDIS_ERR;
		}

		mylog("Background append only file rewriting started by pid %d", childpid);

		/* 记录 AOF 重写的信息 */
		server.aof_rewrite_scheduled = 0;
		server.aof_rewrite_time_start = time(NULL);
		server.aof_child_pid = childpid;

		/* 关闭字典自动 rehash */
		updateDictResizePolicy();

		/* 
		* 将 aof_selected_db 设为 -1 ，
		* 强制让 feedAppendOnlyFile() 下次执行时引发一个 SELECT 命令，
		* 从而确保之后新添加的命令会设置到正确的数据库中
		*/
		server.aof_selected_db = -1;
		return REDIS_OK;
	}
	return REDIS_OK; /* unreached */
}

/* ----------------------------------------------------------------------------
* AOF loading
* ------------------------------------------------------------------------- */

/* 
* Redis 命令必须由客户端执行，
* 所以 AOF 装载程序需要创建一个无网络连接的客户端来执行 AOF 文件中的命令。
*/
struct redisClient *createFakeClient(void) {  /* 构建一个fake client */
	struct redisClient *c = zmalloc(sizeof(*c));

	selectDb(c, 0); /* 切换到0号数据库 */

	c->fd = -1;
	c->name = NULL;
	c->querybuf = sdsempty();
	c->argc = 0;
	c->argv = NULL;
	c->bufpos = 0;
	c->flags = 0;

	c->reply = listCreate();
	c->reply_bytes = 0;
	c->watched_keys = listCreate();
	listSetFreeMethod(c->reply, decrRefCountVoid);
	listSetDupMethod(c->reply, dupClientReplyValue);
	initClientMultiState(c);
	return c;
}

/*
* 释放伪客户端
*/
void freeFakeClient(struct redisClient *c) {

	/* 释放查询缓存 */
	sdsfree(c->querybuf);

	/* 释放回复缓存 */
	listRelease(c->reply);

	/* 释放事务状态 */
	freeClientMultiState(c);

	zfree(c);
}

/* 
* 将 aof 文件的当前大小记录到服务器状态中。
*
* 通常用于 BGREWRITEAOF 执行之后，或者服务器重启之后。
*/
void aofUpdateCurrentSize(void) {
	struct redis_stat sb;
	/* 读取文件状态 */
	if (redis_fstat(server.aof_fd, &sb) == -1) {
		mylog("Unable to obtain the AOF file length. stat: %s",
			strerror(errno));
	}
	else {
		/* 设置到服务器 */
		server.aof_current_size = sb.st_size;
	}
}



/*
* 执行 AOF 文件中的命令。
*
* 出错时返回 REDIS_OK 。
*
* 出现非执行错误（比如文件长度为 0 ）时返回 REDIS_ERR 。
*
* 出现致命错误时打印信息到日志，并且程序退出。
*/
int loadAppendOnlyFile(char *filename) {

	/* 伪客户端 */
	struct redisClient *fakeClient;

	/* 打开 AOF 文件 */
	FILE *fp = fopen(filename, "r");

	struct redis_stat sb;
	int old_aof_state = server.aof_state;
	long loops = 0;

	/* 检查文件的正确性 */
	if (fp && redis_fstat(fileno(fp), &sb) != -1 && sb.st_size == 0) {
		server.aof_current_size = 0;
		fclose(fp);
		return REDIS_ERR;
	}

	/* 检查文件是否正常打开 */
	if (fp == NULL) {
		mylog("Fatal error: can't open the append log file for reading: %s", strerror(errno));
		exit(1);
	}

	/* 
	* 暂时性地关闭 AOF ，防止在执行 MULTI 时，
	* EXEC 命令被传播到正在打开的 AOF 文件中。
	*/
	server.aof_state = REDIS_AOF_OFF;

	fakeClient = createFakeClient();

	/* 设置服务器的状态为：正在载入
	 * startLoading 定义于 rdb.c */
	startLoading(fp);

	while (1) {
		int argc, j;
		unsigned long len;
		robj **argv; /* 参数 */
		char buf[128];
		sds argsds;
		struct redisCommand *cmd; 

		/*
		* 间隔性地处理客户端发送来的请求
		* 因为服务器正处于载入状态，所以能正常执行的只有 PUBSUB 等模块
		*/
		if (!(loops++ % 1000)) {
			loadingProgress(ftello(fp));
			processEventsWhileBlocked();
		}

		/* 读入文件内容到缓存 */
		if (fgets(buf, sizeof(buf), fp) == NULL) {
			if (feof(fp))
				/* 文件已经读完，跳出 */
				break;
			else
				goto readerr;
		}

		/* 确认协议格式，比如 *3\r\n */
		if (buf[0] != '*') goto fmterr;

		/* 取出命令参数，比如 *3\r\n 中的 3 */
		argc = atoi(buf + 1);

		/* 至少要有一个参数（被调用的命令）*/
		if (argc < 1) goto fmterr;

		/* 从文本中创建字符串对象：包括命令，以及命令参数
		 * 例如 $3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
		 * 将创建三个包含以下内容的字符串对象：
		 * SET 、 KEY 、 VALUE */
		argv = zmalloc(sizeof(robj*)*argc);
		for (j = 0; j < argc; j++) {
			if (fgets(buf, sizeof(buf), fp) == NULL) goto readerr;

			if (buf[0] != '$') goto fmterr;

			/* 读取参数值的长度 */
			len = strtol(buf + 1, NULL, 10);
			/* 读取参数值 */
			argsds = sdsnewlen(NULL, len);
			if (len && fread(argsds, len, 1, fp) == 0) goto fmterr;
			/* 为参数创建对象 */
			argv[j] = createObject(REDIS_STRING, argsds);

			if (fread(buf, 2, 1, fp) == 0) goto fmterr; /* discard CRLF */
		}

		/*
		* 查找命令
		*/
		cmd = lookupCommand(argv[0]->ptr);
		if (!cmd) {
			mylog("Unknown command '%s' reading the append only file", (char*)argv[0]->ptr);
			exit(1);
		}

		/* 
		* 调用伪客户端，执行命令
		*/
		fakeClient->argc = argc;
		fakeClient->argv = argv;
		cmd->proc(fakeClient);

		/* The fake client should not have a reply */
		assert(fakeClient->bufpos == 0 && listLength(fakeClient->reply) == 0);

		/* 
		* 清理命令和命令参数对象
		*/
		for (j = 0; j < fakeClient->argc; j++)
			decrRefCount(fakeClient->argv[j]);
		zfree(fakeClient->argv);
	}

	/* 
	* 如果能执行到这里，说明 AOF 文件的全部内容都可以正确地读取，
	* 但是，还要检查 AOF 是否包含未正确结束的事务
	*/
	if (fakeClient->flags & REDIS_MULTI) goto readerr;

	/* 关闭 AOF 文件 */
	fclose(fp);
	/* 释放伪客户端 */
	freeFakeClient(fakeClient);
	/* 复原 AOF 状态 */
	server.aof_state = old_aof_state;
	/* 停止载入 */
	stopLoading();
	/* 更新服务器状态中， AOF 文件的当前大小 */
	aofUpdateCurrentSize();

	/* 记录前一次重写时的大小 */
	server.aof_rewrite_base_size = server.aof_current_size;

	return REDIS_OK;

	/* 读入错误 */
readerr:
	/* 非预期的末尾，可能是 AOF 文件在写入的中途遭遇了停机 */
	if (feof(fp)) {
		mylog("%s", "Unexpected end of file reading the append only file");
	}
	else { /* 文件内容出错 */
		mylog("Unrecoverable error reading the append only file: %s", strerror(errno));
	}
	exit(1);

	/* 内容格式错误 */
fmterr:
	mylog("%s", "Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
	exit(1);
}

/* 每个缓存块的大小 */
#define AOF_RW_BUF_BLOCK_SIZE (1024*1024*10)    /* 10 MB per block */

typedef struct aofrwblock { 
	/* 缓存块已使用字节数和可用字节数 */
	unsigned long used, free;
	/* 缓存块 */
	char buf[AOF_RW_BUF_BLOCK_SIZE];
} aofrwblock;


/* 
* 在另一个线程中，对给定的描述符 fd （指向 AOF 文件）执行一个后台 fsync() 操作。
*/
void aof_background_fsync(int fd) {
	bioCreateBackgroundJob(REDIS_BIO_AOF_FSYNC, (void*)(long)fd, NULL, NULL);
}

/* 
* 将 AOF 缓存写入到文件中。
*
* 因为程序需要在回复客户端之前对 AOF 执行写操作。
* 而客户端能执行写操作的唯一机会就是在事件 loop 中，
* 因此，程序将所有 AOF 写累积到缓存中，
* 并在重新进入事件 loop 之前，将缓存写入到文件中。
*
* 关于 force 参数：
*
* 当 fsync 策略为每秒钟保存一次时，如果后台线程仍然有 fsync 在执行，
* 那么我们可能会延迟执行冲洗（flush）操作，
* 因为 Linux 上的 write(2) 会被后台的 fsync 阻塞。
*
* 当这种情况发生时，说明需要尽快冲洗 aof 缓存，
* 程序会尝试在 serverCron() 函数中对缓存进行冲洗。
*
* 不过，如果 force 为 1 的话，那么不管后台是否正在 fsync ，
* 程序都直接进行写入。
*/
#define AOF_WRITE_LOG_ERROR_RATE 30 /* Seconds between errors logging. */
void flushAppendOnlyFile(int force) { 
	ssize_t nwritten;
	int sync_in_progress = 0;

	/* 缓冲区中没有任何内容，直接返回 */
	if (sdslen(server.aof_buf) == 0) return;

	if (server.aof_fsync_strategy == AOF_FSYNC_EVERYSEC) /* aof 文件的写入是每秒写入一次 */
		sync_in_progress = bioPendingJobsOfType(REDIS_BIO_AOF_FSYNC) != 0; /* 是否有文件同步在后台执行 */
	if (server.aof_fsync_strategy == AOF_FSYNC_EVERYSEC && !force) {
		/*
		* 当 fsync 策略为每秒钟一次时， fsync 在后台执行。
		*
		* 如果后台仍在执行 FSYNC ，那么我们可以延迟写操作一两秒
		* （如果强制执行 write 的话，服务器主线程将阻塞在 write 上面）
		*/
		if (sync_in_progress) {
			if (server.aof_flush_postponed_start == 0) {
				/* 前面没有推迟过 write 操作，这里将推迟写操作的时间记录下来
				 * 然后就返回，不执行 write 或者 fsync
				 */
				server.aof_flush_postponed_start = server.unixtime;
				return;
			}
			else if (server.unixtime - server.aof_flush_postponed_start < 2) {
				/* 
				* 如果之前已经因为 fsync 而推迟了 write 操作
				* 但是推迟的时间不超过 2 秒，那么直接返回
				* 不执行 write 或者 fsync
				*/
				return;
			}
			/*
			* 如果后台还有 fsync 在执行，并且 write 已经推迟 >= 2 秒
			* 那么执行写操作（write 将被阻塞）
			*/
			server.aof_delayed_fsync++; // 被阻塞的文件同步的数目
			mylog("Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
		}
	}

	/* 
	* 执行到这里，程序会对 AOF 文件进行写入。
	*
	* 清零延迟 write 的时间记录
	*/
	server.aof_flush_postponed_start = 0;

	/* 
	* 执行单个 write 操作，如果写入设备是物理的话，那么这个操作应该是原子的
	*
	* 当然，如果出现像电源中断这样的不可抗现象，那么 AOF 文件也是可能会出现问题的
	* 这时就要用 redis-check-aof 程序来进行修复。
	*/
	nwritten = write(server.aof_fd, server.aof_buf, sdslen(server.aof_buf));
	if (nwritten != (signed)sdslen(server.aof_buf)) {

		static time_t last_write_error_log = 0;
		int can_log = 0;

		/* 将日志的记录频率限制在每行 AOF_WRITE_LOG_ERROR_RATE 秒. */
		if ((server.unixtime - last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE) {
			can_log = 1;
			last_write_error_log = server.unixtime;
		}

		/* Lof the AOF write error and record the error code.
		 * 如果写入出错，那么尝试将该情况写入到日志里面 */
		if (nwritten == -1) {
			if (can_log) {
				mylog("Error writing to the AOF file: %s", strerror(errno));
				server.aof_last_write_errno = errno;
			}
		}
		else {
			if (can_log) {
				mylog("Short write while writing to "
					"the AOF file: (nwritten=%lld, "
					"expected=%lld)",
					(long long)nwritten,
					(long long)sdslen(server.aof_buf));
			}

			/* 尝试移除新追加的不完整内容 */
			if (ftruncate(server.aof_fd, server.aof_current_size) == -1) { /* ftruncate表示截断文件的内容 */
				if (can_log) {
					mylog("Could not remove short write "
						"from the append-only file.  Redis may refuse "
						"to load the AOF the next time it starts.  "
						"ftruncate: %s", strerror(errno));
				}
			}
			else { /* If the ftrunacate() succeeded we can set nwritten to
				   * -1 since there is no longer partial data into the AOF. */
				nwritten = -1;
			}
			server.aof_last_write_errno = ENOSPC; /* 记录下出错原因 */
		}

		/* 处理写入 AOF 文件时出现的错误. */
		if (server.aof_fsync_strategy == AOF_FSYNC_ALWAYS) {
			/* We can't recover when the fsync policy is ALWAYS since the
			* reply for the client is already in the output buffers, and we
			* have the contract with the user that on acknowledged write data
			* is synched on disk. */
			mylog("%s", "Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
			exit(1);
		}
		else {
			/* Recover from failed write leaving data into the buffer. However
			* set an error to stop accepting writes as long as the error
			* condition is not cleared. */
			server.aof_last_write_status = REDIS_ERR;

			/* Trim the sds buffer if there was a partial write, and there
			* was no way to undo it with ftruncate(2). */
			if (nwritten > 0) {
				server.aof_current_size += nwritten;
				sdsrange(server.aof_buf, nwritten, -1);
			}
			return; /* We'll try again on the next call... */
		}
	}
	else {
		/* 写入成功，更新最后写入状态 */
		if (server.aof_last_write_status == REDIS_ERR) {
			mylog("%s", "AOF write error looks solved, Redis can write again.");
			server.aof_last_write_status = REDIS_OK;
		}
	}

	/* 更新写入后的 AOF 文件大小 */
	server.aof_current_size += nwritten;

	/*
	* 如果 AOF 缓存的大小足够小的话，那么重用这个缓存，
	* 否则的话，释放 AOF 缓存。
	*/
	if ((sdslen(server.aof_buf) + sdsavail(server.aof_buf)) < 4000) {
		/* 清空缓存中的内容，等待重用 */
		sdsclear(server.aof_buf);
	}
	else {
		/* 释放缓存 */
		sdsfree(server.aof_buf);
		server.aof_buf = sdsempty();
	}

	/* 
	* 如果 no-appendfsync-on-rewrite 选项为开启状态，
	* 并且有 BGSAVE 或者 BGREWRITEAOF 正在进行的话，
	* 那么不执行 fsync
	*/
	if (server.aof_no_fsync_on_rewrite &&
		(server.aof_child_pid != -1 || server.rdb_child_pid != -1))
		return;

	/* 总是执行 fsnyc */
	if (server.aof_fsync_strategy == AOF_FSYNC_ALWAYS) {
		/* aof_fsync is defined as fdatasync() for Linux in order to avoid
		* flushing metadata. */
		aof_fsync(server.aof_fd); /* Let's try to get this data on the disk */
	}
	else if ((server.aof_fsync_strategy == AOF_FSYNC_EVERYSEC &&
		server.unixtime > server.aof_last_fsync)) {
		/* 放到后台执行 */
		if (!sync_in_progress) aof_background_fsync(server.aof_fd);
	}
	/* 更新最后一次执行 fsync 的时间 */
	server.aof_last_fsync = server.unixtime;
}


/*
* 根据传入的命令和命令参数，将它们还原成协议格式。
*/
sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv) {
	char buf[32];
	int len, j;
	robj *o;

	/* 重建命令的个数，格式为 *<count>\r\n 
	 * 例如 *3\r\n */
	buf[0] = '*';
	len = 1 + ll2string(buf + 1, sizeof(buf) - 1, argc);
	buf[len++] = '\r';
	buf[len++] = '\n';
	dst = sdscatlen(dst, buf, len);

	/* 重建命令和命令参数，格式为 $<length>\r\n<content>\r\n
	 * 例如 $3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n */
	for (j = 0; j < argc; j++) {
		o = getDecodedObject(argv[j]);

		/* 组合 $<length>\r\n */
		buf[0] = '$';
		len = 1 + ll2string(buf + 1, sizeof(buf) - 1, sdslen(o->ptr));
		buf[len++] = '\r';
		buf[len++] = '\n';
		dst = sdscatlen(dst, buf, len);

		/* 组合 <content>\r\n */
		dst = sdscatlen(dst, o->ptr, sdslen(o->ptr));
		dst = sdscatlen(dst, "\r\n", 2);

		decrRefCount(o);
	}

	/* 返回重建后的协议内容 */
	return dst;
}



/* 
* 创建 PEXPIREAT 命令的 sds 表示，
* cmd 参数用于指定转换的源指令， seconds 为 TTL （剩余生存时间）。
*
* 这个函数用于将 EXPIRE 、 PEXPIRE 和 EXPIREAT 转换为 PEXPIREAT
* 从而在保证精确度不变的情况下，将过期时间从相对值转换为绝对值（一个 UNIX 时间戳）。
*
* （过期时间必须是绝对值，这样不管 AOF 文件何时被载入，该过期的 key 都会正确地过期。）
*/
sds catAppendOnlyExpireAtCommand(sds buf, struct redisCommand *cmd, robj *key, robj *seconds) {
	long long when;
	robj *argv[3];

	/* 
	* 取出过期值
	*/
	seconds = getDecodedObject(seconds);
	when = strtoll(seconds->ptr, NULL, 10);

	/* 
	* 如果过期值的格式为秒，那么将它转换为毫秒
	*/
	if (cmd->proc == expireCommand || cmd->proc == setexCommand)
	{
		when *= 1000;
	}

	/* 
	* 如果过期值的格式为相对值，那么将它转换为绝对值
	*/
	if (cmd->proc == expireCommand || 
		cmd->proc == setexCommand || cmd->proc == psetexCommand)
	{
		when += mstime();
	}

	decrRefCount(seconds);

	/* 构建 PEXPIREAT 命令 */
	argv[0] = createStringObject("PEXPIREAT", 9);
	argv[1] = key;
	argv[2] = createStringObjectFromLongLong(when);

	/* 追加到 AOF 缓存中 */
	buf = catAppendOnlyGenericCommand(buf, 3, argv);

	decrRefCount(argv[0]);
	decrRefCount(argv[2]);

	return buf;
}

/* 
* 将字符数组 s 追加到 AOF 缓存的末尾，
* 如果有需要的话，分配一个新的缓存块。
*/
void aofRewriteBufferAppend(unsigned char *s, unsigned long len) {

	/* 指向最后一个缓存块 */
	listNode *ln = listLast(server.aof_rewrite_buf_blocks);
	aofrwblock *block = ln ? ln->value : NULL;

	while (len) {
		/* 
		* 如果已经有至少一个缓存块，那么尝试将内容追加到这个缓存块里面
		*/
		if (block) {
			unsigned long thislen = (block->free < len) ? block->free : len;
			if (thislen) {  /* The current block is not already full. */
				memcpy(block->buf + block->used, s, thislen); /* 如果空间不够也没有办法,那就只能拷贝s的一部分到block的buf中 */
				block->used += thislen;
				block->free -= thislen;
				s += thislen;
				len -= thislen;
			}
		}

		/* 如果 block == NULL ，那么这里是创建缓存链表的第一个缓存块 */
		if (len) { /* First block to allocate, or need another block. */
			int numblocks;

			/* 分配缓存块 */
			block = zmalloc(sizeof(*block));
			block->free = AOF_RW_BUF_BLOCK_SIZE;
			block->used = 0;

			/* 链接到链表末尾 */
			listAddNodeTail(server.aof_rewrite_buf_blocks, block);

			/* 
			* 每次创建 10 个缓存块就打印一个日志，用作标记或者提醒
			*/
			numblocks = listLength(server.aof_rewrite_buf_blocks);
			if (((numblocks + 1) % 10) == 0) {
				mylog("%s", "10 buffer was allocated!\n");
			}
		}
		/* 这里必须要说一下的是,这里使用的是while循环.也就是说,没有写完的内容会继续写,一直到len == 0为止 */
	}
}


/*
* 将命令追加到 AOF 文件中，
* 如果 AOF 重写正在进行，那么也将命令追加到 AOF 重写缓存中。
*/
void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc) {
	sds buf = sdsempty();
	robj *tmpargv[3];

	/* 
	* 使用 SELECT 命令，显式设置数据库，确保之后的命令被设置到正确的数据库
	*/
	if (dictid != server.aof_selected_db) {
		char seldb[64];

		snprintf(seldb, sizeof(seldb), "%d", dictid);
		buf = sdscatprintf(buf, "*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
			(unsigned long)strlen(seldb), seldb);

		server.aof_selected_db = dictid;
	}

	/* EXPIRE 、 PEXPIRE 和 EXPIREAT 命令 */
	if (cmd->proc == expireCommand || cmd->proc == pexpireCommand) {
		/* 
		* 将 EXPIRE 、 PEXPIRE 和 EXPIREAT 都翻译成 PEXPIREAT
		*/
		buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);
	}
	else if (cmd->proc == setexCommand || cmd->proc == psetexCommand) { /* SETEX 和 PSETEX 命令 */
		/* 
		* 将两个命令都翻译成 SET 和 PEXPIREAT
		*/

		/* SET */
		tmpargv[0] = createStringObject("SET", 3);
		tmpargv[1] = argv[1];
		tmpargv[2] = argv[3];
		buf = catAppendOnlyGenericCommand(buf, 3, tmpargv);

		/* PEXPIREAT */
		decrRefCount(tmpargv[0]);
		buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);
	}
	else { /* 其他命令 */
		buf = catAppendOnlyGenericCommand(buf, argc, argv);
	}

	/* 
	* 将命令追加到 AOF 缓存中，
	* 在重新进入事件循环之前，这些命令会被冲洗到磁盘上，
	* 并向客户端返回一个回复。
	*/
	if (server.aof_state == REDIS_AOF_ON)
		server.aof_buf = sdscatlen(server.aof_buf, buf, sdslen(buf));

	/* 
	* 如果 BGREWRITEAOF 正在进行，
	* 那么我们还需要将命令追加到重写缓存中，
	* 从而记录当前正在重写的 AOF 文件和数据库当前状态的差异。
	* 注意,这里不是将命令添加到server.aof_buf中,而是添加到server.aof_rewrite_buf_blocks中,这难道就是所谓的重写缓存?
	*/
	if (server.aof_child_pid != -1)
		aofRewriteBufferAppend((unsigned char*)buf, sdslen(buf));

	/*
	* 这里有一个有意思的点,我在这里记录一下,那就是如果有子进程在重写aof文件的话,那么这里的命令会被添加两次
	* 一次是添加到aof_buf中去,另外一次是添加到aof_rewrite_buf_blocks中去,其实,如果有子进程在重写aof的话
	* 只需要将命令添加到aof_rewrite_buf_blocks里面即可,不用添加到aof_buf,因为重写完成后,父进程会调用函数
	* backgroundRewriteDoneHandler,在那个函数里,添加到aof_rewrite_buf_blocks的命令会被写入到aof文件中
	* 而aof_buf会被清空,这里之所以这么干,是作者偷懒而已.
	*/

	/* 释放 */
	sdsfree(buf);
}

/*
* 删除 AOF 重写所产生的临时文件
*/
void aofRemoveTempFile(pid_t childpid) {
	char tmpfile[256];

	snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int)childpid);
	unlink(tmpfile);
}


/* 
* 将重写缓存中的所有内容（可能由多个块组成）写入到给定 fd 中。
*
* 如果没有 short write 或者其他错误发生，那么返回写入的字节数量，
* 否则，返回 -1 。
*/
ssize_t aofRewriteBufferWrite(int fd) { /* 现在是要将缓存区的内容写入文件描述符指向的文件 */
	listNode *ln;
	listIter li;
	ssize_t count = 0;

	/* 遍历所有缓存块 */
	listRewind(server.aof_rewrite_buf_blocks, &li);
	while ((ln = listNext(&li))) { // 我的想法是,写完多少,那么就应该释放掉缓存区里面的内容吧,不过这里貌似没有这么干
		aofrwblock *block = listNodeValue(ln);
		ssize_t nwritten;

		if (block->used) {
			/* 写入缓存块内容到 fd 指向的文件中去 */
			nwritten = write(fd, block->buf, block->used);
			if (nwritten != block->used) {
				if (nwritten == 0) errno = EIO;
				return -1;
			}

			/* 积累写入字节 */
			count += nwritten;
		}
	}

	return count;
}


/* 
* 当子线程完成 AOF 重写时，父进程调用这个函数。
*/
void backgroundRewriteDoneHandler(int exitcode, int bysignal) {
	if (!bysignal && exitcode == 0) {
		int newfd, oldfd;
		char tmpfile[256];
		long long now = ustime();

		mylog("%s", "Background AOF rewrite terminated with success");

		/* Flush the differences accumulated by the parent to the
		* rewritten AOF. */
		/* 打开保存新 AOF 文件内容的临时文件 */
		snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int)server.aof_child_pid);
		newfd = open(tmpfile, O_WRONLY | O_APPEND);
		if (newfd == -1) {
			mylog("Unable to open the temporary AOF produced by the child: %s", strerror(errno));
			goto cleanup;
		}

		/* 将累积的重写缓存块写入到临时文件中,这里说明一点,这些缓存块是子进程在重写aof的时间里产生的.
		 * 这个函数调用的 write 操作会阻塞主进程 */
		if (aofRewriteBufferWrite(newfd) == -1) {
			mylog("Error trying to flush the parent diff to the rewritten AOF: %s", strerror(errno));
			close(newfd);
			goto cleanup;
		}

		mylog("%s", "Parent diff successfully flushed to the rewritten AOF.");

		/* 
		* 剩下的工作就是将临时文件改名为 AOF 程序指定的文件名，
		* 并将新文件的 fd 设为 AOF 程序的写目标。
		*
		* 不过这里有一个问题 ——
		* 我们不想 close(2) 或者 rename(2) 在删除旧文件时阻塞。
		*
		* 以下是两个可能的场景：
		*
		* 1) AOF 被关闭，这个是一次单次的写操作。
		* 临时文件会被改名为 AOF 文件。
		* 本来已经存在的 AOF 文件会被 unlink ，这可能会阻塞服务器。
		*
		* 2)  AOF 被开启，并且重写后的 AOF 文件会立即被用于接收新的写入命令。
		* 当临时文件被改名为 AOF 文件时，原来的 AOF 文件描述符会被关闭。
		* 因为 Redis 会是最后一个引用这个文件的进程，
		* 所以关闭这个文件会引起 unlink ，这可能会阻塞服务器。
		*
		* 为了避免出现阻塞现象，程序会将 close(2) 放到后台线程执行，
		* 这样服务器就可以持续处理请求，不会被中断。
		*/
		if (server.aof_fd == -1) {
			/* AOF disabled */

			/* Don't care if this fails: oldfd will be -1 and we handle that.
			* One notable case of -1 return is if the old file does
			* not exist. */
			oldfd = open(server.aof_filename, O_RDONLY | O_NONBLOCK);
		}
		else {
			/* AOF enabled */
			oldfd = -1; /* We'll set this to the current AOF filedes later. */
		}

		/* 
		* 对临时文件进行改名，替换现有的 AOF 文件。
		*
		* 旧的 AOF 文件不会在这里被 unlink ，因为 oldfd 引用了它。
		*/
		if (rename(tmpfile, server.aof_filename) == -1) {
			mylog("Error trying to rename the temporary AOF file: %s", strerror(errno));
			close(newfd);
			if (oldfd != -1) close(oldfd);
			goto cleanup;
		}

		if (server.aof_fd == -1) {
			/* 
			* AOF 被关闭，直接关闭 AOF 文件，
			* 因为关闭 AOF 本来就会引起阻塞，所以这里就算 close 被阻塞也无所谓
			*/
			close(newfd);
		}
		else {
			/*
			* 用新 AOF 文件的 fd 替换原来 AOF 文件的 fd
			*/
			oldfd = server.aof_fd;
			server.aof_fd = newfd;

			/* 因为前面进行了 AOF 重写缓存追加，所以这里立即 fsync 一次 */
			if (server.aof_fsync_strategy == AOF_FSYNC_ALWAYS)
				aof_fsync(newfd);

			/* 强制引发 SELECT */
			server.aof_selected_db = -1; /* Make sure SELECT is re-issued */

			/* 更新 AOF 文件的大小 */
			aofUpdateCurrentSize();

			/* 记录前一次重写时的大小 */
			server.aof_rewrite_base_size = server.aof_current_size;

			/* 
			* 清空 AOF 缓存，因为它的内容已经被写入过了，没用了
			*/
			sdsfree(server.aof_buf);
			server.aof_buf = sdsempty();
		}

		mylog("%s", "Background AOF rewrite finished successfully");

		/*
		* 如果是第一次创建 AOF 文件，那么更新 AOF 状态
		*/
		if (server.aof_state == REDIS_AOF_WAIT_REWRITE)
			server.aof_state = REDIS_AOF_ON;

		/* 
		* 异步关闭旧 AOF 文件
		*/
		// todo

		mylog("Background AOF rewrite signal handler took %lldus", ustime() - now);
	}
	else if (!bysignal && exitcode != 0) { /* BGREWRITEAOF 重写出错 */
		mylog("%s", "Background AOF rewrite terminated with error");
	}
	else { /* 未知错误 */
		//server.aof_lastbgrewrite_status = REDIS_ERR;
		mylog("Background AOF rewrite terminated by signal %d", bysignal);
	}

cleanup:

	/* 清空 AOF 缓冲区 */
	aofRewriteBufferReset();

	/* 移除临时文件 */
	aofRemoveTempFile(server.aof_child_pid);

	/* 重置默认属性 */
	server.aof_child_pid = -1;
	/* Schedule a new rewrite if we are waiting for it to switch the AOF ON. */
	if (server.aof_state == REDIS_AOF_WAIT_REWRITE)
		server.aof_rewrite_scheduled = 1;
}

