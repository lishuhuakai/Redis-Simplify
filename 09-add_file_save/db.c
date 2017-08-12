#include "networking.h"
#include "db.h"
#include "object.h"
#include "intset.h"
#include "ziplist.h"
#include "util.h"
#include <signal.h>
#include <ctype.h>

/*============================== Variable and Function Declaration =========================*/
extern struct sharedObjectsStruct shared;


/*
 * 从数据库db中取出键key的值(对象)
 * 如果key的值存在,那么返回该值,否则,返回NULL.
 */
robj *lookupKey(redisDb *db, robj *key) {
	// 查找键空间
	char *str = key->ptr;
	dictEntry *de = dictFind(db->dict, key->ptr);

	if (de) {
		robj *val = dictGetVal(de);
		return val;
	}
	else
		return NULL;
}

/*
 * 为执行读取操作而取出键key在数据库db中的值,并根据是否成功找到值,更新服务器中的命中
 * 或者不命中信息,找到时返回值对象,没有找到是返回NULL.
 */
robj *lookupKeyRead(redisDb *db, robj *key) {
	robj *val;
	
	// 从数据库中取出键的值
	val = lookupKey(db, key);

	// todo
	return val;
}

/*
 * 为执行读取操作而从数据库中查找返回key的值.
 * 如果key存在,那么返回key的值对象,否则的话,向客户端发送Reply参数中的信息,并返回NULL.
 */
robj *lookupKeyReadOrReply(redisClient *c, robj *key, robj *reply) {
	// 查找
	robj *o = lookupKeyRead(c->db, key);

	// 决定是否发送信息
	if (!o) addReply(c, reply);
	return o;
}

/* 为执行写入操作而取出键 key 在数据库 db 中的值。
 * 和 lookupKeyRead 不同，这个函数不会更新服务器的命中/不命中信息。
 * 找到时返回值对象，没找到返回 NULL 。
 */
robj* lookupKeyWrite(redisDb *db, robj *key) {
	//  删除过期键
	return lookupKey(db, key);
}

/* 尝试将键值对 key 和 val 添加到数据库中。
 * 调用者负责对 key 和 val 的引用计数进行增加。
 * 程序在键已经存在时会停止。
 */
void dbAdd(redisDb *db, robj *key, robj *val) {
	// 复制键名
	sds copy = sdsdup(key->ptr);
	// 尝试添加键值对
	int retval = dictAdd(db->dict, copy, val);

	// 如果键已经存在,那么停止
	// todo
}

/* 为已存在的键关联一个新值。
 * 调用者负责对新值 val 的引用计数进行增加。
 */
void dbOverwrite(redisDb *db, robj *key, robj *val) {
	dictEntry *de = dictFind(db->dict, key->ptr);
	dictReplace(db->dict, key->ptr, val);
}

/* 高层次的 SET 操作函数。
 *
 * 这个函数可以在不管键 key 是否存在的情况下，将它和 val 关联起来。
 *
 * 1) The ref count of the value object is incremented.
 *    值对象的引用计数会被增加
 *
 * 2) clients WATCHing for the destination key notified.
 *    监视键 key 的客户端会收到键已经被修改的通知
 *
 * 3) The expire time of the key is reset (the key is made persistent).
 *    键的过期时间会被移除（键变为持久的）
 */
void setKey(redisDb *db, robj *key, robj *val) {
	// 添加或者覆写数据库中的键值对
	if (lookupKeyWrite(db, key) == NULL) {
		dbAdd(db, key, val);
	}
	else {
		dbOverwrite(db, key, val);
	}
	incrRefCount(val);
	// 移除键的过期时间
	removeExpire(db, key);
}


/*
 * 将客户端的目标数据库切换成id所指定的数据库
 */
extern struct redisServer server;
int selectDb(redisClient *c, int id) {
	/* 确保id在正确范围内 */
	if (id < 0 || id >= server.dbnum)
		return REDIS_ERR;
	/* 切换数据库(更新指针) */
	c->db = &server.db[id];
	return REDIS_OK;
}

robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o) {
	assert(o->type == REDIS_STRING);
	if (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW) {
		// 这个东西被引用了的话,要重新构建一个对象
		robj *decoded = getDecodedObject(o);
		o = createRawStringObject(decoded->ptr, sdslen(decoded->ptr));
		decrRefCount(decoded);
		dbOverwrite(db, key, o);
	}
	return o;
}
/*
 * 检查键key是否存在于数据库中,存在返回1,不存在返回0
 */
int dbExists(redisDb *db, robj *key) {
	return dictFind(db->dict, key->ptr) != NULL;
}

void existsCommand(redisClient *c) {
	if (dbExists(c->db, c->argv[1])) {
		addReply(c, shared.cone);
	}
	else {
		addReply(c, shared.czero);
	}
}

/*
 * 为执行写入操作而从数据库中查找返回key的值.
 * 如果key存在,那么返回key的值对象.
 * 如果key不存在,那么向客户端发送reply参数中的信息,并返回NULL.
 */
robj* lookupKeyWriteOrReply(redisClient *c, robj *key, robj* reply) {
	robj *o = lookupKeyWrite(c->db, key);
	if (!o) addReply(c, reply);
	return o;
}

/*
 * 从数据库中删除给定的键,键的值,以及键的过期时间.
 * 删除成功返回1,因为键不存在而导致删除失败时,返回0.
 */
int dbDelete(redisDb *db, robj *key) {
	// 删除键值对
	if (dictDelete(db->dict, key->ptr) == DICT_OK) {
		// todo
		return 1;
	}
	else
		return 0;
}

/* Helper function to extract keys from following commands:
* ZUNIONSTORE <destkey> <num-keys> <key> <key> ... <key> <options>
* ZINTERSTORE <destkey> <num-keys> <key> <key> ... <key> <options> */
int *zunionInterGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys) {
	int i, num, *keys;
	REDIS_NOTUSED(cmd);

	num = atoi(argv[2]->ptr);
	/* Sanity check. Don't return any key if the command is going to
	* reply with syntax error. */
	if (num > (argc - 3)) {
		*numkeys = 0;
		return NULL;
	}

	/* Keys in z{union,inter}store come from two places:
	* argv[1] = storage key,
	* argv[3...n] = keys to intersect */
	keys = zmalloc(sizeof(int)*(num + 1));

	/* Add all key positions for argv[3...n] to keys[] */
	for (i = 0; i < num; i++) keys[i] = 3 + i;

	/* Finally add the argv[1] key position (the storage key target). */
	keys[num] = 1;
	*numkeys = num + 1;  /* Total keys = {union,inter} keys + storage key */
	return keys;
}

/*
 * 将键 key 的过期时间设为 when
 */
void setExpire(redisDb *db, robj *key, long long when) {

	dictEntry *kde, *de;
	/* 取出键 */
	kde = dictFind(db->dict, key->ptr);

	assert(kde != NULL);

	/* 根据键取出键的过期时间 */
	de = dictReplaceRaw(db->expires, dictGetKey(kde));

	/* 设置键的过期时间
	 * 这里是直接使用整数值来保存过期时间，不是用 INT 编码的 String 对象 */
	dictSetSignedIntegerVal(de, when);
}

/*
 * 这个函数是 EXPIRE 、 PEXPIRE 、 EXPIREAT 和 PEXPIREAT 命令的底层实现函数。
 *
 * 命令的第二个参数可能是绝对值，也可能是相对值。
 * 当执行 *AT 命令时， basetime 为 0 ，在其他情况下，它保存的就是当前的绝对时间。
 *
 * unit 用于指定 argv[2] （传入过期时间）的格式，
 * 它可以是 UNIT_SECONDS 或 UNIT_MILLISECONDS ，
 * basetime 参数则总是毫秒格式的。
 */
void expireGenericCommand(redisClient *c, long long basetime, int unit) {
	robj *key = c->argv[1], *param = c->argv[2];
	long long when; /* unix time in milliseconds when the key will expire. */

	/* 取出 when 参数 */
	if (getLongLongFromObjectOrReply(c, param, &when, NULL) != REDIS_OK)
		return;

	/* 如果传入的过期时间是以秒为单位的，那么将它转换为毫秒 */
	if (unit == UNIT_SECONDS) when *= 1000;
	when += basetime;

	/* 取出键 */
	if (lookupKeyRead(c->db, key) == NULL) {
		addReply(c, shared.czero);
		return;
	}

	/* 设置键的过期时间 */
	setExpire(c->db, key, when);
	addReply(c, shared.cone);
	server.dirty++;
	return;
}

void expireCommand(redisClient *c) { /* 设置某个键过期 */
	expireGenericCommand(c, mstime(), UNIT_SECONDS);
}

/*
 * 返回给定 key 的过期时间。
 *
 * 如果键没有设置过期时间，那么返回 -1 。
 */
long long getExpire(redisDb *db, robj *key) {
	dictEntry *de;

	/* 获取键的过期时间
	 * 如果过期时间不存在，那么直接返回 */
	if (dictSize(db->expires) == 0 ||
		(de = dictFind(db->expires, key->ptr)) == NULL) return -1;
	assert(dictFind(db->dict, key->ptr) != NULL);

	/* 返回过期时间 */
	return dictGetSignedIntegerVal(de);
}

/*
 * 返回键的剩余生存时间。
 *
 * output_ms 指定返回值的格式：
 *
 *  - 为 1 时，返回毫秒
 *
 *  - 为 0 时，返回秒
 */
void ttlGenericCommand(redisClient *c, int output_ms) {
	long long expire, ttl = -1;

	/* 取出键 */
	if (lookupKeyRead(c->db, c->argv[1]) == NULL) {
		addReplyLongLong(c, -2);
		return;
	}

	/* 取出过期时间 */
	expire = getExpire(c->db, c->argv[1]);

	if (expire != -1) {
		/* 计算剩余生存时间 */
		ttl = expire - mstime();
		if (ttl < 0) ttl = 0;
	}

	if (ttl == -1) {
		/* 键是持久的 */
		addReplyLongLong(c, -1);
	}
	else {
		/* 返回 TTL 
		 * (ttl+500)/1000 计算的是渐近秒数 */
		addReplyLongLong(c, output_ms ? ttl : ((ttl + 500) / 1000));
	}
}


void ttlCommand(redisClient *c) { /* 计算还有多少时间键到期 */
	ttlGenericCommand(c, 0);
}

/*
 * 移除键 key 的过期时间
 */
int removeExpire(redisDb *db, robj *key) {
	/* 确保键带有过期时间 */
	assert(dictFind(db->dict, key->ptr) != NULL);

	/* 删除过期时间 */
	return dictDelete(db->expires, key->ptr) == DICT_OK;
}

void persistCommand(redisClient *c) {
	dictEntry *de;

	/* 取出键 */
	de = dictFind(c->db->dict, c->argv[1]->ptr);

	if (de == NULL) {
		/* 键没有过期时间 */
		addReply(c, shared.czero);

	}
	else {
		/* 键带有过期时间，那么将它移除 */
		if (removeExpire(c->db, c->argv[1])) {
			addReply(c, shared.cone);
			server.dirty++;
		}
		else { /* 键已经是持久的了 */
			addReply(c, shared.czero);
		}
	}
}

/*
 * 尝试解析游标的值.
 */
int parseScanCursorOrReply(redisClient *c, robj *o, unsigned long *cursor) {
	char *eptr;

	errno = 0;
	*cursor = strtoul(o->ptr, &eptr, 10); /* 首先解析游标 */
	if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' || errno == ERANGE)
	{
		addReplyError(c, "invalid cursor");
		return REDIS_ERR;
	}
	return REDIS_OK;
}

/*
 * 检查 key 是否已经过期，如果是的话，将它从数据库中删除。
 *
 * 返回 0 表示键没有过期时间，或者键未过期。
 *
 * 返回 1 表示键已经因为过期而被删除了。
 */
int expireIfNeeded(redisDb *db, robj *key) {

	/* 取出键的过期时间 */
	mstime_t when = getExpire(db, key);
	mstime_t now;

	/* 没有过期时间 */
	if (when < 0) return 0; /* No expire for this key */

	now = mstime();
	/* 如果未过期，返回 0 */
	if (now <= when) return 0;

	/* 将过期键从数据库中删除 */
	return dbDelete(db, key);
}

/* This callback is used by scanGenericCommand in order to collect elements
* returned by the dictionary iterator into a list. */
void scanCallback(void *privdata, const dictEntry *de) {
	void **pd = (void**)privdata;
	list *keys = pd[0];
	robj *o = pd[1];
	robj *key, *val = NULL;

	if (o == NULL) {
		sds sdskey = dictGetKey(de);
		key = createStringObject(sdskey, sdslen(sdskey));
	}
	else if (o->type == REDIS_SET) {
		key = dictGetKey(de);
		incrRefCount(key);
	}
	else if (o->type == REDIS_HASH) {
		key = dictGetKey(de);
		incrRefCount(key);
		val = dictGetVal(de);
		incrRefCount(val);
	}
	else if (o->type == REDIS_ZSET) {
		key = dictGetKey(de);
		incrRefCount(key);
		val = createStringObjectFromLongDouble(*(double*)dictGetVal(de));
	}
	else {
		assert(NULL);
	}
	listAddNodeTail(keys, key);
	if (val) listAddNodeTail(keys, val);
}


/* 
 * 这是 SCAN 、 HSCAN 、 SSCAN 命令的实现函数。
 *
 * 如果给定了对象 o ，那么它必须是一个哈希对象或者集合对象，
 * 如果 o 为 NULL 的话，函数将使用当前数据库作为迭代对象。
 *
 * 如果参数 o 不为 NULL ，那么说明它是一个键对象，函数将跳过这些键对象，
 * 对给定的命令选项进行分析（parse）。
 *
 * 如果被迭代的是哈希对象，那么函数返回的是键值对。
 */
void scanGenericCommand(redisClient *c, robj *o, unsigned long cursor) {
	int rv;
	int i, j;
	char buf[REDIS_LONGSTR_SIZE];
	list *keys = listCreate();
	listNode *node, *nextnode;
	long long count = 10;
	sds pat;
	int patlen, use_pattern = 0;
	dict *ht;

	/* 输入类型检查 */
	assert(o == NULL || o->type == REDIS_SET || o->type == REDIS_HASH ||
		o->type == REDIS_ZSET);

	/* Set i to the first option argument. The previous one is the cursor. */
	// 设置第一个选项参数的索引位置
	// 0    1      2      3  
	// SCAN OPTION <op_arg>         SCAN 命令的选项值从索引 2 开始
	// HSCAN <key> OPTION <op_arg>  而其他 *SCAN 命令的选项值从索引 3 开始
	i = (o == NULL) ? 2 : 3; /* Skip the key argument if needed. */

	/* Step 1: 分析选项参数. */
	while (i < c->argc) {
		j = c->argc - i;

		/* COUNT <number>
		* count 选项的作用就是让用户告知迭代命令， 在每次迭代中应该从数据集里返回多少元素
		* count 的默认值是10
		*/
		if (!strcasecmp(c->argv[i]->ptr, "count") && j >= 2) {
			if (getLongFromObjectOrReply(c, c->argv[i + 1], &count, NULL)
				!= REDIS_OK)
			{
				goto cleanup;
			}

			if (count < 1) {
				addReply(c, shared.syntaxerr);
				goto cleanup;
			}
			i += 2;
			
		}
		else if (!strcasecmp(c->argv[i]->ptr, "match") && j >= 2) {
			/* MATCH <pattern>
			* 增量式迭代命令通过给定 MATCH 参数的方式实现了通过提供一个 glob 风格的模式参数， 让命令只返回和给定模式相匹配的元素。
			*/
			pat = c->argv[i + 1]->ptr;
			patlen = sdslen(pat);
			/* The pattern always matches if it is exactly "*", so it is
			* equivalent to disabling it. */
			use_pattern = !(pat[0] == '*' && patlen == 1);
			i += 2;
		}
		else { // error
			addReply(c, shared.syntaxerr);
			goto cleanup;
		}
	}

	/* Step 2: 对容器进行迭代.
	 * 如果对象的底层实现为 ziplist 、intset 而不是哈希表，
	 * 那么这些对象应该只包含了少量元素，
	 * 为了保持不让服务器记录迭代状态的设计
	 * 我们将 ziplist 或者 intset 里面的所有元素都一次返回给调用者
	 * 并向调用者返回游标（cursor） 0 */

	/* Handle the case of a hash table. */
	ht = NULL;
	if (o == NULL) {
		/* 迭代目标为当前数据库 */
		ht = c->db->dict;
	}
	else if (o->type == REDIS_SET && o->encoding == REDIS_ENCODING_HT) {
		/* 迭代目标为 HT 编码的集合 */
		ht = o->ptr;
	}
	else if (o->type == REDIS_HASH && o->encoding == REDIS_ENCODING_HT) {
		/* 迭代目标为 HT 编码的哈希 */
		ht = o->ptr;
		count *= 2; /* We return key / value for this type. */
	}
	else if (o->type == REDIS_ZSET && o->encoding == REDIS_ENCODING_SKIPLIST) {
		/* 迭代目标为 HT 编码的跳跃表 */
		zset *zs = o->ptr;
		ht = zs->dict;
		count *= 2; /* We return key / value for this type. */
	}

	if (ht) {
		void *privdata[2];

		/* 我们向回调函数传入两个指针：
		 * 一个是用于记录被迭代元素的列表
		 * 另一个是字典对象
		 * 从而实现类型无关的数据提取操作 */
		privdata[0] = keys;
		privdata[1] = o;
		do {
			cursor = dictScan(ht, cursor, scanCallback, privdata); /* 最终将数据放入了keys中 */
		} while (cursor && listLength(keys) < count); /* 要提取到足够的元素个数才行 */
	}
	else if (o->type == REDIS_SET) {
		int pos = 0;
		int64_t ll;

		while (intsetGet(o->ptr, pos++, &ll))
			listAddNodeTail(keys, createStringObjectFromLongLong(ll));
		cursor = 0;
	}
	else if (o->type == REDIS_HASH || o->type == REDIS_ZSET) {
		unsigned char *p = ziplistIndex(o->ptr, 0);
		unsigned char *vstr;
		unsigned int vlen;
		long long vll;

		while (p) {
			ziplistGet(p, &vstr, &vlen, &vll);
			listAddNodeTail(keys,
				(vstr != NULL) ? createStringObject((char*)vstr, vlen) :
				createStringObjectFromLongLong(vll));
			p = ziplistNext(o->ptr, p);
		}
		cursor = 0;
	}
	else {
		assert(NULL);
	}

	/* Step 3: 过滤掉一些元素. */
	node = listFirst(keys);
	while (node) {
		robj *kobj = listNodeValue(node);
		nextnode = listNextNode(node);
		int filter = 0;

		/* Filter element if it does not match the pattern. */
		if (!filter && use_pattern) {
			if (sdsEncodedObject(kobj)) {
				if (!stringmatchlen(pat, patlen, kobj->ptr, sdslen(kobj->ptr), 0))
					filter = 1;
			}
			else {
				char buf[REDIS_LONGSTR_SIZE];
				int len;

				assert(kobj->encoding == REDIS_ENCODING_INT);
				len = ll2string(buf, sizeof(buf), (long)kobj->ptr);
				if (!stringmatchlen(pat, patlen, buf, len, 0)) filter = 1;
			}
		}

		/* Filter element if it is an expired key. */
		if (!filter && o == NULL && expireIfNeeded(c->db, kobj)) filter = 1;

		/* Remove the element and its associted value if needed. */
		if (filter) {
			decrRefCount(kobj);
			listDelNode(keys, node);
		}

		/* If this is a hash or a sorted set, we have a flat list of
		* key-value elements, so if this element was filtered, remove the
		* value, or skip it if it was not filtered: we only match keys. */
		if (o && (o->type == REDIS_ZSET || o->type == REDIS_HASH)) {
			node = nextnode;
			nextnode = listNextNode(node);
			if (filter) {
				kobj = listNodeValue(node);
				decrRefCount(kobj);
				listDelNode(keys, node);
			}
		}
		node = nextnode;
	}

	/* Step 4: Reply to the client. */
	addReplyMultiBulkLen(c, 2);
	rv = snprintf(buf, sizeof(buf), "%lu", cursor);
	assert(rv < sizeof(buf));
	addReplyBulkCBuffer(c, buf, rv);

	addReplyMultiBulkLen(c, listLength(keys));
	while ((node = listFirst(keys)) != NULL) {
		robj *kobj = listNodeValue(node);
		addReplyBulk(c, kobj);
		decrRefCount(kobj);
		listDelNode(keys, node);
	}

cleanup:
	listSetFreeMethod(keys, decrRefCountVoid);
	listRelease(keys);
}




/* 不出什么意外的话,scan命令是我剖析的最后一个比较大型的命令了! */
void scanCommand(redisClient *c) { 
	unsigned long cursor;
	if (parseScanCursorOrReply(c, c->argv[1], &cursor) == REDIS_ERR) return;
	scanGenericCommand(c, NULL, cursor);
}

void pexpireCommand(redisClient *c) {
	expireGenericCommand(c, mstime(), UNIT_MILLISECONDS);
}

void selectCommand(redisClient *c) {
	long long id;

	/* 不合法的数据库号码 */
	if (getLongFromObjectOrReply(c, c->argv[1], &id,
		"invalid DB index") != REDIS_OK)
		return;

	/* 切换数据库 */
	if (selectDb(c, id) == REDIS_ERR) {
		addReplyError(c, "invalid DB index");
	}
	else {
		addReply(c, shared.ok);
	}
}




