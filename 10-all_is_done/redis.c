#include "redis.h"

#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <locale.h>
#include "db.h"
#include "object.h"
#include "util.h"
#include "t_string.h"
#include "t_hash.h"
#include "networking.h"
#include "t_list.h"
#include "t_set.h"
#include "t_zset.h"
#include "aof.h"
#include "multi.h"

struct sharedObjectsStruct shared;

/*=============================== Function declariton =======================*/
void setCommand(redisClient *c);
void getCommand(redisClient *c);
/*================================= Globals ================================= */

/* Global vars */
struct redisServer server; /* server global state */
double R_Zero, R_PosInf, R_NegInf, R_Nan;

struct redisCommand redisCommandTable[] = {
	{ "get",getCommand,2,"r",0, NULL,1,1,1,0,0 },
	{ "set",setCommand,-3,"wm",0,NULL,1,1,1,0,0 },
	{ "setnx",setnxCommand,3,"wm",0,NULL,1,1,1,0,0 },
	{ "setex",setexCommand,4,"wm",0,NULL,1,1,1,0,0 },
	{ "psetex",psetexCommand,4,"wm",0,NULL,1,1,1,0,0 },
	{ "append",appendCommand,3,"wm",0,NULL,1,1,1,0,0 },
	{ "strlen",strlenCommand,2,"r",0,NULL,1,1,1,0,0 },
	{ "exists",existsCommand,2,"r",0,NULL,1,1,1,0,0 },
	{ "setrange",setrangeCommand,4,"wm",0,NULL,1,1,1,0,0 },
	{ "getrange",getrangeCommand,4,"r",0,NULL,1,1,1,0,0 },
	{ "substr",getrangeCommand,4,"r",0,NULL,1,1,1,0,0 }, // 求子串居然是getrange的alias 
	{ "incr",incrCommand,2,"wm",0,NULL,1,1,1,0,0 },
	{ "decr",decrCommand,2,"wm",0,NULL,1,1,1,0,0 },
	{ "mget",mgetCommand,-2,"r",0,NULL,1,-1,1,0,0 },
	{ "mset",msetCommand,-3,"wm",0,NULL,1,-1,2,0,0 },
	{ "msetnx",msetnxCommand,-3,"wm",0,NULL,1,-1,2,0,0 },
	{ "incrby",incrbyCommand,3,"wm",0,NULL,1,1,1,0,0 },
	{ "decrby",decrbyCommand,3,"wm",0,NULL,1,1,1,0,0 },
	/* hashset command */
	{ "hexists",hexistsCommand,3,"r",0,NULL,1,1,1,0,0 },
	{ "hset",hsetCommand,4,"wm",0,NULL,1,1,1,0,0 },
	{ "hget",hgetCommand,3,"r",0,NULL,1,1,1,0,0 },
	{ "hgetall",hgetallCommand,2,"r",0,NULL,1,1,1,0,0 },
	{ "hmget",hmgetCommand,-3,"r",0,NULL,1,1,1,0,0 },
	{ "hmset",hmsetCommand,-4,"wm",0,NULL,1,1,1,0,0 },
	{ "hkeys",hkeysCommand,2,"rS",0,NULL,1,1,1,0,0 },
	{ "hvals",hvalsCommand,2,"rS",0,NULL,1,1,1,0,0 },
	{ "hlen",hlenCommand,2,"r",0,NULL,1,1,1,0,0 },
	/* list command */
	{ "lpush",lpushCommand,-3,"wm",0,NULL,1,1,1,0,0 },
	{ "rpush", rpushCommand, -3, "wm", 0, NULL, 1, 1, 1, 0, 0 },
	{ "rpop",rpopCommand,2,"w",0,NULL,1,1,1,0,0 },
	{ "lpop",lpopCommand,2,"w",0,NULL,1,1,1,0,0 },
	{ "lindex",lindexCommand,3,"r",0,NULL,1,1,1,0,0 },
	{ "llen",llenCommand,2,"r",0,NULL,1,1,1,0,0 },
	{ "lset",lsetCommand,4,"wm",0,NULL,1,1,1,0,0 },
	/* set command */
	{ "sadd",saddCommand,-3,"wm",0,NULL,1,1,1,0,0 },
	{ "smembers",sinterCommand,2,"rS",0,NULL,1,1,1,0,0 },
	{ "scard",scardCommand,2,"r",0,NULL,1,1,1,0,0 },
	{ "spop",spopCommand,2,"wRs",0,NULL,1,1,1,0,0 },
	{ "sismember",sismemberCommand,3,"r",0,NULL,1,1,1,0,0 },
	{ "smove",smoveCommand,4,"w",0,NULL,1,2,1,0,0 },
	{ "sunion",sunionCommand,-2,"rS",0,NULL,1,-1,1,0,0 },
	{ "sunionstore",sunionstoreCommand,-3,"wm",0,NULL,1,-1,1,0,0 },
	{ "sdiff",sdiffCommand,-2,"rS",0,NULL,1,-1,1,0,0 },
	{ "sdiffstore",sdiffstoreCommand,-3,"wm",0,NULL,1,-1,1,0,0 },
	/* zset command */
	{ "zadd",zaddCommand,-4,"wm",0,NULL,1,1,1,0,0 },
	{ "zcard",zcardCommand,2,"r",0,NULL,1,1,1,0,0 },
	{ "zcount",zcountCommand,4,"r",0,NULL,1,1,1,0,0 },
	{ "zrank",zrankCommand,3,"r",0,NULL,1,1,1,0,0 },
	{ "zincrby",zincrbyCommand,4,"wm",0,NULL,1,1,1,0,0 },
	{ "zunionstore",zunionstoreCommand,-4,"wm",0,zunionInterGetKeys,0,0,0,0,0 },
	{ "zinterstore",zinterstoreCommand,-4,"wm",0,zunionInterGetKeys,0,0,0,0,0 },

	{ "ttl",ttlCommand,2,"r",0,NULL,1,1,1,0,0 },
	{ "persist", persistCommand,2,"w",0,NULL,1,1,1,0,0 },
	{ "expire",expireCommand,3,"w",0,NULL,1,1,1,0,0 },
	{ "pexpire",pexpireCommand,3,"w",0,NULL,1,1,1,0,0 },
	{ "scan",scanCommand,-2,"rR",0,NULL,0,0,0,0,0 },
	{ "save",saveCommand,1,"ars",0,NULL,0,0,0,0,0 },
	{ "select",selectCommand,2,"rl",0,NULL,0,0,0,0,0 },
	/* 事务功能 */
	{ "exec",execCommand,1,"sM",0,NULL,0,0,0,0,0 },
	{ "discard",discardCommand,1,"rs",0,NULL,0,0,0,0,0 },
	{ "watch",watchCommand,-2,"rs",0,NULL,1,-1,1,0,0 },
	{ "unwatch",unwatchCommand,1,"rs",0,NULL,0,0,0,0,0 },
	{ "multi",multiCommand,1,"rs",0,NULL,0,0,0,0,0 },
};

/*================================ Dict ===================================== */
unsigned int dictSdsHash(const void *key) {
	return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

unsigned int dictSdsCaseHash(const void *key) {
	return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictSdsKeyCompare(void *privdata, const void *key1,
	const void *key2) { /* 比较两个键的值是否相等,值得一提的是,两个key应该是字符串对象 */
	int l1, l2;

	l1 = sdslen((sds)key1); /* 获得长度 */
	l2 = sdslen((sds)key2);
	if (l1 != l2) return 0;
	return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the command lookup table and other
 * places where case insensitive non binary-safe comparison is needed. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
	const void *key2) {
	return strcasecmp(key1, key2) == 0; // 不区分大小写 
}
 /* 
  * 设置析构函数 
  */
void dictSdsDestructor(void *privdata, void *val) {
	sdsfree(val);
}

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
	dictSdsCaseHash,           /* hash function */
	NULL,                      /* key dup */
	NULL,                      /* val dup */
	dictSdsKeyCaseCompare,     /* key compare */
	dictSdsDestructor,         /* key destructor */
	NULL                       /* val destructor */
};

void dictRedisObjectDestructor(void *privdata, void *val) {
	if (val == NULL) return; /* Values of swapped out keys as set to NULL */
	decrRefCount(val); // 减少一个引用 
}

dictType dbDictType = {
	dictSdsHash,                /* hash function */
	NULL,                       /* key dup */
	NULL,                       /* val dup */
	dictSdsKeyCompare,          /* key compare */
	dictSdsDestructor,          /* key destructor */
	dictRedisObjectDestructor   /* val destructor */
};

/* Db->expires */
dictType keyptrDictType = {
	dictSdsHash,               /* hash function */
	NULL,                      /* key dup */
	NULL,                      /* val dup */
	dictSdsKeyCompare,         /* key compare */
	NULL,                      /* key destructor */
	NULL                       /* val destructor */
};


unsigned int dictEncObjHash(const void *key) {
	robj *o = (robj*)key;

	if (sdsEncodedObject(o)) {
		return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
	}
	else {
		if (o->encoding == REDIS_ENCODING_INT) {
			char buf[32];
			int len;

			len = ll2string(buf, 32, (long)o->ptr);
			return dictGenHashFunction((unsigned char*)buf, len);
		}
		else {
			unsigned int hash;

			o = getDecodedObject(o);
			hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
			decrRefCount(o);
			return hash;
		}
	}
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
	const void *key2)
{
	robj *o1 = (robj*)key1, *o2 = (robj*)key2;
	int cmp;

	if (o1->encoding == REDIS_ENCODING_INT &&
		o2->encoding == REDIS_ENCODING_INT)
		return o1->ptr == o2->ptr;

	o1 = getDecodedObject(o1);
	o2 = getDecodedObject(o2);
	cmp = dictSdsKeyCompare(privdata, o1->ptr, o2->ptr);
	decrRefCount(o1);
	decrRefCount(o2);
	return cmp;
}

/* Hash type hash table (note that small hashes are represented with ziplists) */
dictType hashDictType = {
	dictEncObjHash,             /* hash function */
	NULL,                       /* key dup */
	NULL,                       /* val dup */
	dictEncObjKeyCompare,       /* key compare */
	dictRedisObjectDestructor,  /* key destructor */
	dictRedisObjectDestructor   /* val destructor */
};

/* Sets type hash table */
dictType setDictType = {
	dictEncObjHash,            /* hash function */
	NULL,                      /* key dup */
	NULL,                      /* val dup */
	dictEncObjKeyCompare,      /* key compare */
	dictRedisObjectDestructor, /* key destructor */
	NULL                       /* val destructor */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
	dictEncObjHash,            /* hash function */
	NULL,                      /* key dup */
	NULL,                      /* val dup */
	dictEncObjKeyCompare,      /* key compare */
	dictRedisObjectDestructor, /* key destructor */
	NULL                       /* val destructor */
};

unsigned int dictObjHash(const void *key) {
	const robj *o = key;
	return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

int dictObjKeyCompare(void *privdata, const void *key1,
	const void *key2) {
	const robj *o1 = key1, *o2 = key2;
	return dictSdsKeyCompare(privdata, o1->ptr, o2->ptr);
}

void dictListDestructor(void *privdata, void *val) {
	DICT_NOTUSED(privdata);
	listRelease((list*)val);
}
/* Keylist hash table type has unencoded redis objects as keys and
* lists as values. It's used for blocking operations (BLPOP) and to
* map swapped keys to a list of clients waiting for this keys to be loaded. */
dictType keylistDictType = {
	dictObjHash,                /* hash function */
	NULL,                       /* key dup */
	NULL,                       /* val dup */
	dictObjKeyCompare,          /* key compare */
	dictRedisObjectDestructor,  /* key destructor */
	dictListDestructor          /* val destructor */
};

/* This function is called once a background process of some kind terminates,
* as we want to avoid resizing the hash tables when there is a child in order
* to play well with copy-on-write (otherwise when a resize happens lots of
* memory pages are copied). The goal of this function is to update the ability
* for dict.c to resize the hash tables accordingly to the fact we have o not
* running childs. */
void updateDictResizePolicy(void) {
	if (server.rdb_child_pid == -1 && server.aof_child_pid == -1)
		dictEnableResize();
	else
		dictDisableResize();
}

/*================================== Commands ================================ */

/*
 * 根据 redis.c 文件顶部的命令列表，创建命令表
 */
void populateCommandTable(void) {
	int j;

	// 命令的数量
	int numcommands = sizeof(redisCommandTable) / sizeof(struct redisCommand);

	for (j = 0; j < numcommands; j++) {

		// 指定命令
		struct redisCommand *c = redisCommandTable + j;

		// 取出字符串 FLAG
		char *f = c->sflags;

		int retval1, retval2;

		// 根据字符串 FLAG 生成实际 FLAG
		while (*f != '\0') {
			switch (*f) {
			case 'w': c->flags |= REDIS_CMD_WRITE; break;
			case 'r': c->flags |= REDIS_CMD_READONLY; break;
			case 'm': c->flags |= REDIS_CMD_DENYOOM; break;
			case 'a': c->flags |= REDIS_CMD_ADMIN; break;
			case 'p': c->flags |= REDIS_CMD_PUBSUB; break;
			case 's': c->flags |= REDIS_CMD_NOSCRIPT; break;
			case 'R': c->flags |= REDIS_CMD_RANDOM; break;
			case 'S': c->flags |= REDIS_CMD_SORT_FOR_SCRIPT; break;
			case 'l': c->flags |= REDIS_CMD_LOADING; break;
			case 't': c->flags |= REDIS_CMD_STALE; break;
			case 'M': c->flags |= REDIS_CMD_SKIP_MONITOR; break;
			case 'k': c->flags |= REDIS_CMD_ASKING; break;
			default: 
				break;
			}
			f++;
		}

		// 将命令关联到命令表
		retval1 = dictAdd(server.commands, sdsnew(c->name), c);

		/*
		 * 将命令也关联到原始命令表
		 *
		 * 原始命令表不会受 redis.conf 中命令改名的影响
		 */
		retval2 = dictAdd(server.orig_commands, sdsnew(c->name), c);
	}
}

/*
 * 根据给定命令名字（SDS），查找命令
 */
struct redisCommand *lookupCommand(sds name) {
	return dictFetchValue(server.commands, name);
}

/* 
 * 将指定命令（以及执行该命令的上下文，比如数据库 id 等信息）传播到 AOF 和 slave 。
 *
 * flags are an xor between:
 * FLAG 可以是以下标识的 xor ：
 *
 * + REDIS_PROPAGATE_NONE (no propagation of command at all)
 *   不传播
 *
 * + REDIS_PROPAGATE_AOF (propagate into the AOF file if is enabled)
 *   传播到 AOF
 *
 */
void propagate(struct redisCommand *cmd, int dbid, robj **argv, int argc,
	int flags)
{
	/* 传播到 AOF */
	if (server.aof_state != REDIS_AOF_OFF && flags & REDIS_PROPAGATE_AOF)
		feedAppendOnlyFile(cmd, dbid, argv, argc);
}

void call(redisClient *c, int flags) {
	long long dirty;
	/* 保留旧 dirty 计数器值 */
	dirty = server.dirty;
	/* 将命令复制到 AOF */
	if (flags & REDIS_CALL_PROPAGATE) {
		int flags = REDIS_PROPAGATE_NONE;

		/* 强制 AOF 传播 */
		if (c->flags & REDIS_FORCE_AOF) flags |= REDIS_PROPAGATE_AOF;

		/* 如果数据库有被修改，那么启用AOF 传播 */
		if (dirty)
			flags |= REDIS_PROPAGATE_AOF;

		if (flags != REDIS_PROPAGATE_NONE)
			propagate(c->cmd, c->db->id, c->argv, c->argc, flags);
	}
	c->cmd->proc(c); // 执行实现函数
}

/*
 * 这个函数执行时，我们已经读入了一个完整的命令到客户端，
 * 这个函数负责执行这个命令，
 * 或者服务器准备从客户端中进行一次读取。
 * 如果这个函数返回 1 ，那么表示客户端在执行命令之后仍然存在，
 * 调用者可以继续执行其他操作。
 * 否则，如果这个函数返回 0 ，那么表示客户端已经被销毁。
 */
int processCommand(redisClient *c) {
	// 查找命令，并进行命令合法性检查，以及命令参数个数检查
	
	/* 特别处理 quit 命令 */
	if (!strcasecmp(c->argv[0]->ptr, "quit")) {
		addReply(c, shared.ok);
		c->flags |= REDIS_CLOSE_AFTER_REPLY;
		return REDIS_ERR;
	}

	/* 查找命令，并进行命令合法性检查，以及命令参数个数检查 */
	c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
	if (!c->cmd) { /* 没有找到指定的命令 */
		flagTransaction(c);
		addReplyErrorFormat(c, "unknown command '%s'",
			(char*)c->argv[0]->ptr);
		return REDIS_OK;
	}
	else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
		(c->argc < -c->cmd->arity)) { /* 参数个数错误 */
		flagTransaction(c);
		addReplyErrorFormat(c, "wrong number of arguments for '%s' command",
			c->cmd->name);
		return REDIS_OK;
	}

	/* 如果服务器正在载入数据到数据库，那么只执行带有 REDIS_CMD_LOADING
	 * 标识的命令，否则将出错 */
	if (server.loading && !(c->cmd->flags & REDIS_CMD_LOADING)) {
		addReply(c, shared.loadingerr);
		return REDIS_OK;
	}

	if (c->flags & REDIS_MULTI &&
		c->cmd->proc != execCommand && c->cmd->proc != discardCommand &&
		c->cmd->proc != multiCommand && c->cmd->proc != watchCommand) {
		/* 在事务上下文中,除了EXEC, DISCARD, MULTI和WATCH命令之外,
		 * 其他所有的命令都会被入队到事务队列中 */
		queueMultiCommand(c);
		addReply(c, shared.queued);
	}
	else {
		call(c, REDIS_CALL_FULL);
	}
	return REDIS_OK;
}


static void sigtermHandler(int sig) {
	// todo
}

/*
 * 设置信号处理函数
 */
void setupSignalHandlers(void) {
	struct sigaction act;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	act.sa_handler = sigtermHandler;
	sigaction(SIGTERM, &act, NULL);

}

/* =========================== Server initialization ======================== */

/* 
 * 释放旧的 AOF 重写缓存，并初始化一个新的 AOF 缓存。
 *
 * 这个函数也可以单纯地用于 AOF 重写缓存的初始化。
 */
void aofRewriteBufferReset(void) {
	/* 释放旧的缓存(链表) */
	if (server.aof_rewrite_buf_blocks)
		listRelease(server.aof_rewrite_buf_blocks);
	/* 初始化新的缓存(链表) */
	server.aof_rewrite_buf_blocks = listCreate();
	listSetFreeMethod(server.aof_rewrite_buf_blocks, zfree); /* 设置用来释放内存的函数么? */
}

void createSharedObjects(void) {
	int j;
	// 常用的回复
	shared.crlf = createObject(REDIS_STRING, sdsnew("\r\n"));
	shared.ok = createObject(REDIS_STRING, sdsnew("+OK\r\n"));
	shared.err = createObject(REDIS_STRING, sdsnew("-ERR\r\n"));
	shared.emptybulk = createObject(REDIS_STRING, sdsnew("$0\r\n\r\n"));
	shared.czero = createObject(REDIS_STRING, sdsnew(":0\r\n"));
	shared.cone = createObject(REDIS_STRING, sdsnew(":1\r\n"));
	shared.cnegone = createObject(REDIS_STRING, sdsnew(":-1\r\n"));
	shared.nullbulk = createObject(REDIS_STRING, sdsnew("$-1\r\n"));
	shared.nullmultibulk = createObject(REDIS_STRING, sdsnew("*-1\r\n"));
	shared.emptymultibulk = createObject(REDIS_STRING, sdsnew("*0\r\n"));
	shared.pong = createObject(REDIS_STRING, sdsnew("+PONG\r\n"));
	shared.queued = createObject(REDIS_STRING, sdsnew("+QUEUED\r\n"));
	shared.emptyscan = createObject(REDIS_STRING, sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));

	// 常用错误回复
	shared.wrongtypeerr = createObject(REDIS_STRING, sdsnew(
		"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
	shared.nokeyerr = createObject(REDIS_STRING, sdsnew(
		"-ERR no such key\r\n"));
	shared.syntaxerr = createObject(REDIS_STRING, sdsnew(
		"-ERR syntax error\r\n"));
	shared.sameobjecterr = createObject(REDIS_STRING, sdsnew(
		"-ERR source and destination objects are the same\r\n"));
	shared.outofrangeerr = createObject(REDIS_STRING, sdsnew(
		"-ERR index out of range\r\n"));
	shared.noscripterr = createObject(REDIS_STRING, sdsnew(
		"-NOSCRIPT No matching script. Please use EVAL.\r\n"));
	shared.loadingerr = createObject(REDIS_STRING, sdsnew(
		"-LOADING Redis is loading the dataset in memory\r\n"));
	shared.slowscripterr = createObject(REDIS_STRING, sdsnew(
		"-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
	shared.masterdownerr = createObject(REDIS_STRING, sdsnew(
		"-MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no'.\r\n"));
	shared.bgsaveerr = createObject(REDIS_STRING, sdsnew(
		"-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk. Commands that may modify the data set are disabled. Please check Redis logs for details about the error.\r\n"));
	shared.roslaveerr = createObject(REDIS_STRING, sdsnew(
		"-READONLY You can't write against a read only slave.\r\n"));
	shared.noautherr = createObject(REDIS_STRING, sdsnew(
		"-NOAUTH Authentication required.\r\n"));
	shared.oomerr = createObject(REDIS_STRING, sdsnew(
		"-OOM command not allowed when used memory > 'maxmemory'.\r\n"));
	shared.execaborterr = createObject(REDIS_STRING, sdsnew(
		"-EXECABORT Transaction discarded because of previous errors.\r\n"));
	shared.noreplicaserr = createObject(REDIS_STRING, sdsnew(
		"-NOREPLICAS Not enough good slaves to write.\r\n"));
	shared.busykeyerr = createObject(REDIS_STRING, sdsnew(
		"-BUSYKEY Target key name already exists.\r\n"));

	// 常用字符
	shared.space = createObject(REDIS_STRING, sdsnew(" "));
	shared.colon = createObject(REDIS_STRING, sdsnew(":"));
	shared.plus = createObject(REDIS_STRING, sdsnew("+"));

	// 常用select命令
	for (j = 0; j < REDIS_SHARED_SELECT_CMDS; j++) {
		char dictid_str[64];
		int dictid_len;

		dictid_len = ll2string(dictid_str, sizeof(dictid_str), j);
		shared.select[j] = createObject(REDIS_STRING,
			sdscatprintf(sdsempty(),
				"*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
				dictid_len, dictid_str));
	}

	// 常用命令
	shared.del = createStringObject("DEL", 3);
	shared.rpop = createStringObject("RPOP", 4);
	shared.lpop = createStringObject("LPOP", 4);
	shared.lpush = createStringObject("LPUSH", 5);

	// 常用整数
	for (j = 0; j < REDIS_SHARED_INTEGERS; j++) {
		shared.integers[j] = createObject(REDIS_STRING, (void*)(long)j);
		shared.integers[j]->encoding = REDIS_ENCODING_INT;
	}

	// 常用长度bulk或者multi bulk回复
	for (j = 0; j < REDIS_SHARED_BULKHDR_LEN; j++) {
		shared.mbulkhdr[j] = createObject(REDIS_STRING,
			sdscatprintf(sdsempty(), "*%d\r\n", j));
		shared.bulkhdr[j] = createObject(REDIS_STRING,
			sdscatprintf(sdsempty(), "$%d\r\n", j));
	}

}

/*
 * 根据给定命令名字（C 字符串），查找命令
 */
struct redisCommand *lookupCommandByCString(char *s) {
	struct redisCommand *cmd;
	sds name = sdsnew(s);

	cmd = dictFetchValue(server.commands, name);
	sdsfree(name);
	return cmd;
}

void initServerConfig() {
	int j;

	// 服务器状态
	server.hz = REDIS_DEFAULT_HZ;
	server.port = REDIS_SERVERPORT; // 6379号端口监听
	server.tcp_backlog = REDIS_TCP_BACKLOG;
	server.bindaddr_count = 0;

	server.maxclients = REDIS_MAX_CLIENTS;
	server.maxidletime = REDIS_MAXIDLETIME;
	server.hash_max_ziplist_value = REDIS_HASH_MAX_ZIPLIST_VALUE; // 压缩链表所能容忍的最大值
	server.list_max_ziplist_value = REDIS_LIST_MAX_ZIPLIST_VALUE;
	server.list_max_ziplist_entries = REDIS_LIST_MAX_ZIPLIST_ENTRIES;
	server.set_max_intset_entries = REDIS_SET_MAX_INTSET_ENTRIES;
	server.zset_max_ziplist_value = REDIS_ZSET_MAX_ZIPLIST_VALUE;
	server.zset_max_ziplist_entries = REDIS_ZSET_MAX_ZIPLIST_ENTRIES;
	server.ipfd_count = 0;
	server.dbnum = REDIS_DEFAULT_DBNUM;
	server.tcpkeepalive = REDIS_DEFAULT_TCP_KEEPALIVE;
	server.commands = dictCreate(&commandTableDictType, NULL);
	server.orig_commands = dictCreate(&commandTableDictType, NULL);

	server.rdb_compression = REDIS_DEFAULT_RDB_COMPRESSION;
	server.rdb_checksum = REDIS_DEFAULT_RDB_CHECKSUM;
	server.rdb_filename = zstrdup(REDIS_DEFAULT_RDB_FILENAME);
	server.aof_filename = zstrdup(REDIS_DEFAULT_AOF_FILENAME); /* 默认的aof文件的名字 */
	server.aof_rewrite_incremental_fsync = REDIS_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC;
	server.loading = 0;
	server.loading_process_events_interval_bytes = (1024 * 1024 * 2);

	server.cronloops = 0;

	/* 与aof相关的变量的初始化 */
	server.aof_selected_db = -1; /* Make sure the first time will not match */
	server.rdb_child_pid = -1;
	server.aof_child_pid = -1;
	server.aof_buf = sdsempty(); /* aof的缓冲区 */
	aofRewriteBufferReset();
	server.aof_last_write_status = REDIS_OK;
	server.aof_last_write_errno = 0;
	server.aof_state = REDIS_AOF_ON; /* aof默认是关闭的 */
	server.aof_rewrite_perc = REDIS_AOF_REWRITE_PERC;
	server.aof_rewrite_min_size = REDIS_AOF_REWRITE_MIN_SIZE;
	server.aof_fd = -1;
	server.aof_rewrite_base_size = 0;
	server.aof_flush_postponed_start = 0;
	server.aof_fsync_strategy = REDIS_DEFAULT_AOF_FSYNC;
	server.aof_no_fsync_on_rewrite = REDIS_DEFAULT_AOF_NO_FSYNC_ON_REWRITE;
	server.aof_delayed_fsync = 0;
	server.aof_last_fsync = time(NULL);
	server.aof_rewrite_time_start = -1;
	/* 初始化浮点常量 */
	R_Zero = 0.0;
	R_PosInf = 1.0 / R_Zero;
	R_NegInf = -1.0 / R_Zero;
	R_Nan = R_Zero / R_Zero;
	populateCommandTable();  // 安装命令处理函数

	/* 如果AOF持久化功能已经打开,那么打开或创建一个 AOF 文件 */
	if (server.aof_state == REDIS_AOF_ON) {
		server.aof_fd = open(server.aof_filename,
			O_WRONLY | O_APPEND | O_CREAT, 0644);
		if (server.aof_fd == -1) {
			mylog("Can't open the append-only file: %s",
				strerror(errno));
			exit(1);
		}
	}

	/* 一些常用的命令 */
	server.multiCommand = lookupCommandByCString("multi");

	/* 初始化 BIO 系统 */
	bioInit();
}

/*
 * 在port端口监听
 */
int listenToPort(int port, int *fds, int *count) {
	int j;

	if (server.bindaddr_count == 0) server.bindaddr[0] = NULL;

	for (j = 0; j < server.bindaddr_count || j == 0; j++) {
		if (server.bindaddr[j] == NULL) {
			// 这里做了一些简化工作,那就是仅支持ipv4即可
			fds[*count] = anetTcpServer(server.neterr, port, NULL, server.tcp_backlog);
			if (fds[*count] != ANET_ERR) {
				anetNonBlock(NULL, fds[*count]); // 设置为非阻塞
				(*count)++;
			}
			if (*count) break;
		}
		else {
			// Bind IPv4 address.
			fds[*count] = anetTcpServer(server.neterr, port, server.bindaddr[j],
				server.tcp_backlog);
		}

		if (fds[*count] == ANET_ERR) {
			return REDIS_ERR;
		}
		anetNonBlock(NULL, fds[*count]);
		(*count)++;
	}
	return REDIS_OK;
}

/* 
 * 返回当前时间的us秒表示
 */
long long ustime(void) { 
	struct timeval tv;
	long long ust;
	gettimeofday(&tv, NULL);
	ust = ((long long)tv.tv_sec) * 1000000;
	ust += tv.tv_usec;
	return ust;
}

/*========================= helper function ===============================*/

/* After an RDB dump or AOF rewrite we exit from children using _exit() instead of
* exit(), because the latter may interact with the same file objects used by
* the parent process. However if we are testing the coverage normal exit() is
* used in order to obtain the right coverage information. */
void exitFromChild(int retcode) {
	_exit(retcode);
}


long long mstime(void) {
	return ustime() / 1000; // 1s = 1000 us
 }

void updateCachedTime(void) {
	server.unixtime = time(NULL);
	server.mstime = mstime();
}

/*================================== Shutdown =============================== */

/* 
 * Close listening sockets. Also unlink the unix domain socket if
 * unlink_unix_socket is non-zero. 
 */
void closeListeningSockets(int unlink_unix_socket) {
	int j;
	for (j = 0; j < server.ipfd_count; j++) close(server.ipfd[j]);
}

int prepareForShutdown(int flags) {
	// 关闭监听套接字,这样在重启的时候会快一点
	closeListeningSockets(1);
	return REDIS_OK;
}


/*
 * 释放客户端
 */
void freeClient(redisClient *c) {
	listNode *ln;
	if (server.current_client == c) {
		server.current_client = NULL;
	}

	sdsfree(c->querybuf);
	c->querybuf = NULL;

	// 关闭套接字,并从事件处理器中删除该套接字的事件
	if (c->fd != -1) {
		aeDeleteFileEvent(server.el, c->fd, AE_READABLE);
		aeDeleteFileEvent(server.el, c->fd, AE_WRITABLE);
		close(c->fd);
	}

	listRelease(c->reply); // 清空回复缓冲区
	freeClientArgv(c); // 清空命令参数

	// 从服务器的客户端链表中删除自身
	if (c->fd != -1) {
		ln = listSearchKey(server.clients, c);
		assert(ln != NULL);
		listDelNode(server.clients, ln);
	}

	if (c->name) decrRefCount(c->name);
	zfree(c->argv);
	freeClientMultiState(c); // 清除事务状态信息
	zfree(c);
	mylog("%s", "a client was closed");
}
/* ======================= Cron: called every 100 ms ======================== */

/* 
 * activeExpireCycle() 函数使用的检查键是否过期的辅佐函数。
 *
 * 如果 de 中的键已经过期，那么移除它，并返回 1 ，否则不做动作，并返回 0 。
 *
 * 参数 now 是毫秒格式的当前时间
 */
int activeExpireCycleTryExpire(redisDb *db, dictEntry *de, long long now) {
	/* 获取键的过期时间 */
	long long t = dictGetSignedIntegerVal(de);
	if (now > t) {
		/* 键已过期 */
		sds key = dictGetKey(de); /* 得到key,这是一个char *类型的东西 */
		robj *keyobj = createStringObject(key, sdslen(key));

		/* 从数据库中删除该键 */
		dbDelete(db, keyobj);
		decrRefCount(keyobj);
		return 1;
	}
	else {
		/* 键未过期 */
		return 0;
	}
}


/* 
 * 函数尝试删除数据库中已经过期的键。
 * 当带有过期时间的键比较少时，函数运行得比较保守，
 * 如果带有过期时间的键比较多，那么函数会以更积极的方式来删除过期键，
 * 从而可能地释放被过期键占用的内存。
 *
 * 每次循环中被测试的数据库数目不会超过 REDIS_DBCRON_DBS_PER_CALL 。
 *
 * 如果 timelimit_exit 为真，那么说明还有更多删除工作要做，
 * 那么在 beforeSleep() 函数调用时，程序会再次执行这个函数。
 *
 * 过期循环的类型：
 *
 * 如果循环的类型为 ACTIVE_EXPIRE_CYCLE_FAST ，
 * 那么函数会以“快速过期”模式执行，
 * 执行的时间不会长过 EXPIRE_FAST_CYCLE_DURATION 毫秒，
 * 并且在 EXPIRE_FAST_CYCLE_DURATION 毫秒之内不会再重新执行。
 *
 * 如果循环的类型为 ACTIVE_EXPIRE_CYCLE_SLOW ，
 * 那么函数会以“正常过期”模式执行，
 * 函数的执行时限为 REDIS_HS 常量的一个百分比，
 * 这个百分比由 REDIS_EXPIRELOOKUPS_TIME_PERC 定义。
 */

void activeExpireCycle(int type) {
	/* 静态变量，用来累积函数连续执行时的数据 */
	static unsigned int current_db = 0; /* Last DB tested. */
	static int timelimit_exit = 0;      /* Time limit hit in previous call? */
	static long long last_fast_cycle = 0; /* When last fast cycle ran. */

	unsigned int j, iteration = 0;
	/* 默认每次处理的数据库数量 */
	unsigned int dbs_per_call = REDIS_DBCRON_DBS_PER_CALL;
	/* 函数开始的时间 */
	long long start = ustime(), timelimit;

	if (type == ACTIVE_EXPIRE_CYCLE_FAST) { /* 快速模式 */
		/* 如果上次函数没有触发 timelimit_exit ，那么不执行处理 */
		if (!timelimit_exit) return;
		/* 如果距离上次执行未够一定时间，那么不执行处理 */
		if (start < last_fast_cycle + ACTIVE_EXPIRE_CYCLE_FAST_DURATION * 2) return;
		/* 运行到这里，说明执行快速处理，记录当前时间 */
		last_fast_cycle = start;
	}

	/* 
	* 一般情况下，函数只处理 REDIS_DBCRON_DBS_PER_CALL 个数据库，
	* 除非：
	*
	* 1) 当前数据库的数量小于 REDIS_DBCRON_DBS_PER_CALL
	*
	* 2) 如果上次处理遇到了时间上限，那么这次需要对所有数据库进行扫描，
	*    这可以避免过多的过期键占用空间
	*/
	if (dbs_per_call > server.dbnum || timelimit_exit)
		dbs_per_call = server.dbnum;

	/* 函数处理的微秒时间上限
	 * ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 默认为 25 ，也即是 25 % 的 CPU 时间 */
	timelimit = 1000000 * ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC / server.hz / 100;
	timelimit_exit = 0;
	if (timelimit <= 0) timelimit = 1;

	/* 如果是运行在快速模式之下
	 * 那么最多只能运行 FAST_DURATION 微秒 
	 * 默认值为 1000 （微秒） */
	if (type == ACTIVE_EXPIRE_CYCLE_FAST)
		timelimit = ACTIVE_EXPIRE_CYCLE_FAST_DURATION; /* in microseconds. */

	for (j = 0; j < dbs_per_call; j++) {
		int expired;
		/* 指向要处理的数据库 */
		redisDb *db = server.db + (current_db % server.dbnum);

		/* 为 DB 计数器加一，如果进入 do 循环之后因为超时而跳出
		 * 那么下次会直接从下个 DB 开始处理 */
		current_db++;

		do {
			unsigned long num, slots;
			long long now, ttl_sum;
			int ttl_samples;

			/* 获取数据库中带过期时间的键的数量
			 * 如果该数量为 0 ，直接跳过这个数据库 */
			if ((num = dictSize(db->expires)) == 0) {
				break;
			}
			/* 获取数据库中键值对的数量 */
			slots = dictSlots(db->expires);
			/* 当前时间 */
			now = mstime();

			/* 这个数据库的使用率低于 1% ，扫描起来太费力了（大部分都会 MISS）
			 * 跳过，等待字典收缩程序运行 */
			if (num && slots > DICT_HT_INITIAL_SIZE &&
				(num * 100 / slots < 1)) break;		

			/* 每次最多只能检查 LOOKUPS_PER_LOOP 个键 */
			if (num > ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP)
				num = ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP;

			while (num--) {
				dictEntry *de;
				long long ttl;

				/* 从 expires 中随机取出一个带过期时间的键 */
				if ((de = dictGetRandomKey(db->expires)) == NULL) break;
				/* 计算 TTL */
				ttl = dictGetSignedIntegerVal(de) - now;
				/* 如果键已经过期，那么删除它，并将 expired 计数器增一 */
				if (activeExpireCycleTryExpire(db, de, now)) expired++;
				if (ttl < 0) ttl = 0;
			}

			/* 我们不能用太长时间处理过期键，
			 * 所以这个函数执行一定时间之后就要返回 */

			/* 更新遍历次数 */
			iteration++;

			/* 每遍历 16 次执行一次 */
			if ((iteration & 0xf) == 0 && /* check once every 16 iterations. */
				(ustime() - start) > timelimit)
			{
				/* 如果遍历次数正好是 16 的倍数
				 * 并且遍历的时间超过了 timelimit
				 * 那么断开 timelimit_exit */
				timelimit_exit = 1;
			}

			/* 已经超时了，返回 */
			if (timelimit_exit) return;

			/* 如果已删除的过期键占当前总数据库带过期时间的键数量的 25 %
			 * 那么不再遍历 */
		} while (expired > ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP / 4);
	}
}

/* 对数据库执行删除过期键，调整大小，以及主动和渐进式 rehash */
void databasesCron(void) {
	/* 函数先从数据库中删除过期键，然后再对数据库的大小进行修改 */
	activeExpireCycle(ACTIVE_EXPIRE_CYCLE_SLOW);
}


/* 
 * 检查客户端是否已经超时,如果超时就关闭客户端,并返回1 
 */
int clientsCronHandleTimeout(redisClient *c) {
	time_t now = server.unixtime; // 获取当前的时间
	if (now - c->lastinteraction > server.maxidletime) {
		// 客户端最后一个与服务器通讯的时间已经超过了maxidletime
		mylog("%s", "Closing idle client");
		freeClient(c); // 关闭超时客户端 
		return 1;
	}
	return 0;
}

void clientsCron(void) {
	// todo
	int numclients = listLength(server.clients); // 客户端的数量
	int iterations = numclients / (server.hz * 10); // 要处理的客户端的数量

	while (listLength(server.clients) && iterations--) {
		redisClient *c;
		listNode *head;
		// 翻转列表,然后取出表头元素,这样以来上一个被处理的客户端就会被放到表头
		// 另外,如果程序要删除当前客户端,那么只需要删除表头元素就可以了.
		listRotate(server.clients);
		head = listFirst(server.clients);
		c = listNodeValue(head);
		if (clientsCronHandleTimeout(c)) continue;
	}
}

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
	/* 会以一定的频率来运行这个函数 */

	int j;
	updateCachedTime();
	// 服务器进程收到 SIGTERM 消息,关闭服务器
	if (server.shutdown_asap) {
		// 尝试关闭服务器
		if (prepareForShutdown(0) == REDIS_OK) exit(0);
		// 运行到这里说明关闭失败
		server.shutdown_asap = 0;
	}
	// 检查客户端,关闭超时的客户端,并释放客户端多余的缓冲区
	clientsCron();
	databasesCron(); /* 对数据库执行各种操作 */

	/* 如果 BGSAVE 和 BGREWRITEAOF 都没有在执行
	 * 并且有一个 BGREWRITEAOF 在等待，那么执行 BGREWRITEAOF */
	if (server.rdb_child_pid == -1 && server.aof_child_pid == -1 &&
		server.aof_rewrite_scheduled) {
		rewriteAppendOnlyFileBackground(); /* 开启一个进程重写 aof 文件 */
	}

	/* 检查 BGSAVE 或者 BGREWRITEAOF 是否已经执行完毕 */
	if (server.rdb_child_pid != -1 || server.aof_child_pid != -1) {
		int statloc;
		pid_t pid;
		/* 接收子进程发来的信号，非阻塞 */
		if ((pid = wait3(&statloc, WNOHANG, NULL)) != 0) {
			int exitcode = WEXITSTATUS(statloc); /* 得到退出码 */
			int bysignal = 0;
			if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

			if (pid == server.rdb_child_pid) { /* BGSAVE 执行完毕 */
				backgroundSaveDoneHandler(exitcode, bysignal);
			}
			else if (pid == server.aof_child_pid) { /* BGREWRITEAOF 执行完毕 */
				backgroundRewriteDoneHandler(exitcode, bysignal);
			}
			else {
				mylog("Warning, detected child with unmatched pid: %ld", (long)pid);
			}
			updateDictResizePolicy();
		}
	}
	else { /* 既然没有 BGSAVE 或者 BGREWRITEAOF 在执行，那么检查是否需要执行它们 */
		/* 触发 BGREWRITEAOF */
		if (server.rdb_child_pid == -1 &&
			server.aof_child_pid == -1 &&
			server.aof_rewrite_perc &&
			server.aof_current_size > server.aof_rewrite_min_size) {
			/* 上一次完成AOF写入之后,AOF文件的大小 */
			long long base = server.aof_rewrite_base_size ?
				server.aof_rewrite_base_size : 1;
			/* AOF 文件当前的体积相对于base的体积的百分比 */
			long long growth = (server.aof_current_size * 100 / base) - 100;
			/* 如果增长体积的百分比超过了 growth ，那么执行 BGREWRITEAOF */
			if (growth >= server.aof_rewrite_perc) {
				mylog("Starting automatic rewriting of AOF on %lld%% growth", growth);
				rewriteAppendOnlyFileBackground(); /* 这和前面执行的函数倒是没有多么大的区别 */
			}
		}
	}

	/* 根据 AOF 政策， 考虑是否需要将 AOF 缓冲区中的内容写入到 AOF 文件中 */
	if (server.aof_flush_postponed_start) flushAppendOnlyFile(0);

	run_with_period(1000) { /* 每隔一段周期来运行一次 */
		if (server.aof_last_write_status == REDIS_ERR)
			flushAppendOnlyFile(0);
	}

	/* 关闭那些需要异步关闭的客户端 */
	freeClientsInAsyncFreeQueue();
	server.cronloops++; /* 增加 loop 计数器 */
	return 1000 / server.hz; /* 这个返回的值决定了下次什么时候再调用这个函数 */
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);


void initServer() {
	int j;
	// 忽略SIGPIPE以及SIGHUP两个消息
	signal(SIGHUP, SIG_IGN);
	signal(SIGPIPE, SIG_IGN);
	setupSignalHandlers();

	// 初始化并创建数据结构
	server.clients = listCreate();
	server.clients_to_close = listCreate();

	// 创建共享对象
	createSharedObjects();

	server.el = aeCreateEventLoop(server.maxclients + REDIS_EVENTLOOP_FDSET_INCR);
	server.db = zmalloc(sizeof(redisDb) * server.dbnum); // 创建数据库

	// 打开 TCP 监听端口,用于等待客户端的命令请求
	if (server.port != 0 &&
		listenToPort(server.port, server.ipfd, &server.ipfd_count) == REDIS_ERR) {
		exit(1);
	}
	
	for (j = 0; j < server.dbnum; j++) {
		server.db[j].dict = dictCreate(&dbDictType, NULL);
		server.db[j].expires = dictCreate(&keyptrDictType, NULL);
		server.db[j].watched_keys = dictCreate(&keylistDictType, NULL);
		server.db[j].id = j;
	}

	server.rdb_child_pid = -1;
	/* 为serverCron() 创建时间事件 */
	if (aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL) == AE_ERR) {
		exit(1);
	}

	// 为 TCP 连接关联应答(accept)处理器,用于接收并应答客户端的connect()调用
	for (j = 0; j < server.ipfd_count; j++) {
		if (aeCreateFileEvent(server.el, server.ipfd[j], AE_READABLE,
			acceptTcpHandler, NULL) == AE_ERR) {
			mylog("%s", "createFileEvent error!");
			exit(-1);
		}
	}
}

/* Function called at startup to load RDB or AOF file in memory. */
void loadDataFromDisk(void) {
	/* 记录开始时间 */
	long long start = ustime();

	if (server.aof_state == REDIS_AOF_ON) { /* AOF 持久化已打开？ */
		if (loadAppendOnlyFile(server.aof_filename) == REDIS_OK) /* 尝试载入 AOF 文件 */
			/* 打印载入信息，并计算载入耗时长度 */
			mylog("DB loaded from append only file: %.3f seconds", (float)(ustime() - start) / 1000000);
	} 
	else {
		if (rdbLoad(server.rdb_filename) == REDIS_OK) { /* 尝试载入 RDB 文件 */
			/* 打印载入信息，并计算载入耗时长度 */
			mylog("DB loaded from disk: %.3f seconds",
				(float)(ustime() - start) / 1000000);
		}
		else if (errno != ENOENT) {
			mylog("Fatal error loading the DB: %s. Exiting.", strerror(errno));
			exit(1);
		}
	}	
}


/* This function gets called every time Redis is entering the
* main loop of the event driven library, that is, before to sleep
* for ready file descriptors. */
void beforeSleep(struct aeEventLoop *eventLoop) {
	REDIS_NOTUSED(eventLoop);

	/* Run a fast expire cycle (the called function will return
	* ASAP if a fast cycle is not needed). */
	
	activeExpireCycle(ACTIVE_EXPIRE_CYCLE_FAST);

	/* 将 AOF 缓冲区的内容写入到 AOF 文件 */
	flushAppendOnlyFile(0);
}

int main(int argc, char **argv) {
	initServerConfig();
	initServer();
	/* 从 AOF 文件或者 RDB 文件中载入数据 */
	loadDataFromDisk();
	/* 运行事件处理器,一直到服务器关闭为止 */
	aeSetBeforeSleepProc(server.el, beforeSleep);
	aeMain(server.el);
	/* 服务器关闭，停止事件循环 */
	aeDeleteEventLoop(server.el);
	return 0;
}