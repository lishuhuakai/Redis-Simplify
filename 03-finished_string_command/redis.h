#ifndef __REDIS_H_
#define __REDIS_H_

#include "fmacros.h"
#include "dict.h"
#include "adlist.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <signal.h>
#include <assert.h>
#include "anet.h"
#include "ae.h"
#include "sds.h"
#include "zmalloc.h"

/* Error codes */
#define REDIS_OK				0
#define REDIS_ERR				-1

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
// 对象编码
#define REDIS_ENCODING_RAW 0     /* Raw representation */

/* 对象类型 */
#define REDIS_STRING 0 
#define REDIS_LIST 1
#define REDIS_SET 2
#define REDIS_ZSET 3
#define REDIS_HASH 4

/* Protocol and I/O related defines */
#define REDIS_REPLY_CHUNK_BYTES (16*1024) /* 16k output buffer */
#define REDIS_IOBUF_LEN         (1024*16)  /* Generic I/O buffer size */
#define REDIS_INLINE_MAX_SIZE   (1024*64) /* Max size of inline reads */
#define REDIS_MBULK_BIG_ARG     (1024*32)
#define REDIS_MIN_RESERVED_FDS 32
#define REDIS_EVENTLOOP_FDSET_INCR (REDIS_MIN_RESERVED_FDS+96)
#define REDIS_MAX_CLIENTS 10000 /* 最大所支持的用户数目 */

/* Static server configuration */
#define REDIS_DEFAULT_HZ        10 
#define REDIS_SERVERPORT		6379 /* TCP port */
#define REDIS_TCP_BACKLOG       511     /* TCP listen backlog */
#define REDIS_BINDADDR_MAX		16
#define REDIS_IP_STR_LEN INET6_ADDRSTRLEN
#define REDIS_DEFAULT_DBNUM     16
#define REDIS_DEFAULT_TCP_KEEPALIVE 0
#define REDIS_SHARED_SELECT_CMDS 10
#define REDIS_SHARED_INTEGERS 10000
#define REDIS_SHARED_BULKHDR_LEN 32
#define REDIS_MAXIDLETIME       0       /* default client timeout: infinite */

/* Client request types */
#define REDIS_REQ_INLINE	1
#define REDIS_REQ_MULTIBULK 2 /* 多条查询 */


/* Units */
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

/* 对象编码 */
#define REDIS_ENCODING_RAW 0     /* Raw representation */
#define REDIS_ENCODING_INT 1     /* Encoded as integer */
#define REDIS_ENCODING_EMBSTR 8  /* Embedded sds string encoding */

/* 命令标志 */
#define REDIS_CMD_WRITE 1                   /* "w" flag */
#define REDIS_CMD_READONLY 2                /* "r" flag */
#define REDIS_CMD_DENYOOM 4                 /* "m" flag */
#define REDIS_CMD_NOT_USED_1 8              /* no longer used flag */
#define REDIS_CMD_ADMIN 16                  /* "a" flag */
#define REDIS_CMD_PUBSUB 32                 /* "p" flag */
#define REDIS_CMD_NOSCRIPT  64              /* "s" flag */
#define REDIS_CMD_RANDOM 128                /* "R" flag */
#define REDIS_CMD_SORT_FOR_SCRIPT 256       /* "S" flag */
#define REDIS_CMD_LOADING 512               /* "l" flag */
#define REDIS_CMD_STALE 1024                /* "t" flag */
#define REDIS_CMD_SKIP_MONITOR 2048         /* "M" flag */
#define REDIS_CMD_ASKING 4096               /* "k" flag */

/* Command call flags, see call() function */
#define REDIS_CALL_NONE 0
#define REDIS_CALL_SLOWLOG 1
#define REDIS_CALL_STATS 2
#define REDIS_CALL_PROPAGATE 4
#define REDIS_CALL_FULL (REDIS_CALL_SLOWLOG | REDIS_CALL_STATS | REDIS_CALL_PROPAGATE)


/*====================================== define marco ===================================*/
/* 用于判断objptr是否为sds */
#define sdsEncodedObject(objptr) (objptr->encoding == REDIS_ENCODING_RAW || objptr->encoding == REDIS_ENCODING_EMBSTR)

/*
* Redis 对象
*/
#define REDIS_LRU_BITS 24

//
// redisObject Redis对象
// 
typedef struct redisObject {

	unsigned type : 4; // 类型

	unsigned encoding : 4; // 编码

	int refcount; // 引用计数

	void *ptr; // 指向实际值的指针
} robj;


typedef struct redisDb {
	dict *dict;                 // 数据库键空间，保存着数据库中的所有键值对
	int id;                     // 数据库号码
} redisDb;

/*
 * 因为 I/O 复用的缘故，需要为每个客户端维持一个状态。
 *
 * 多个客户端状态被服务器用链表连接起来。
 */
typedef struct redisClient {
	int fd; //  套接字描述符

	redisDb *db; // 当前正在使用的数据库

	int dictid; //  当前正在使用的数据库的 id （号码）

	robj *name; // 客户端的名字

	sds querybuf; // 查询缓冲区

	int argc; // 参数数量

	robj **argv; // 参数对象数组

	struct redisCommand *cmd, *lastcmd; // 记录被客户端执行的命令

	int reqtype; // 请求的类型,是内联命令还是多条命令 

	int multibulklen; // 剩余未读取的命令内容数量

	long bulklen; // 命令内容的长度

	list *reply; // 回复链表

	int sentlen; // 已发送字节,处理short write时使用

	unsigned long reply_bytes; // 回复链表中对象的总大小

	int bufpos; // 回复偏移量

	char buf[REDIS_REPLY_CHUNK_BYTES];

	time_t lastinteraction; // 客户端最后一次和服务器互动的时间

} redisClient;


struct redisServer {

	/* General */
	char *configfile;   // 配置文件的绝对路径,要么就是NULL

	int hz;             // serverCron() 每秒调用的次数

	redisDb *db;		// 一个数组,保存着服务器中所有的数据库

	dict *commands;     // 命令表（受到 rename 配置选项的作用）

	dict *orig_commands;        // Command table before command renaming.

	aeEventLoop *el; // 事件状态

	int shutdown_asap; // 关闭服务器的标识

	int port;					// TCP 监听端口
	int tcp_backlog;			// TCP listen() backlog
	char *bindaddr[REDIS_BINDADDR_MAX]; // ip地址
	int bindaddr_count; // 地址的数量

	int ipfd[REDIS_BINDADDR_MAX];  // TCP 描述符
	int ipfd_count;				   // 已经使用了的描述符的数目

	list *clients;	// 一个链表,保存了所有的客户端状态结构

	list *clients_to_close; // 链表,保存了所有待关闭的客户端

	redisClient *current_client; // 服务器当前服务的客户端,仅用于崩溃报告
	
	char neterr[ANET_ERR_LEN]; // 用于记录网络错误 

	int tcpkeepalive;	// 是否开启 SO_KEEPALIVE选项
	int dbnum;			// 数据库的总数目

	/* Limits */
	int maxclients;   // Max number of simultaneous clients
	int maxidletime; // 客户端的最大空转时间

	time_t unixtime; // 记录时间
	long long mstime; // 这个精度要高一些
};


typedef void redisCommandProc(redisClient *c);
typedef int *redisGetKeysProc(struct redisCommand *cmd, robj **argv, int argc, int *numkeys);

//
// redisCommand Redis 命令
//
struct redisCommand {

	// 命令名字
	char *name;

	// 实现函数
	redisCommandProc *proc;

	// 参数个数
	int arity;

	// 字符串表示的 FLAG
	char *sflags; /* Flags as string representation, one char per flag. */

	int flags; // 实际flag 

	// 做了一些简化,删除了一些不常用的域
};


// 通过复用来减少内存碎片，以及减少操作耗时的共享对象
struct sharedObjectsStruct {
	robj *crlf, *ok, *err, *emptybulk, *czero, *cone, *cnegone, *pong, *space,
		*colon, *nullbulk, *nullmultibulk, *queued,
		*emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
		*outofrangeerr, *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr,
		*masterdownerr, *roslaveerr, *execaborterr, *noautherr, *noreplicaserr,
		*busykeyerr, *oomerr, *plus, *del, *rpop, *lpop,
		*lpush, *emptyscan, *minstring, *maxstring,
		*select[REDIS_SHARED_SELECT_CMDS],
		*integers[REDIS_SHARED_INTEGERS],
		*mbulkhdr[REDIS_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
		*bulkhdr[REDIS_SHARED_BULKHDR_LEN];  /* "$<value>\r\n" */
};

/* api */
int processCommand(redisClient *c);
void freeClient(redisClient *c);
#endif