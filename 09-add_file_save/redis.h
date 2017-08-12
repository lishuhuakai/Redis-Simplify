#ifndef __REDIS_H_
#define __REDIS_H_

#include "fmacros.h"
#include "dict.h"
#include "adlist.h"
#include "bio.h"
#include "rdb.h"
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
#define REDIS_LONGSTR_SIZE      21          /* Bytes needed for long -> str */

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
#define REDIS_MAXIDLETIME       0			 /* default client timeout: infinite */
#define REDIS_DEFAULT_RDB_COMPRESSION 1
#define REDIS_DEFAULT_RDB_CHECKSUM 1
#define REDIS_AOF_REWRITE_PERC  100
#define REDIS_AOF_REWRITE_MIN_SIZE (64*1024*1024)
#define REDIS_AOF_REWRITE_ITEMS_PER_CMD 64
#define REDIS_DEFAULT_RDB_FILENAME "dump.rdb"
#define REDIS_DEFAULT_AOF_FILENAME "appendonly.aof"
#define REDIS_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC 1
#define REDIS_DEFAULT_AOF_NO_FSYNC_ON_REWRITE 0

/* 指示 AOF 程序每累积这个量的写入数据
 * 就执行一次显式的 fsync */
#define REDIS_AOF_AUTOSYNC_BYTES (1024*1024*32) /* fdatasync every 32MB */

/* Client request types */
#define REDIS_REQ_INLINE	1
#define REDIS_REQ_MULTIBULK 2 /* 多条查询 */


/* Units */
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

/* 对象编码 */
#define REDIS_ENCODING_RAW 0     /* Raw representation */
#define REDIS_ENCODING_INT 1     /* Encoded as integer */
#define REDIS_ENCODING_HT 2      /* Encoded as hash table */
#define REDIS_ENCODING_ZIPMAP 3  /* Encoded as zipmap */
#define REDIS_ENCODING_LINKEDLIST 4 /* Encoded as regular linked list */
#define REDIS_ENCODING_ZIPLIST 5 /* Encoded as ziplist */
#define REDIS_ENCODING_INTSET 6
#define REDIS_ENCODING_SKIPLIST 7  /* Encoded as skiplist */
#define REDIS_ENCODING_EMBSTR 8  /* Embedded sds string encoding */

/* List related stuff */
#define REDIS_HEAD 0
#define REDIS_TAIL 1

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

/* Zip structure related defaults */
#define REDIS_HASH_MAX_ZIPLIST_VALUE 64
#define REDIS_HASH_MAX_ZIPLIST_ENTRIES 512  // 压缩链表最多能有512项
#define REDIS_LIST_MAX_ZIPLIST_VALUE 64
#define REDIS_LIST_MAX_ZIPLIST_ENTRIES 512
#define REDIS_SET_MAX_INTSET_ENTRIES 512
#define REDIS_ZSET_MAX_ZIPLIST_ENTRIES 128
#define REDIS_ZSET_MAX_ZIPLIST_VALUE 64

/* Command call flags, see call() function */
#define REDIS_CALL_NONE 0
#define REDIS_CALL_SLOWLOG 1
#define REDIS_CALL_STATS 2
#define REDIS_CALL_PROPAGATE 4
#define REDIS_CALL_FULL (REDIS_CALL_SLOWLOG | REDIS_CALL_STATS | REDIS_CALL_PROPAGATE)

#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^32 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */

/* Client flags */
#define REDIS_CLOSE_ASAP (1<<10)/* Close this client ASAP */
#define REDIS_MULTI (1<<3)   /* This client is in a MULTI context */
#define REDIS_FORCE_AOF (1<<14)   /* Force AOF propagation of current cmd. */

/* Sets operations codes */
#define REDIS_OP_UNION 0
#define REDIS_OP_DIFF 1
#define REDIS_OP_INTER 2

/* Anti-warning macro... */
#define REDIS_NOTUSED(V) ((void) V)

#define REDIS_DBCRON_DBS_PER_CALL 16

#define ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP 20 /* Loopkups per loop. */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000 /* Microseconds */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25 /* CPU max % for keys collection */
#define ACTIVE_EXPIRE_CYCLE_SLOW 0
#define ACTIVE_EXPIRE_CYCLE_FAST 1

/* AOF states */
#define REDIS_AOF_OFF 0             /* AOF is off */
#define REDIS_AOF_ON 1              /* AOF is on */
#define REDIS_AOF_WAIT_REWRITE 2    /* AOF waits rewrite to start appending */

/* Append only defines */
#define AOF_FSYNC_NO 0
#define AOF_FSYNC_ALWAYS 1
#define AOF_FSYNC_EVERYSEC 2
#define REDIS_DEFAULT_AOF_FSYNC AOF_FSYNC_EVERYSEC

/* Command propagation flags, see propagate() function */
#define REDIS_PROPAGATE_NONE 0
#define REDIS_PROPAGATE_AOF 1

typedef long long mstime_t;

/*====================================== define marco ===================================*/
/* 用于判断objptr是否为sds */
#define sdsEncodedObject(objptr) (objptr->encoding == REDIS_ENCODING_RAW || objptr->encoding == REDIS_ENCODING_EMBSTR)

/* Using the following macro you can run code inside serverCron() with the
* specified period, specified in milliseconds.
* The actual resolution depends on server.hz. */
#define run_with_period(_ms_) if ((_ms_ <= 1000/server.hz) || !(server.cronloops%((_ms_)/(1000/server.hz))))

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
	dict *expires;				// 键的过期时间,字典的键为键,字典的值为过期事件 UNIX 时间戳
	int id;                     // 数据库号码
} redisDb;

/*
 * 事务命令
 */
typedef struct multiCmd {
	/* 参数 */
	robj **argv;
	/* 参数数量 */
	int argc;
	/* 命令指针 */
	struct redisCommand *cmd;
} multiCmd;

/*
* 事务状态
*/
typedef struct multiState {

	/* 事务队列，FIFO 顺序 */
	multiCmd *commands;     /* Array of MULTI commands */
	int count;              /* 已入队命令计数 */
	int minreplicas;        /* MINREPLICAS for synchronous replication */
	time_t minreplicas_timeout; /* MINREPLICAS timeout as unixtime. */
} multiState;

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
	/* 客户端状态标志 */
	int flags;              /* REDIS_SLAVE | REDIS_MONITOR | REDIS_MULTI ... */

	multiState mstate;      /* MULTI/EXEC state */
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
	size_t hash_max_ziplist_value;
	size_t hash_max_ziplist_entries;
	size_t list_max_ziplist_value;
	size_t list_max_ziplist_entries;
	size_t set_max_intset_entries;
	size_t zset_max_ziplist_entries;
	size_t zset_max_ziplist_value;

	/* 有关于数据库存储的一些量 */
	pid_t rdb_child_pid;   /* PID of RDB saving child */
	char *rdb_filename;             /* Name of RDB file */
	int rdb_compression;            /* Use compression in RDB? */
	int rdb_checksum;               /* Use RDB checksum? */

	/* 一些关于数据库文件存储加载的变量 */
	int loading;					/* We are loading data from disk if true */
	off_t loading_total_bytes;		/* 正在载入的数据大小 */
	off_t loading_loaded_bytes;		/* 已载入的数据的大小 */

	time_t loading_start_time;		/* 开始进行载入的时间 */
	off_t loading_process_events_interval_bytes;

	int cronloops;					/* serverCron()函数的运行次数计数器 */

	/* aof相关的变量 */
	int aof_state;                  /* REDIS_AOF_(ON|OFF|WAIT_REWRITE) */
	char *aof_filename;             /* Name of the AOF file */
	int aof_rewrite_perc;           /* Rewrite AOF if % growth is > M and... */
	off_t aof_rewrite_min_size;     /* the AOF file is at least N bytes. */
	off_t aof_current_size;         /* AOF 文件的当前字节大小. */
	int aof_rewrite_scheduled;      /* Rewrite once BGSAVE terminates. */
	pid_t aof_child_pid;            /* 负责进行 AOF 重写的子进程 ID */
	list *aof_rewrite_buf_blocks;   /* AOF 重写缓存链表，链接着多个缓存块. */
	sds aof_buf;      /* AOF 缓冲区, written before entering the event loop */
	int aof_fd;       /* AOF 文件的描述符 */
	int aof_last_write_status;      /* REDIS_OK or REDIS_ERR */
	int aof_last_write_errno;       /* Valid if aof_last_write_status is ERR */
	int aof_selected_db;		    /* AOF 的当前目标数据库 */
	int aof_rewrite_incremental_fsync; /* 指示是否需要每写入一定量的数据，就主动执行一次 fsync() */

	off_t aof_rewrite_base_size;    /* 最后一次执行 BGREWRITEAOF 时, AOF 文件的大小. */
	time_t aof_flush_postponed_start; /* 推迟 write 操作的时间 */
	int aof_fsync_strategy;			/* 文件同步策略 */
	int aof_no_fsync_on_rewrite;    /* Don't fsync if a rewrite is in prog. */
								
	long long dirty;                /* 自从上次 SAVE 执行以来，数据库被修改的次数 */
	unsigned long aof_delayed_fsync; /* 记录 AOF 的 write 操作被推迟了多少次 */
	time_t aof_last_fsync;           /* 最后一直执行 fsync 的时间 */
	time_t aof_rewrite_time_start;	 /* AOF 重写的开始时间 */
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
	/* Use a function to determine keys arguments in a command line.
	* Used for Redis Cluster redirect. */
	
	// 从命令中判断命令的键参数。在 Redis 集群转向时使用。
	redisGetKeysProc *getkeys_proc;

	/* What keys should be loaded in background when calling this command? */
	// 指定哪些参数是 key
	int firstkey; /* The first argument that's a key (0 = no keys) */
	int lastkey;  /* The last argument that's a key */
	int keystep;  /* The step between first and last key */

	// 统计信息
	// microseconds 记录了命令执行耗费的总毫微秒数
	// calls 是命令被执行的总次数
	long long microseconds, calls;
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

//
// hashTypeIterator 哈希对象的迭代器
//
typedef struct {
	// 被迭代的哈希对象
	robj *subject;
	// 哈希对象的编码
	int encoding;
	// 域指针和值指针
	unsigned char *fptr, *vptr;
	// 字典迭代器和指向当前迭代字典节点的指针,在迭代HT编码的哈希对象时使用
	dictIterator *di;
	dictEntry *de;
} hashTypeIterator;

#define REDIS_HASH_KEY 1
#define REDIS_HASH_VALUE 2


/*
 * 列表迭代器对象
 */
typedef struct {
	/* 列表对象 */
	robj *subject;
	/* 对象所使用的编码 */
	unsigned char encoding;
	/* 迭代的方向 */
	unsigned char direction;
	/* ziplist索引,迭代ziplist编码的列表时使用 */
	unsigned char *zi;
	/* 链表节点的指针,迭代双端链表编码的列表时使用 */
	listNode *ln;
} listTypeIterator;

/* 
 * 迭代列表时使用的记录结构，
 * 用于保存迭代器，以及迭代器返回的列表节点。
 */
typedef struct {

	/* 列表迭代器 */
	listTypeIterator *li;

	/* ziplist 节点索引 */
	unsigned char *zi;  

	/* 双端链表节点指针 */
	listNode *ln; 
} listTypeEntry;

/*
 * 多态集合迭代器
 */
typedef struct {

	/* 被迭代的对象 */
	robj *subject;

	/* 对象的编码 */
	int encoding;

	/* 索引值，编码为 intset 时使用 */
	int ii;

	/* 字典迭代器，编码为 HT 时使用 */
	dictIterator *di;

} setTypeIterator;
/*
 * 跳跃表
 */
typedef struct zskiplist {

	/* 表头节点和表尾节点 */
	struct zskiplistNode *header, *tail;

	/* 表中节点的数量 */
	unsigned long length;

	/* 表中层数最大的节点的层数 */
	int level;

} zskiplist;

/*
 * 有序集合
 */
typedef struct zset {

	/* 字典，键为成员，值为分值
	 * 用于支持 O(1) 复杂度的按成员取分值操作 */
	dict *dict;

	/* 跳跃表，按分值排序成员
	 * 用于支持平均复杂度为 O(log N) 的按分值定位成员操作
	 * 以及范围操作 */
	zskiplist *zsl;

} zset;


/*
 * 跳跃表节点
 */
typedef struct zskiplistNode {

	/* 成员对象 */
	robj *obj;

	/* 分值 */
	double score;

	/* 后退指针 */
	struct zskiplistNode *backward;

	/* 层 */
	struct zskiplistLevel {

		/* 前进指针 */
		struct zskiplistNode *forward;

		/* 跨度 */
		unsigned int span;

	} level[];

} zskiplistNode;
 
/* 表示开区间/闭区间范围的结构 */
typedef struct {

	/* 最小值和最大值 */
	double min, max;

	/* 指示最小值和最大值是否*不*包含在范围之内
	 * 值为 1 表示不包含，值为 0 表示包含 */
	int minex, maxex; /* are min or max exclusive? */
} zrangespec;

/* Struct to hold an inclusive/exclusive range spec by lexicographic comparison. */
typedef struct {
	robj *min, *max;  /* May be set to shared.(minstring|maxstring) */
	int minex, maxex; /* are min or max exclusive? */
} zlexrangespec;

/* api */
int processCommand(redisClient *c);
void freeClient(redisClient *c);
void initClientMultiState(redisClient *c);  /* mult.c */
long long mstime(void);
long long ustime(void);
void exitFromChild(int retcode);
void updateDictResizePolicy(void);
void freeClientMultiState(redisClient *c);
void closeListeningSockets(int unlink_unix_socket);
struct redisCommand *lookupCommand(sds name);
void aofRewriteBufferReset(void);
void saveCommand(redisClient *c);
#endif