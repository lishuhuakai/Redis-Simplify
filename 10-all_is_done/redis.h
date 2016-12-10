#ifndef __REDIS_H_
#define __REDIS_H_

#include "fmacros.h"
#include "dict.h"
#include "bio.h"
#include "rdb.h"
#include <assert.h>
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
#include "zmalloc.h"
#include "util.h"
#include "anet.h"
#include "ae.h"
#include "adlist.h"
#include "sds.h"
#include "g_var.h"

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

#define REDIS_HASH_KEY 1
#define REDIS_HASH_VALUE 2

typedef struct redisObject {
	unsigned type : 4; /* 类型 */
	unsigned encoding : 4; /* 编码 */
	int refcount; /* 引用计数 */
	void *ptr; /* 指向实际值的指针 */
} robj;

typedef struct redisDb {
	dict *dict;                 /* 数据库键空间，保存着数据库中的所有键值对 */
	dict *expires;				/* 键的过期时间,字典的键为键,字典的值为过期事件 UNIX 时间戳 */
	dict *watched_keys;			/* 正在被watch命令监视的键 */
	int id;                     /* 数据库号码 */
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
	int fd; /*  套接字描述符 */
	redisDb *db; /* 当前正在使用的数据库 */
	int dictid; /*  当前正在使用的数据库的 id （号码） */
	robj *name; /* 客户端的名字 */
	sds querybuf; /* 查询缓冲区 */
	int argc; /* 参数数量 */
	robj **argv; /* 参数对象数组 */
	struct redisCommand *cmd, *lastcmd; /* 记录被客户端执行的命令 */
	int reqtype; /* 请求的类型,是内联命令还是多条命令 */
	int multibulklen; /* 剩余未读取的命令内容数量 */
	long bulklen; /* 命令内容的长度 */
	list *reply; /* 回复链表 */

	int sentlen; /* 已发送字节,处理short write时使用 */

	unsigned long reply_bytes; /* 回复链表中对象的总大小 */

	int bufpos; /* 回复偏移量 */

	char buf[REDIS_REPLY_CHUNK_BYTES];

	time_t lastinteraction; /* 客户端最后一次和服务器互动的时间 */
	/* 客户端状态标志 */
	int flags;              /* REDIS_SLAVE | REDIS_MONITOR | REDIS_MULTI ... */

	multiState mstate;      /* MULTI/EXEC state */

	list *watched_keys;	    /* 正在被WATCH命令监视的键 */
} redisClient;

struct redisServer {

	/* General */
	char *configfile;           /* 配置文件的绝对路径,要么就是NULL */

	int hz;                     /* serverCron() 每秒调用的次数 */

	redisDb *db;				/* 一个数组,保存着服务器中所有的数据库 */

	dict *commands;             /* 命令表（受到 rename 配置选项的作用） */

	dict *orig_commands;        /* Command table before command renaming. */

	aeEventLoop *el; /* 事件状态 */

	int shutdown_asap; /* 关闭服务器的标识 */

	int port;					/* TCP 监听端口 */
	int tcp_backlog;			/* TCP listen() backlog */
	char *bindaddr[REDIS_BINDADDR_MAX]; /* ip地址 */
	int bindaddr_count; /* 地址的数量 */

	int ipfd[REDIS_BINDADDR_MAX];  /* TCP 描述符 */
	int ipfd_count;				   /* 已经使用了的描述符的数目 */

	list *clients;	/* 一个链表,保存了所有的客户端状态结构 */

	list *clients_to_close; /* 链表,保存了所有待关闭的客户端 */

	redisClient *current_client; /* 服务器当前服务的客户端,仅用于崩溃报告 */
	
	char neterr[ANET_ERR_LEN]; /* 用于记录网络错误 */

	int tcpkeepalive; /* 是否开启 SO_KEEPALIVE选项 */

	int dbnum; /* 数据库的总数目 */

	/* Limits */
	int maxclients;   /* Max number of simultaneous clients */
	int maxidletime; /* 客户端的最大空转时间 */

	time_t unixtime; /* 记录时间 */
	long long mstime; /* 这个精度要高一些 */

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

	/* 常用命令的快捷连接 */
	struct redisCommand *multiCommand;
};

/*
* Redis 命令
*/
struct redisCommand {

	char *name; /* 命令名字 */
	redisCommandProc *proc; /* 实现函数 */
	int arity; /* 参数个数 */
	char *sflags; /* Flags as string representation, one char per flag. */
	int flags; /* 实际flag */
};

/* 通过复用来减少内存碎片，以及减少操作耗时的共享对象 */
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

/*
* 哈希对象的迭代器
*/
typedef struct {
	/* 被迭代的哈希对象 */
	robj *subject;
	/* 哈希对象的编码 */
	int encoding;
	/* 域指针和值指针 */
	unsigned char *fptr, *vptr;
	/* 字典迭代器和指向当前迭代字典节点的指针,在迭代HT编码的哈希对象时使用 */
	dictIterator *di;
	dictEntry *de;
} hashTypeIterator;

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

typedef void redisCommandProc(redisClient *c);
typedef int *redisGetKeysProc(struct redisCommand *cmd, robj **argv, int argc, int *numkeys);

/* 一些命令的处理函数 */
int *zunionInterGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys);
void freeClientArgv(redisClient *c);
void setCommand(redisClient *c);
void getCommand(redisClient *c);
void setnxCommand(redisClient *c);
void setexCommand(redisClient *c);
void psetexCommand(redisClient *c);
void appendCommand(redisClient *c);
void strlenCommand(redisClient *c);
void existsCommand(redisClient *c);
void setrangeCommand(redisClient *c);
void getrangeCommand(redisClient *c);
void incrCommand(redisClient *c);
void decrCommand(redisClient *c);
void msetCommand(redisClient *c);
void msetnxCommand(redisClient *c);
void incrbyCommand(redisClient *c);
void decrbyCommand(redisClient *c);
void mgetCommand(redisClient *c);
void hexistsCommand(redisClient *c);
void hsetCommand(redisClient *c);
void hgetCommand(redisClient *c);
void hgetallCommand(redisClient *c);
void hmsetCommand(redisClient *c);
void hmgetCommand(redisClient *c);
void hincrbyCommand(redisClient *c);
void hlenCommand(redisClient *c);
void hkeysCommand(redisClient *c);
void hvalsCommand(redisClient *c);

void lpushCommand(redisClient *c);
void rpushCommand(redisClient *c);
void lpopCommand(redisClient *c);
void rpopCommand(redisClient *c);
void lindexCommand(redisClient *c);
void llenCommand(redisClient *c);
void lsetCommand(redisClient *c);
void saddCommand(redisClient *c);
void sinterCommand(redisClient *c);
void scardCommand(redisClient *c);
void spopCommand(redisClient *c);
void sismemberCommand(redisClient *c);
void smoveCommand(redisClient *c);
void sunionCommand(redisClient *c);
void sunionstoreCommand(redisClient *c);
void sdiffstoreCommand(redisClient *c);
void sdiffCommand(redisClient *c);

void zaddCommand(redisClient *c);
void zcardCommand(redisClient *c);
void zrankCommand(redisClient *c);
void zunionstoreCommand(redisClient *c);
void zinterstoreCommand(redisClient *c);
void zincrbyCommand(redisClient *c);
void zcountCommand(redisClient *c);

void scanCommand(redisClient *c);
void expireCommand(redisClient *c);
void pexpireCommand(redisClient *c);
void ttlCommand(redisClient *c);
void persistCommand(redisClient *c);
void saveCommand(redisClient *c);
void selectCommand(redisClient *c);
void execCommand(redisClient *c);
void discardCommand(redisClient *c);
void watchCommand(redisClient *c);
void unwatchCommand(redisClient *c);
void multiCommand(redisClient *c);

#endif