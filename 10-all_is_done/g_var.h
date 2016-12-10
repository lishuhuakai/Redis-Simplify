#ifndef _G_VAR_
#define _G_VAR_

/* 这个文件主要保存全局的一些变量的声明 */
extern struct sharedObjectsStruct shared;
extern struct redisServer server;
extern struct dictType zsetDictType;
extern struct dictType setDictType;
extern struct dictType hashDictType;
extern struct dictType commandTableDictType;
extern struct dictType dbDictType;
extern struct dictType keyptrDictType;
extern struct dictType keylistDictType;

/* 一些全局的宏定义 */

/* Error codes */
#define REDIS_OK				0
#define REDIS_ERR				-1

/* Objects encoding. Some kind of objects like Strings and Hashes can be
* internally represented in multiple ways. The 'encoding' field of the object
* is set to one of this fields for this object. */
/* 对象编码 */
#define REDIS_ENCODING_RAW 0     /* Raw representation */

/* 对象类型 */
#define REDIS_STRING 0 
#define REDIS_LIST 1
#define REDIS_SET 2
#define REDIS_ZSET 3
#define REDIS_HASH 4

/* Protocol and I/O related defines */
#define REDIS_REPLY_CHUNK_BYTES (16*1024)	 /* 16k output buffer */
#define REDIS_IOBUF_LEN         (1024*16)	 /* Generic I/O buffer size */
#define REDIS_INLINE_MAX_SIZE   (1024*64)	 /* Max size of inline reads */
#define REDIS_MBULK_BIG_ARG     (1024*32)
#define REDIS_MIN_RESERVED_FDS 32
#define REDIS_EVENTLOOP_FDSET_INCR (REDIS_MIN_RESERVED_FDS+96)
#define REDIS_MAX_CLIENTS 10000				/* 最大所支持的用户数目 */
#define REDIS_LONGSTR_SIZE      21          /* Bytes needed for long -> str */

/* Static server configuration */
#define REDIS_DEFAULT_HZ        10 
#define REDIS_SERVERPORT		6379		/* TCP port */
#define REDIS_TCP_BACKLOG       511			/* TCP listen backlog */
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
#define REDIS_REQ_MULTIBULK 2					 /* 多条查询 */


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
#define REDIS_HASH_MAX_ZIPLIST_ENTRIES 512
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
#define REDIS_DIRTY_CAS (1<<5) /* Watched keys modified. EXEC will fail. */
#define REDIS_CLOSE_AFTER_REPLY (1<<6) /* Close after writing entire reply. */
#define REDIS_DIRTY_EXEC (1<<12)  /* EXEC will fail for errors while queueing */
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

#endif
