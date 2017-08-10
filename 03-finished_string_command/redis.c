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
#include "networking.h"

struct sharedObjectsStruct shared;

/*=============================== Function declariton =======================*/
void setCommand(redisClient *c);
void getCommand(redisClient *c);
/*================================= Globals ================================= */

/* Global vars */
struct redisServer server; /* server global state */

struct redisCommand redisCommandTable[] = {
	{ "get",getCommand,2,"r",0 },
	{ "set",setCommand,-3,"wm",0 },
	{ "setnx",setnxCommand,3,"wm",0},
	{ "setex",setexCommand,4,"wm",0},
	{ "psetex",psetexCommand,4,"wm"},
	{ "append",appendCommand,3,"wm",0},
	{ "strlen",strlenCommand,2,"r",0},
	{ "exists",existsCommand,2,"r",0},
	{ "setrange",setrangeCommand,4,"wm",0},
	{ "getrange",getrangeCommand,4,"r",0},
	{ "substr",getrangeCommand,4,"r",0}, // 求子串居然是getrange的alias
	{ "incr",incrCommand,2,"wm",0},
	{ "decr",decrCommand,2,"wm",0},
	{ "mget",mgetCommand,-2,"r",0},
	{ "mset",msetCommand,-3,"wm",0},
	{ "msetnx",msetnxCommand,-3,"wm",0},
	{ "incrby",incrbyCommand,3,"wm",0},
	{ "decrby",decrbyCommand,3,"wm",0},
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

void call(redisClient *c, int flags) {
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
	
	char *cmd = c->argv[0]->ptr;
	c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
	call(c, REDIS_CALL_FULL);
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

void initServerConfig() {
	int j;

	// 服务器状态
	server.hz = REDIS_DEFAULT_HZ;
	server.port = REDIS_SERVERPORT; // 6379号端口监听
	server.tcp_backlog = REDIS_TCP_BACKLOG;
	server.bindaddr_count = 0;

	server.maxclients = REDIS_MAX_CLIENTS; // 默认情况下,最多支持 10000 个客户同时连接
	server.maxidletime = REDIS_MAXIDLETIME; 
	server.ipfd_count = 0;
	server.dbnum = REDIS_DEFAULT_DBNUM;
	server.tcpkeepalive = REDIS_DEFAULT_TCP_KEEPALIVE;
	server.commands = dictCreate(&commandTableDictType, NULL);
	server.orig_commands = dictCreate(&commandTableDictType, NULL);
	populateCommandTable();  // 安装命令处理函数
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
* 释放所有事务状态相关的资源
*/
void freeClientMultiState(redisClient *c) {
	// todo
	
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
}
/* ======================= Cron: called every 100 ms ======================== */

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
	return 1000000 / server.hz; // 这个返回的值决定了下次什么时候再调用这个函数
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
		server.db[j].id = j;
	}


	// 为serverCron() 创建定时事件
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


int main(int argc, char **argv) {
	initServerConfig();
	initServer();
	// 运行事件处理器,一直到服务器关闭为止
	aeMain(server.el);
	return 0;
}