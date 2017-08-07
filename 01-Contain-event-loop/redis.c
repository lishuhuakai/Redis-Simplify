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

/*=============================== Function declariton =======================*/
void setCommand(redisClient *c);
void getCommand(redisClient *c);
/*================================= Globals ================================= */

/* Global vars */
struct redisServer server; /* server global state */

struct redisCommand redisCommandTable[] = {
	{ "get",getCommand,2,"r",0, NULL,1,1,1,0,0 },
	{ "set",setCommand,-3,"wm",0,NULL,1,1,1,0,0 },
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

/*
 * 初始化服务器的配置信息
 */
void initServerConfig() {
	int j;

	// 服务器状态
	server.hz = REDIS_DEFAULT_HZ;
	server.port = REDIS_SERVERPORT; // 6379号端口监听
	server.tcp_backlog = REDIS_TCP_BACKLOG;
	server.bindaddr_count = 0;

	server.maxclients = REDIS_MAX_CLIENTS;
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
	return ustime() / 1000; /* 1s = 1000 us */
 }

void updateCachedTime(void) {
	//server.unixtime = time(NULL);
	//server.mstime = mstime();
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

void clientsCron(void) {
	// todo
}

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
	int j;
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

	server.el = aeCreateEventLoop(server.maxclients + REDIS_EVENTLOOP_FDSET_INCR);

	// 打开 TCP 监听端口,用于等待客户端的命令请求
	if (server.port != 0 &&
		listenToPort(server.port, server.ipfd, &server.ipfd_count) == REDIS_ERR) {
		exit(1);
	}
	
	// ipfd_count 用于记录监听描述符的个数,一般而言,只有一个

	// 为serverCron() 创建定时事件
	if (aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL) == AE_ERR) {
		exit(1);
	}

	// 为 TCP 连接关联应答(accept)处理器,用于接收并应答客户端的connect()调用
	for (j = 0; j < server.ipfd_count; j++) {
		if (aeCreateFileEvent(server.el, server.ipfd[j], AE_READABLE,
			acceptTcpHandler, NULL) == AE_ERR) {
			printf("NO!\n");
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
