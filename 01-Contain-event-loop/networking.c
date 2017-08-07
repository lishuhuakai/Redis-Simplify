#include "redis.h"
#include "util.h"
#include "zmalloc.h"
#include "object.h"
#include <sys/uio.h>
#include <math.h>

extern struct redisServer server;
void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask); 
void *dupClientReplyValue(void *o);
void decrRefCountVoid(void *o);

/*
 * 如果在读入协议内容是,发现内容不符合协议,那么异步地关闭这个客户端 
 */
static void setProtocolError(redisClient *c, int pos) { 
	// todo
}

/*
* 创建一个新的客户端
*/
redisClient *createClient(int fd) {
	redisClient *c = zmalloc(sizeof(redisClient));
	// 当 fd 不为 -1 时，创建带网络连接的客户端
	// 如果 fd 为 -1 ，那么创建无网络连接的伪客户端
	// 因为 Redis 的命令必须在客户端的上下文中使用，所以在执行 Lua 环境中的命令时
	// 需要用到这种伪终端. 
	if (fd != -1) {
		anetNonBlock(NULL, fd);
		anetEnableTcpNoDelay(NULL, fd); // 禁用 Nagle算法

		if (server.tcpkeepalive) {
			anetKeepAlive(NULL, fd, server.tcpkeepalive);
		}

		// 绑定读事件到事件loop(开始接收命令请求)
		if (aeCreateFileEvent(server.el, fd, AE_READABLE,
			readQueryFromClient, c) == AE_ERR) {
			close(fd);
			zfree(c);
			return NULL;
		}
	}

	/* 初始化各个属性 */
	c->fd = fd;
	c->name = NULL;
	c->bufpos = 0; // 回复缓冲区的偏移量
	c->querybuf = sdsempty();
	c->reqtype = 0; // 命令请求的类型
	c->argc = 0; // 命令参数的数量 
	c->argv = NULL; // 命令参数 
	c->cmd = c->lastcmd = NULL; // 当前执行的命令和最近一次执行的命令

	c->bulklen = -1; // 读入的参数的长度 
	c->multibulklen = 0; // 查询缓冲区中未读入的命令内容数量

	c->reply = listCreate(); // 回复链表 
	c->reply_bytes = 0; //  回复链表的字节量

	listSetFreeMethod(c->reply, decrRefCountVoid);
	listSetDupMethod(c->reply, dupClientReplyValue);

	if (fd != -1) listAddNodeTail(server.clients, c); // 如果不是伪客户端,那么添加服务器的客户端到客户端链表之中

	return c;

}

/*================================ Parse command ==================================*/

/*
 * 处理内联命令，并创建参数对象
 *
 * 内联命令的各个参数以空格分开，并以 \r\n 结尾
 * 例子：
 *
 * <arg0> <arg1> <arg...> <argN>\r\n
 *
 * 这些内容会被用于创建参数对象，
 * 比如
 *
 * argv[0] = arg0
 * argv[1] = arg1
 * argv[2] = arg2
 */


/*
 * 这个函数相当于parse,处理命令
 */
int processInlineBuffer(redisClient *c) {
	char *newline;
	int argc, j;
	sds *argv, aux;
	size_t querylen;

	newline = strchr(c->querybuf, '\n'); // 寻找一行的结尾

	// 如果接收到了错误的内容,出错
	if (newline == NULL) {
		return REDIS_ERR;
	}

	/* 处理\r\n */ 
	if (newline && newline != c->querybuf && *(newline - 1) == '\r')
		newline--;

	/* 然后根据空格,来分割命令的参数
	 * 比如说 SET msg hello \r\n将被分割成
	 * argv[0] = SET
	 * argv[1] = msg
	 * argv[2] = hello
	 * argc = 3
	 */
	querylen = newline - (c->querybuf);
	aux = sdsnewlen(c->querybuf, querylen);
	argv = sdssplitargs(aux, &argc);
	sdsfree(aux);

	/* 从缓冲区中删除已经读取了的内容,剩下的内容是未被读取的 */
	sdsrange(c->querybuf, querylen + 2, -1);

	if (c->argv) free(c->argv);
	c->argv = zmalloc(sizeof(robj*)*argc);

	// 为每个参数创建一个字符串对象
	for (c->argc = 0, j = 0; j < argc; j++) {
		if (sdslen(argv[j])) {
			// argv[j] 已经是 SDS 了
			// 所以创建的字符串对象直接指向该 SDS
			c->argv[c->argc] = createObject(REDIS_STRING, argv[j]);
			c->argc++;
		}
		else {
			sdsfree(argv[j]);
		}
	}
	zfree(argv);
	return REDIS_OK;
}



/*
 * 将 c->querybuf 中的协议内容转换成 c->argv 中的参数对象
 *
 * 比如 *3\r\n$3\r\nSET\r\n$3\r\nMSG\r\n$5\r\nHELLO\r\n
 * 将被转换为：
 * argv[0] = SET
 * argv[1] = MSG
 * argv[2] = HELLO
 */
int processMultibulkBuffer(redisClient *c) {
	char *newline = NULL;
	int pos = 0, ok;
	long long ll;

	// 读入命令的参数个数
	// 比如 *3\r\n$3\r\nSET\r\n... 将令 c->multibulklen = 3
	if (c->multibulklen == 0) {
		newline = strchr(c->querybuf, '\r');
		// 将参数个数，也即是 * 之后， \r\n 之前的数字取出并保存到 ll 中 
		// 比如对于 *3\r\n ，那么 ll 将等于 3
		ok = string2ll(c->querybuf + 1, newline - (c->querybuf + 1), &ll);

		// 参数数量之后的位置 
		// 比如对于 *3\r\n$3\r\n$SET\r\n... 来说，
		// pos 指向 *3\r\n$3\r\n$SET\r\n...
		//                ^
		//                |
		//               pos
		pos = (newline - c->querybuf) + 2;

		// 设置参数数量
		c->multibulklen = ll;

		// 根据参数数量,为各个参数对象分配空间 
		if (c->argv) zfree(c->argv);
		c->argv = zmalloc(sizeof(robj*) * c->multibulklen);
	}

	// 从c->querybuf中读入参数,并创建各个参数对象到c->argv
	while (c->multibulklen) {
		// 读入参数长度
		if (c->bulklen == -1) { // 这里指的是命令的长度
			// 确保"\r\n"存在 
			newline = strchr(c->querybuf + pos, '\r');

			// 读取长度,比如说 $3\r\nSET\r\n 会让 ll 的值变成3
			ok = string2ll(c->querybuf + pos + 1, newline - (c->querybuf + pos + 1), &ll);

			// 定位到参数的开头
			// 比如 
			// $3\r\nSET\r\n...
			//       ^
			//       |
			//      pos
			pos += newline - (c->querybuf + pos) + 2;
			c->bulklen = ll;
		}
		
		// 为参数创建字符串对象
		if (pos == 0 &&
			c->bulklen >= REDIS_MBULK_BIG_ARG &&
			(signed)sdslen(c->querybuf) == c->bulklen + 2)
		{
			c->argv[c->argc++] = createObject(REDIS_STRING, c->querybuf);
			sdsIncrLen(c->querybuf, -2); // 去掉\r\n
			c->querybuf = sdsempty();
			c->querybuf = sdsMakeRoomFor(c->querybuf, c->bulklen + 2);
			pos = 0;
		}
		else {
			c->argv[c->argc++] =
				createStringObject(c->querybuf + pos, c->bulklen);
			pos += c->bulklen + 2;
		}

		// 清空参数长度
		c->bulklen = -1;

		// 减少还需读入的参数个数
		c->multibulklen--;
	}
	
	if (pos) sdsrange(c->querybuf, pos, -1); // 从querybuf中删除已被读取的内容

	// 如果本条命令的所有参数都已经读取完,那么返回
	if (c->multibulklen == 0) return REDIS_OK;

	// 如果还有参数未读取完,那么就是协议出错了!
	return REDIS_ERR;
}

// 在客户端执行完命令之后执行：重置客户端以准备执行下个命令
void resetClient(redisClient *c) {
	// todo
}

void processInputBuffer(redisClient *c) {
	// 尽可能地处理查询缓存区中的内容.如果读取出现short read, 那么可能会有内容滞留在读取缓冲区里面
	// 这些滞留的内容也许不能完整构成一个符合协议的命令,需要等待下次读事件的就绪.
	while (sdslen(c->querybuf)) {

		if (!c->reqtype) {
			if (c->querybuf[0] == '*') {
				c->reqtype = REDIS_REQ_MULTIBULK; // 多条查询 
			}
			else {
				c->reqtype = REDIS_REQ_INLINE; // 内联查询 
			}
		}
		// 将缓冲区的内容转换成命令,以及命令参数
		if (c->reqtype == REDIS_REQ_INLINE) {
			if (processInlineBuffer(c) != REDIS_OK) break;
		}
		else if (c->reqtype == REDIS_REQ_MULTIBULK) {
			if (processMultibulkBuffer(c) != REDIS_OK) break;
		}
		else {
			// todo
		}

		if (c->argc == 0) {
			resetClient(c); // 重置客户端
		}
		else {
			if (processCommand(c) == REDIS_OK)
				resetClient(c);
		}
	}
}


/*
 * 读取客户端的查询缓冲区内容
 */
void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
	redisClient *c = (redisClient *)privdata;
	int nread, readlen;
	size_t qblen;

	server.current_client = c; // 设置服务器的当前客户端

	readlen = REDIS_IOBUF_LEN; // 读入长度,默认为16MB

	// 获取查询缓冲区当前内容的长度 
	qblen = sdslen(c->querybuf); 
	// 如果有需要,更新缓冲区内容长度的峰值(peak)
	if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;
	// 为查询缓冲区分配空间
	c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);
	// 读入内容到查询缓存
	nread = read(fd, c->querybuf + qblen, readlen);

	if (nread == -1) {
		if (errno == EAGAIN) {
			nread = 0;
		} 
		else {
			//freeClient(c);
			return;
		}
	}
	else if (nread == 0) { // 对方关闭了连接
		// todo
		//freeClient(c);
		return;
	}

	if (nread) {
		sdsIncrLen(c->querybuf, nread);
	} 
	else {
		// 在 nread == -1 且 errno == EAGAIN 时运行
		server.current_client = NULL;
		return;
	}
	//
	// 从查询缓存中读取内容,创建参数,并执行命令,函数会执行到缓存中的所有内容都被处理完为止
	//
	processInputBuffer(c);
	server.current_client = NULL;
}


/*
 * TCP 连接 accept 处理器
 */
#define MAX_ACCEPTS_PER_CALL 1000
static void acceptCommonHandler(int fd, int flags) {
	// 创建客户端
	redisClient *c;
	// 在createClient函数中大概做了这么一些事情,首先是构造了一个redisClient结构,
	// 最为重要的是,完成了fd与RedisClient结构的关联,并且完成了该结构的初始化工作.
	// 并将这个结果挂到了server的clients链表上.
	// 还有一点,那就是对该fd的读事件进行了监听.
	if ((c = createClient(fd)) == NULL) {
		// log
		close(fd);
		return;
	}
}


/*
 * 创建一个 TCP 连接处理器
 */
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
	int cport, cfd, max = 2;
	char cip[REDIS_IP_STR_LEN];

	while (max--) {
		cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport); // 获得ip地址以及端口号
		if (cfd == ANET_ERR) {
			if (errno != EWOULDBLOCK) {
				// log
				return;
			}
		}
		acceptCommonHandler(cfd, 0); // 为客户端创建客户端状态
	}

}