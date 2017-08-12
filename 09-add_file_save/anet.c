#include "fmacros.h"
#include <stddef.h>
#include "anet.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <netdb.h>

/*
 * 打印错误信息
 */
static void anetSetError(char *err, const char *fmt, ...) {
	va_list ap;
	if (!err) return;
	va_start(ap, fmt);
	vsnprintf(err, ANET_ERR_LEN, fmt, ap);
	va_end(ap);
}

/*
 * 设置地址为可重用
 */
static int anetSetReuseAddr(char *err, int fd) {
	int yes = 1;
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
		anetSetError(err, "setsocketopt SO_REUSEADDR: %s", strerror(errno));
		return ANET_ERR;
	}
	return ANET_OK;
}

/*
 * anetListen 绑定并创建监听套接字
 */
static int anetListen(char *err, int fd, struct sockaddr *sa, socklen_t len, int backlog) {
	if (bind(fd, sa, len) == -1) {
		anetSetError(err, "bind: %s", strerror(errno));
		close(fd);
		return ANET_ERR;
	}

	if (listen(fd, backlog) == -1) {
		anetSetError(err, "listen: %s", strerror(errno));
		close(fd);
		return ANET_ERR;
	}

	return ANET_OK;
}

/*
 * anetGenericAccept 用于获取一个连接,返回连接的描述符.
 */
static int anetGenericAccept(char *err, int s, struct sockaddr *sa, socklen_t *len) {
	int fd;
	while (1) {
		fd = accept(s, sa, len);
		if (fd == -1) {
			if (errno == EINTR)
				continue;
			else {
				anetSetError(err, "accept: %s", strerror(errno));
				return ANET_ERR;
			}
		}
		break;
	}
	return fd;
}


/*
 * TCP 连接 accept 函数, 对acccept函数做了简单的封装, 最终返回连接的描述符
 */
int anetTcpAccept(char *err, int s, char *ip, size_t ip_len, int *port) {
	int fd;
	struct sockaddr_storage sa;
	socklen_t salen = sizeof(sa);
	if ((fd = anetGenericAccept(err, s, (struct sockaddr*)&sa, &salen)) == -1)
		return ANET_ERR;

	if (sa.ss_family == AF_INET) {
		struct sockaddr_in *s = (struct sockaddr_in *)&sa;
		// 将对方的信息记录下来,包括ip地址以及端口信息
		if (ip) inet_ntop(AF_INET, (void*)&(s->sin_addr), ip, ip_len);
		if (port) *port = ntohs(s->sin_port);
	}
	return fd;
}

static int _anetTcpServer(char *err, int port, char *bindaddr, int af, int backlog) {
	int s, rv;
	char _port[6];
	struct addrinfo hints;
	struct addrinfo *servinfo, *p;

	snprintf(_port, 6, "%d", port); // 将端口的值转换为char类型
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = af;
	hints.ai_socktype = SOCK_STREAM; // 流式套接字
	//hints.ai_flags = AI_PASSIVE;

	if ((rv = getaddrinfo(bindaddr, _port, &hints, &servinfo)) != 0) {
		//anetSetError(err, "%s", gai_sererror(rv));
		return ANET_ERR;
	}

	for (p = servinfo; p != NULL; p = p->ai_next) {
		if ((s = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
			continue; // 创建套接字失败的话,继续
		if (anetSetReuseAddr(err, s) == ANET_ERR) goto error;
		if (anetListen(err, s, p->ai_addr, p->ai_addrlen, backlog) == ANET_ERR) goto error;

		goto end;
	}
	if (p == NULL) {
		anetSetError(err, "unable to bind socket");
		goto error;
	}
error:
	s = ANET_ERR;
end:
	freeaddrinfo(servinfo);
	return s;
}

int anetTcpServer(char *err, int port, char *bindaddr, int backlog) {
	return _anetTcpServer(err, port, bindaddr, AF_INET, backlog);
}

/*
 * 将 fd 设置为非阻塞模式 (O_NONBLOCK)
 */
int anetNonBlock(char *err, int fd) {
	int flags;

	if ((flags = fcntl(fd, F_GETFL)) == -1) { // 首先获得原本在fd上的mask
		anetSetError(err, "fcntl(F_GETFL): %s", strerror(errno));
		return ANET_ERR;
	}
	if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) { // 然后添上O_NONBLOCK这个属性
		anetSetError(err, "fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
		return ANET_ERR;
	}
	return ANET_OK;
}


/*
 * 打开或者关闭 Nagle 算法
 */

static int anetSetTcpNoDelay(char *err, int fd, int val) {
	if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == -1) {
		anetSetError(err, "setsockopt TCP_NODELAY: %s", strerror(errno));
		return ANET_ERR;
	}
	return ANET_OK;
}


/*
 * 禁用 Nagle 算法
 */
int anetEnableTcpNoDelay(char *err, int fd) {
	return anetSetTcpNoDelay(err, fd, 1);
}

/*
 * 修改 TCP 连接的 keepalive选项
 */
int anetKeepAlive(char *err, int fd, int interval) {
	int val = 1;
	if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) == -1) {
		anetSetError(err, "setsockopt SO_KEEPALIVE: %s", strerror(errno));
		return ANET_ERR;
	}
	return ANET_OK;
}