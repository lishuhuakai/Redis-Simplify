/* anet.c -- Basic TCP socket stuff made a bit less boring */

#ifndef ANET_H
#define ANET_H


#define ANET_OK 0
#define ANET_ERR -1
#define ANET_ERR_LEN 256

/* Flags used with certain functions. */
#define ANET_NONE 0
#define ANET_IP_ONLY (1<<0)

int anetTcpServer(char *err, int port, char *bindaddr, int backlog);
int anetTcpAccept(char *err, int serversock, char *ip, size_t ip_len, int *port);
int anetNonBlock(char *err, int fd);
int anetEnableTcpNoDelay(char *err, int fd);
int anetKeepAlive(char *err, int fd, int interval);

#endif
