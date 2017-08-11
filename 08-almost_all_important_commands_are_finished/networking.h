#ifndef __NETWORKING_H_
#define __NETWORKING_H_

#include "redis.h"
/* api */
size_t zmalloc_size_sds(sds s);
size_t getStringObjectSdsUsedMemory(robj *o);
void addReplyString(redisClient *c, char *s, size_t len);
void addReplyLongLongWithPrefix(redisClient *c, long long ll, char prefix);
void addReplyBulkLen(redisClient *c, robj *obj);
void addReplyBulk(redisClient *c, robj *obj);
void *dupClientReplyValue(void *o);
robj *dupLastObjectIfNeeded(list *reply);
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask);
int prepareClientToWrite(redisClient *c);
redisClient *createClient(int fd);
void addReply(redisClient *c, robj *obj);
int processInlineBuffer(redisClient *c);
int processMultibulkBuffer(redisClient *c);
void resetClient(redisClient *c);
void processInputBuffer(redisClient *c);
void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
void *dupClientReplyValue(void *o);
void decrRefCountVoid(void *o);
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void addReplyErrorLength(redisClient *c, char *s, size_t len);
void addReplyError(redisClient *c, char *err);
void freeClientArgv(redisClient *c);

void addReplyLongLong(redisClient *c, long long ll);
void addReplyBulkBuffer(redisClient *c, void *p, size_t len);
void addReplyMultiBulkLen(redisClient *c, long length);
void addReplyBulkCBuffer(redisClient*c, void *p, size_t len);
void addReplyBulkLongLong(redisClient *c, long long ll);

void *addDeferredMultiBulkLength(redisClient *c);
void setDeferredMultiBulkLength(redisClient *c, void *node, long length);


void addReplyBulkCString(redisClient *c, char *s);
void addReplyDouble(redisClient *c, double d);

void freeClientAsync(redisClient *c);
void freeClientsInAsyncFreeQueue(void);
int checkClientOutputBufferLimits(redisClient *c);
void asyncCloseClientOnOutputBufferLimitReached(redisClient *c);

#endif