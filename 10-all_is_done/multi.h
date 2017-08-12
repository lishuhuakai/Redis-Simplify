#ifndef __MULT_H_
#define __MULT_H_

#include "redis.h"
void initClientMultiState(redisClient *c);
void multiCommand(redisClient *c);
void freeClientMultiState(redisClient *c);
void queueMultiCommand(redisClient *c);
void unwatchAllKeys(redisClient *c);
void discardTransaction(redisClient *c);
void execCommandPropagateMulti(redisClient *c);
void execCommand(redisClient *c);
void watchForKey(redisClient *c, robj *key);
void touchWatchedKey(redisDb *db, robj *key);
void watchCommand(redisClient *c);
void unwatchCommand(redisClient *c);
void discardCommand(redisClient *c);
void flagTransaction(redisClient *c);

#endif /* __MULT_H_ */