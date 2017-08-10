#ifndef __DB_H_
#define __DB_H_

#include "redis.h"
robj *lookupKey(redisDb *db, robj *key);
robj *lookupKeyRead(redisDb *db, robj *key);
robj *lookupKeyReadOrReply(redisClient *c, robj *key, robj *reply);
robj* lookupKeyWrite(redisDb *db, robj *key);
void dbAdd(redisDb *db, robj *key, robj *val);
void dbOverwrite(redisDb *db, robj *key, robj *val);
void setKey(redisDb *db, robj *key, robj *val);
int selectDb(redisClient *c, int id);
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o);
int dbExists(redisDb *db, robj *key);
void existsCommand(redisClient *c);
#endif