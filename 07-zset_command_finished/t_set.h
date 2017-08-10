#ifndef __T_SET_H_
#define __T_SET_H_

#include "redis.h"

setTypeIterator *setTypeInitIterator(robj *subject);
robj *setTypeNextObject(setTypeIterator *si);

int setTypeRandomElement(robj *setobj, robj **objele, int64_t*llele);

void setTypeReleaseIterator(setTypeIterator *si);
int setTypeNext(setTypeIterator *si, robj **objele, int64_t *llele);
int setTypeRemove(robj *setobj, robj *value);
unsigned long setTypeSize(robj *subject);
robj *createSetObject(void);
robj *setTypeCreate(robj *value);
void setTypeConvert(robj *setobj, int enc);
int setTypeAdd(robj *subject, robj *value);
int setTypeIsMember(robj *subject, robj *value);
void saddCommand(redisClient *c);
int qsortCompareSetsByCardinality(const void *s1, const void *s2);
void sinterCommand(redisClient *c);
void sinterstoreCommand(redisClient *c);
void sismemberCommand(redisClient *c);
void scardCommand(redisClient *c);
void spopCommand(redisClient *c);
void smoveCommand(redisClient *c);
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2);

void sunionCommand(redisClient *c);
void sunionstoreCommand(redisClient *c);
void sdiffCommand(redisClient *c);
void sdiffstoreCommand(redisClient *c);

#endif
