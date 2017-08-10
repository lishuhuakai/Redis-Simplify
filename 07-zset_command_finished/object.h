#ifndef __OBJECT_H_
#define __OBJECT_H_

#include "redis.h"
#include "zmalloc.h"

/* api */
robj *createObject(int type, void *ptr);
robj *createEmbeddedStringObject(char *ptr, size_t len);
robj *createRawStringObject(char *ptr, size_t len);
robj *createStringObject(char *ptr, size_t len);
robj *createHashObject(void);
robj *createObject(int type, void *ptr);
void freeStringObject(robj *o);
void decrRefCount(robj *o);
void decrRefCountVoid(void *o);
robj *dupStringObject(robj *o);
void incrRefCount(robj *o);
robj *createEmbeddedStringObject(char *ptr, size_t len);
int getLongLongFromObject(robj *o, long long *target);
int getLongLongFromObjectOrReply(redisClient *c, robj *o, long long *target, const char *msg);
int getLongFromObjectOrReply(redisClient *c, robj *o, long long *target, const char* msg);
robj *tryObjectEncoding(robj *o);
robj *getDecodedObject(robj *o);

size_t stringObjectLen(robj *o);
robj *createStringObjectFromLongLong(long long value);
void freeListObject(robj *o);
robj *createZiplistObject(void);

robj* createIntsetObject(void);
int isObjectRepresentableAsLongLong(robj *o, long long *llval);

int isObjectRepresentableAsLongLong(robj *o, long long *llval);
int getDoubleFromObject(robj *o, double *target);
robj *createZsetObject(void);
robj* createZsetZiplistObject(void);
int compareStringObjectsWithFlags(robj *a, robj *b, int flags);
int compareStringObjects(robj *a, robj *b);
int equalStringObjects(robj *a, robj *b);

#endif
