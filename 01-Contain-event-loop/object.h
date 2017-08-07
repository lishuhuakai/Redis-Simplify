#ifndef __OBJECT_H_
#define __OBJECT_H_

#include "redis.h"
#include "zmalloc.h"

/* api */
robj *createObject(int type, void *ptr);
robj *createEmbeddedStringObject(char *ptr, size_t len);
robj *createRawStringObject(char *ptr, size_t len);
robj *createStringObject(char *ptr, size_t len);
robj *createObject(int type, void *ptr);
void freeStringObject(robj *o);
void decrRefCount(robj *o);
void decrRefCountVoid(void *o);
void incrRefCount(robj *o);
robj *createEmbeddedStringObject(char *ptr, size_t len);

#endif
