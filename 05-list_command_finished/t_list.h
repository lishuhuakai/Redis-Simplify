#ifndef __T_LIST_H_
#define __T_LIST_H_

#include "redis.h"
robj *listTypeGet(listTypeEntry *entry);
unsigned long listTypeLength(robj *subject);
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction);
void listTypeReleaseIterator(listTypeIterator *li);
int listTypeNext(listTypeIterator *li, listTypeEntry *entry);
void listTypeConvert(robj *subject, int enc);
void listTypeTryConversion(robj *subject, robj *value);
void listTypePush(robj *subject, robj *value, int where);
void pushGenericCommand(redisClient *c, int where);
void lpushCommand(redisClient *c);
void rpushCommand(redisClient *c);
robj *listTypePop(robj *subject, int where);
void popGenericCommand(redisClient *c, int where);
void lpopCommand(redisClient *c);
void rpopCommand(redisClient *c);
void lindexCommand(redisClient *c);
void llenCommand(redisClient *c);
void lsetCommand(redisClient *c);

#endif /* __T_LIST_H_ */
