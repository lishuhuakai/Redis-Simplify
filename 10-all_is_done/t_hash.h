#ifndef __T_HASH_H_
#define __T_HASH_H_
#include "redis.h"

/* api */
void hdelCommand(redisClient *c);
void hsetCommand(redisClient *c);
void hgetCommand(redisClient *c);
void hgetallCommand(redisClient *c);
void hmgetCommand(redisClient *c);
void hmsetCommand(redisClient *c);
void hincrbyCommand(redisClient *c);
void hlenCommand(redisClient *c);
void hkeysCommand(redisClient *c);
void hvalsCommand(redisClient *c);
void hexistsCommand(redisClient *c);

/* 不向外暴露的函数 */
int hashTypeGetFromZiplist(robj *o, robj *field, unsigned char **vstr, unsigned int *vlen, long long *vll);
robj *hashTypeLookupWriteOrCreate(redisClient *c, robj *key);
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what);
robj *hashTypeGetObject(robj *o, robj *field);
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll);
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst);
unsigned long hashTypeLength(robj *o);
hashTypeIterator *hashTypeInitIterator(robj *subject);
int hashTypeNext(hashTypeIterator *hi);
void hashTypeReleaseIterator(hashTypeIterator *hi);
void hashTypeConvert(robj *o, int enc);
#endif /* __T_HASH_H_ */