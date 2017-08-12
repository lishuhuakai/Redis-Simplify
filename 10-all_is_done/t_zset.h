#ifndef __T_ZSET_H_
#define __T_ZSET_H_

#include "redis.h"

int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen);
int zslDelete(zskiplist *zsl, double score, robj *obj);
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update);
void zslFreeNode(zskiplistNode *node);
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
int zslRandomLevel(void);
zskiplistNode *zslInsert(zskiplist *zsl, double score, robj *obj);
zskiplistNode *zslCreateNode(int level, double score, robj *obj);

unsigned int zzlLength(unsigned char *zl);
void zsetConvert(robj *zobj, int encoding);
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr);
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, robj *ele, double score);
double zzlGetScore(unsigned char *sptr);
unsigned char *zzlInsert(unsigned char *zl, robj *ele, double score);
zskiplist *zslCreate(void);
unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score);
int getDoubleFromObjectOrReply(redisClient *c, robj *o, double *target, const char *msg);
void zaddCommand(redisClient *c);
void zincrbyCommand(redisClient *c);
unsigned int zsetLength(robj *zobj);
void zcardCommand(redisClient *c);
int zzlIsInRange(unsigned char *zl, zrangespec *range);
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range);
unsigned long zslGetRank(zskiplist *zsl, double score, robj *o);
int zslIsInRange(zskiplist *zsl, zrangespec *range);
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range);
void zcountCommand(redisClient *c);
void zrankGenericCommand(redisClient *c, int reverse);
void zrankCommand(redisClient *c);
void zscoreCommand(redisClient *c);
void zslFree(zskiplist *zsl);
/*
int zuiLongLongFromValue(zsetopval *val);
robj *zuiObjectFromValue(zsetopval *val);
int zuiBufferFromValue(zsetopval *val);
int zuiFind(zsetopsrc *op, zsetopval *val, double *score);
int zuiCompareByCardinality(const void *s1, const void *s2);
void zuiInitIterator(zsetopsrc *op);
void zuiClearIterator(zsetopsrc *op);
int zuiLength(zsetopsrc *op);
int zuiNext(zsetopsrc *op, zsetopval *val);
*/
void zunionInterGenericCommand(redisClient *c, robj *dstkey, int op);
void zunionstoreCommand(redisClient *c);

void zinterstoreCommand(redisClient *c);

#endif /* __T_ZSET_H_ */