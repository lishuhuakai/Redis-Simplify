#ifndef __T_STRING_H_
#define __T_STRING_H_
#include "redis.h"

void setnxCommand(redisClient *c);

void setexCommand(redisClient *c);
void psetexCommand(redisClient *c);
void appendCommand(redisClient *c);
void setrangeCommand(redisClient *c);
void getrangeCommand(redisClient *c);
void strlenCommand(redisClient *c);
void incrDecrCommand(redisClient *c, long long incr);
void incrCommand(redisClient *c);
void decrCommand(redisClient *c);
void mgetCommand(redisClient *c);
void msetGenericCommand(redisClient *c, int nx);
void msetCommand(redisClient *c);
void msetnxCommand(redisClient *c);
void getsetCommand(redisClient *c);
void incrbyCommand(redisClient *c);
void decrbyCommand(redisClient *c);

int checkType(redisClient *c, robj *o, int type);

#endif