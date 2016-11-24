#include "redis.h"
#include <math.h> /* isnan(), isinf() */


int getGenericCommand(redisClient *c) {
	printf("get command!\n");
}


void getCommand(redisClient *c) {
	getGenericCommand(c);
}

/* SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>] */
void setCommand(redisClient *c) {
	printf("set command!\n");
}