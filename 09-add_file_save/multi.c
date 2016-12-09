/* 这个文件主要用于实现事务的机制. */
#include "redis.h"

/* ================================ MULTI/EXEC ============================== */

/* 
* 初始化客户端的事务状态
*/
void initClientMultiState(redisClient *c) {

	/* 命令队列 */
	c->mstate.commands = NULL;

	/* 命令计数 */
	c->mstate.count = 0;
}