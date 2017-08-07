#include "redis.h"
#include <math.h> /* isnan(), isinf() */
#include "db.h"
#include "networking.h"
#include "object.h"

extern struct sharedObjectsStruct shared;

#define REDIS_SET_NO_FLAGS 0
#define REDIS_SET_NX (1<<0)     // Set if key not exists.
#define REDIS_SET_XX (1<<1)     // Set if key exists.

void setGenericCommand(redisClient *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {
	long long milliseconds = 0; // 微秒
	if (expire) { // 取出过期时间 
		// todo
	}
	// todo
	setKey(c->db, key, val); // 将键值关联到数据库
	
	// 设置成功,向客户端发送回复,回复的内容由ok_reply决定
	addReply(c, ok_reply ? ok_reply : shared.ok);
}


int getGenericCommand(redisClient *c) {
	robj *o;
	// 尝试从数据库中取出键为c->argv[1]对应的值对象,如果键不存在时,向客户端发送回复信息,
	// 并返回NULL 
	if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk)) == NULL)
		return REDIS_OK;
	// 如果值存在,则检查它的类型
	if (o->type != REDIS_STRING) {
		// 类型错误
		addReply(c, shared.wrongtypeerr);
		return REDIS_ERR;
	}
	else { // 类型正确
		addReplyBulk(c, o);
		return REDIS_OK;
	}
}

void getCommand(redisClient *c) {
	getGenericCommand(c);
}

/*
 * 命令格式为:
 *	SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>] 
 */
void setCommand(redisClient *c) {
	int j;
	robj* expire = NULL; // 过期时间
	int flags = REDIS_SET_NO_FLAGS;
	int unit = UNIT_SECONDS;

	// 设置参数选项
	for (j = 3; j < c->argc; j++) {
		char *a = c->argv[j]->ptr;
		robj *next = (j == c->argc - 1) ? NULL : c->argv[j + 1];
		if ((a[0] == 'n' || a[0] == 'N') &&
			(a[1] == 'x' || a[1] == 'X') && a[2] == '\0') {
			flags |= REDIS_SET_NX;
		}
		else if ((a[0] == 'x' || a[0] == 'X') &&
			(a[1] == 'x' || a[1] == 'X') && a[2] == '\0') {
			flags |= REDIS_SET_XX;
		}
		else if ((a[0] == 'e' || a[0] == 'E') &&
			(a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && next) {
			unit = UNIT_SECONDS;
			expire = next;
			j++;
		}
		else if ((a[0] == 'p' || a[0] == 'P') &&
			(a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && next) {
			unit = UNIT_MILLISECONDS;
			expire = next;
			j++;
		}
		else {
			addReply(c, shared.syntaxerr); // 命令错误
			return;
		}
	}

	// 尝试对值对象进行编码
	c->argv[2] = tryObjectEncoding(c->argv[2]);
	setGenericCommand(c, flags, c->argv[1], c->argv[2], expire, unit, NULL, NULL);
}