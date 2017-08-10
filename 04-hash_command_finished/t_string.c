#include "redis.h"
#include <math.h> /* isnan(), isinf() */
#include "db.h"
#include "networking.h"
#include "object.h"
#include "t_string.h"
#include "util.h"

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

/*
 * getCommand 用于获取键对应的值
 */
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

void setnxCommand(redisClient *c) {
	c->argv[2] = tryObjectEncoding(c->argv[2]); // 尝试着压缩
	setGenericCommand(c, REDIS_SET_NX, c->argv[1], c->argv[2], NULL, 0, shared.cone, shared.czero);
}

void setexCommand(redisClient *c) {
	c->argv[3] = tryObjectEncoding(c->argv[3]);
	setGenericCommand(c, REDIS_SET_NO_FLAGS, c->argv[1], c->argv[3], c->argv[2], UNIT_SECONDS, NULL, NULL);
}

void psetexCommand(redisClient *c) {
	c->argv[3] = tryObjectEncoding(c->argv[3]);
	// 唯一的不同是,这里使用了毫秒
	setGenericCommand(c, REDIS_SET_NO_FLAGS, c->argv[1], c->argv[3], c->argv[2], UNIT_MILLISECONDS, NULL, NULL);
}

/*
 * 检查对象 o 的类型是否和 type 相同：
 *
 *  - 相同返回 0
 *
 *  - 不相同返回 1 ，并向客户端回复一个错误
 */
int checkType(redisClient *c, robj *o, int type) {
	if (o->type != type) {
		addReply(c, shared.wrongtypeerr);
		return 1;
	}
	return 0;
}


void appendCommand(redisClient *c) {
	size_t totlen;
	robj *o, *append;

	// 取出键相应的值对象
	o = lookupKeyWrite(c->db, c->argv[1]);
	char *key = c->argv[1]->ptr;
	if (o == NULL) {
		// 这个键如果不存在的话,创建一个新的
		c->argv[2] = tryObjectEncoding(c->argv[2]);
		dbAdd(c->db, c->argv[1], c->argv[2]);
		incrRefCount(c->argv[2]);
		totlen = stringObjectLen(c->argv[2]);
	}
	else {
		// 如果键值对存在的话
		if (checkType(c, o, REDIS_STRING)) // 检查类型
			return;

		append = c->argv[2];
		char *key = c->argv[2]->ptr;
		// 执行追加操作
		o = dbUnshareStringValue(c->db, c->argv[1], o);
		o->ptr = sdscatlen(o->ptr, append->ptr, sdslen(append->ptr));
		totlen = sdslen(o->ptr);
	}
	// 发送回复
	addReplyLongLong(c, totlen);
}

void setrangeCommand(redisClient *c) {
	robj *o;
	long long offset;
	sds value = c->argv[3]->ptr;
	// 取出offset参数
	if (getLongFromObjectOrReply(c, c->argv[2], &offset, NULL) != REDIS_OK) {
		return;
	}

	if (offset < 0) { // 范围不对
		addReplyError(c, "offset is out of range");
		return;
	}

	o = lookupKeyWrite(c->db, c->argv[1]); // 取出键现在的值对象
	if (o == NULL) {
		// 键不存在数据库中
		if (sdslen(value) == 0) {
			addReply(c, shared.czero);
			return;
		}

		// 如果设置后的长度会超过Redis的限制的话,那么放弃设置,向客户端发送一个出错回复
		o = createObject(REDIS_STRING, sdsempty());
		dbAdd(c->db, c->argv[1], o);
	}
	else {
		// 值对象存在 */
		size_t olen;
		if (checkType(c, o, REDIS_STRING)) {
			return;
		}

		olen = stringObjectLen(o);
		// value为空,没有什么可设置的,向客户端返回0
		if (sdslen(value) == 0) {
			addReplyLongLong(c, olen);
			return;
		}

		// 如果o被共享了或者被encoded了的话,重新创建一个对象
		o = dbUnshareStringValue(c->db, c->argv[1], o);
	}
	if (sdslen(value) > 0) {
		// 扩展字符串对象
		o->ptr = sdsgrowzero(o->ptr, offset + sdslen(value));
		// 将value的值复制到字符串中指定的位置
		memcpy((char *)o->ptr + offset, value, sdslen(value));
	}
	// 设置成功,返回新的字符串值给客户端
	addReplyLongLong(c, sdslen(o->ptr));
}

void getrangeCommand(redisClient *c) {
	robj *o;
	long long start, end;
	char* str, llbuf[32];
	size_t strlen;

	// 取出start参数
	if (getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK)
		return;

	// 取出end参数
	if (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)
		return;
	// 从数据库中查找键 c->argv[1]
	if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.emptybulk)) == NULL ||
		checkType(c, o, REDIS_STRING)) return;
	
	// 根据编码,对对象的值进行处理
	if (o->encoding == REDIS_ENCODING_INT) {
		str = llbuf;
		strlen = ll2string(llbuf, sizeof(llbuf), (long)o->ptr);
	}
	else {
		str = o->ptr;
		strlen = sdslen(str);
	}

	// 将负数索引转换为整数索引
	if (start < 0) start = strlen + start;
	if (end < 0) end = strlen + end;
	if (start < 0) start = 0;
	if (end < 0) end = 0;
	if ((unsigned)end >= strlen)  end = strlen - 1;

	if (start > end) {
		addReply(c, shared.emptybulk);
	}
	else {
		addReplyBulkBuffer(c, (char*)str + start, end - start + 1);
	}
}

void strlenCommand(redisClient *c) {
	robj *o;
	// 取出值对象,并进行类型检查
	if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.czero)) == NULL ||
		checkType(c, o, REDIS_STRING)) return;
	
	// 返回字符串值的长度
	addReplyLongLong(c, stringObjectLen(o));
}

void incrDecrCommand(redisClient *c, long long incr) {
	long long value, oldvalue;
	robj *o, *new;

	// 取出值对象 
	o = lookupKeyWrite(c->db, c->argv[1]);

	// 检查对象是否存在,以及类型是否正确
	if (o != NULL && checkType(c, o, REDIS_STRING)) return;
	// 取出对象的整数值,并保存到value参数中
	if (getLongFromObjectOrReply(c, o, &value, NULL) != REDIS_OK) {
		return;
	}

	// 检查加法操作执行之后值释放会溢出
	oldvalue = value;
	if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN - oldvalue)) ||
		(incr > 0 && oldvalue > 0 && incr > (LLONG_MAX - oldvalue))) {
		addReplyError(c, "increment or decrement would overflow");
		return;
	}

	// 进行加法操作,并将值保存到值对象中,然后用新的值对象替换原来的值对象
	value += incr;
	new = createStringObjectFromLongLong(value);
	
	dbOverwrite(c->db, c->argv[1], new);
	addReply(c, shared.colon);
	addReply(c, new);
	addReply(c, shared.crlf);
}

void incrCommand(redisClient *c) {
	incrDecrCommand(c, 1);
}

void decrCommand(redisClient *c) {
	incrDecrCommand(c, -1);
}

void mgetCommand(redisClient *c) { // 一次性获取多个值
	int j;
	addReplyMultiBulkLen(c, c->argc - 1);

	// 查找并返回所有输入键的值
	for (j = 1; j < c->argc; j++) {
		robj *o = lookupKeyRead(c->db, c->argv[j]);
		if (o == NULL) {
			addReply(c, shared.nullbulk);
		}
		else {
			if (o->type != REDIS_STRING) {
				// 值存在,但是不是字符串类型
				addReply(c, shared.nullbulk);
			}
			else {
				addReply(c, o); // 值存在,并且是字符串
			}
		}
	}
}


void msetGenericCommand(redisClient *c, int nx) {
	int j, busykeys = 0;
	// 键值参数不是成双成对出现的,格式不正确
	if ((c->argc % 2) == 0) {
		addReplyError(c, "wrong number of arguments for MSET");
		return;
	}

	// 如果nx参数为真,那么检查所有的输入键在数据库中是否存在,只要有一个键是存在的,
	// 那么就向客户端发送空回复,并放弃接下来的设置操作 
	if (nx) {
		for (j = 1; j < c->argc; j += 2) {
			if (lookupKeyWrite(c->db, c->argv[j]))
				busykeys++;
		}
		// 键存在的话,就发送空白回复,并放弃执行接下来的设置操作
		if (busykeys) {
			addReply(c, shared.czero);
			return;
		}
	}

	// 设置所有的键值对
	for (j = 1; j < c->argc; j += 2) {
		// 对值对象进行编码
		c->argv[j + 1] = tryObjectEncoding(c->argv[j + 1]);
		setKey(c->db, c->argv[j], c->argv[j + 1]);
	}
	// mset返回ok,而msetnx返回1
	addReply(c, nx ? shared.cone : shared.ok);
}

void msetCommand(redisClient *c) { // 设置多个键值对
	msetGenericCommand(c, 0);
}

void msetnxCommand(redisClient *c) {
	msetGenericCommand(c, 1);
}

void getsetCommand(redisClient *c) {
	// 取出并返回键的值对象
	if (getGenericCommand(c) == REDIS_ERR) return;

	// 编码键的新值
	c->argv[2] = tryObjectEncoding(c->argv[2]);

	setKey(c->db, c->argv[1], c->argv[2]);
}

void incrbyCommand(redisClient *c) {
	long long incr;

	if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) {
		return;
	}

	incrDecrCommand(c, incr); // 非常简单的一个命令,就是增长多少而已
}

void decrbyCommand(redisClient *c) { // 这也是非常简单的一个命令
	long long incr;

	if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;
	incrDecrCommand(c, -incr);
}
