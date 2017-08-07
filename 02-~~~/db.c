#include "networking.h"
#include "db.h"
#include "object.h"
#include <signal.h>
#include <ctype.h>

/*
 * 从数据库db中取出键key的值(对象)
 * 如果key的值存在,那么返回该值,否则,返回NULL.
 */
robj *lookupKey(redisDb *db, robj *key) {
	// 查找键空间
	char *str = key->ptr;
	dictEntry *de = dictFind(db->dict, key->ptr);

	if (de) {
		robj *val = dictGetVal(de);
		return val;
	}
	else
		return NULL;
}

/*
 * 为执行读取操作而取出键key在数据库db中的值,并根据是否成功找到值,更新服务器中的命中
 * 或者不命中信息,找到时返回值对象,没有找到是返回NULL.
 */
robj *lookupKeyRead(redisDb *db, robj *key) {
	robj *val;
	
	// 从数据库中取出键的值
	val = lookupKey(db, key);

	// todo
	return val;
}

/*
 * 为执行读取操作而从数据库中查找返回key的值.
 * 如果key存在,那么返回key的值对象,否则的话,向客户端发送Reply参数中的信息,并返回NULL.
 */
robj *lookupKeyReadOrReply(redisClient *c, robj *key, robj *reply) {
	// 查找
	robj *o = lookupKeyRead(c->db, key);

	// 决定是否发送信息
	if (!o) addReply(c, reply);
	return o;
}

/* 为执行写入操作而取出键 key 在数据库 db 中的值。
 * 和 lookupKeyRead 不同，这个函数不会更新服务器的命中/不命中信息。
 * 找到时返回值对象，没找到返回 NULL 。
 */
robj* lookupKeyWrite(redisDb *db, robj *key) {
	//  删除过期键
	return lookupKey(db, key);
}

/* 尝试将键值对 key 和 val 添加到数据库中。
 * 调用者负责对 key 和 val 的引用计数进行增加。
 * 程序在键已经存在时会停止。
 */
void dbAdd(redisDb *db, robj *key, robj *val) {
	// 复制键名
	sds copy = sdsdup(key->ptr);
	// 尝试添加键值对
	int retval = dictAdd(db->dict, copy, val);

	// 如果键已经存在,那么停止
	// todo
}

/* 为已存在的键关联一个新值。
 * 调用者负责对新值 val 的引用计数进行增加。
 */
void dbOverwrite(redisDb *db, robj *key, robj *val) {
	dictEntry *de = dictFind(db->dict, key->ptr);
	dictReplace(db->dict, key->ptr, val);
}

/* 高层次的 SET 操作函数。
 *
 * 这个函数可以在不管键 key 是否存在的情况下，将它和 val 关联起来。
 *
 * 1) The ref count of the value object is incremented.
 *    值对象的引用计数会被增加
 *
 * 2) clients WATCHing for the destination key notified.
 *    监视键 key 的客户端会收到键已经被修改的通知
 *
 * 3) The expire time of the key is reset (the key is made persistent).
 *    键的过期时间会被移除（键变为持久的）
 */
void setKey(redisDb *db, robj *key, robj *val) {
	// 添加或者覆写数据库中的键值对
	if (lookupKeyWrite(db, key) == NULL) {
		dbAdd(db, key, val);
	}
	else {
		dbOverwrite(db, key, val);
	}
	incrRefCount(val);
	// 移除键的过期时间
	//removeExpire(db, key);
}


/*
 * 将客户端的目标数据库切换成id所指定的数据库
 */
extern struct redisServer server;
int selectDb(redisClient *c, int id) {
	/* 确保id在正确范围内 */
	if (id < 0 || id >= server.dbnum)
		return REDIS_ERR;
	/* 切换数据库(更新指针) */
	c->db = &server.db[id];
	return REDIS_OK;
}