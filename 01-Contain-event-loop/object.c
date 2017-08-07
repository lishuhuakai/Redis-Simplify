/* Redis Object implementation. */
#include "object.h"
#include <unistd.h>
#include <math.h>
#include <ctype.h>


/* 
 * 创建一个 REDIS_ENCODING_EMBSTR 编码的字符对象
 * 这个字符串对象中的 sds 会和字符串对象的 redisObject 结构一起分配
 * 因此这个字符也是不可修改的 
 */
robj *createEmbeddedStringObject(char *ptr, size_t len) {
	robj *o = zmalloc(sizeof(robj) + sizeof(struct sdshdr) + len + 1);
	struct sdshdr *sh = (void*)(o + 1);

	o->type = REDIS_STRING;
	o->encoding = REDIS_ENCODING_EMBSTR;
	o->ptr = sh + 1;
	o->refcount = 1;
	//o->lru = LRU_CLOCK();

	sh->len = len;
	sh->free = 0;
	if (ptr) {
		memcpy(sh->buf, ptr, len);
		sh->buf[len] = '\0';
	}
	else {
		memset(sh->buf, 0, len + 1);
	}
	return o;
}

/* 
 * 创建一个 REDIS_ENCODING_RAW 编码的字符对象
 * 对象的指针指向一个 sds 结构 
 */
robj *createRawStringObject(char *ptr, size_t len) {
	return createObject(REDIS_STRING, sdsnewlen(ptr, len));
}

/* Create a string object with EMBSTR encoding if it is smaller than
* REIDS_ENCODING_EMBSTR_SIZE_LIMIT, otherwise the RAW encoding is
* used.
*
* The current limit of 39 is chosen so that the biggest string object
* we allocate as EMBSTR will still fit into the 64 byte arena of jemalloc. */
#define REDIS_ENCODING_EMBSTR_SIZE_LIMIT 39
robj *createStringObject(char *ptr, size_t len) {
	if (len <= REDIS_ENCODING_EMBSTR_SIZE_LIMIT)
		return createEmbeddedStringObject(ptr, len);
	else
		return createRawStringObject(ptr, len);
}

/*
 * 创建一个新 robj 对象
 */
robj *createObject(int type, void *ptr) {

	robj *o = zmalloc(sizeof(*o));

	o->type = type;
	o->encoding = REDIS_ENCODING_RAW;
	o->ptr = ptr;
	o->refcount = 1;

	/* Set the LRU to the current lruclock (minutes resolution). */
	//o->lru = LRU_CLOCK();
	return o;
}


/*
 * 释放字符串对象
 */
void freeStringObject(robj *o) {
	if (o->encoding == REDIS_ENCODING_RAW) {
		sdsfree(o->ptr);
	}
}

/*
 * 为对象的引用计数减一
 *
 * 当对象的引用计数降为 0 时，释放对象。
 */
void decrRefCount(robj *o) {

	//if (o->refcount <= 0) redisPanic("decrRefCount against refcount <= 0");

	// 释放对象
	if (o->refcount == 1) {
		switch (o->type) {
		case REDIS_STRING: freeStringObject(o); break;
		//case REDIS_LIST: freeListObject(o); break;
		//case REDIS_SET: freeSetObject(o); break;
		//case REDIS_ZSET: freeZsetObject(o); break;
		//case REDIS_HASH: freeHashObject(o); break;
		default:
			//redisPanic("Unknown object type"); 
			break;
		}
		zfree(o);

		// 减少计数
	}
	else {
		o->refcount--;
	}
}

/* This variant of decrRefCount() gets its argument as void, and is useful
 * as free method in data structures that expect a 'void free_object(void*)'
 * prototype for the free method.
 *
 * 作用于特定数据结构的释放函数包装
 */
void decrRefCountVoid(void *o) {
	decrRefCount(o);
}

/*
 * 为对象的引用计数增一
 */
void incrRefCount(robj *o) {
	o->refcount++;
}

/*
 * 回复内容复制函数
 */
void *dupClientReplyValue(void *o) {
	incrRefCount((robj*)o);
	return o;
}
