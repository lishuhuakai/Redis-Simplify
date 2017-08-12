/* Redis Object implementation. */
#include "object.h"
#include "util.h"
#include "ziplist.h"
#include "networking.h"
#include "intset.h"
#include "t_zset.h"

#include <unistd.h>
#include <math.h>
#include <ctype.h>

extern struct sharedObjectsStruct shared;
extern struct dictType zsetDictType;

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
 * 释放列表对象
 */
void freeListObject(robj *o) {

	switch (o->encoding) {

	case REDIS_ENCODING_LINKEDLIST:
		listRelease((list*)o->ptr);
		break;

	case REDIS_ENCODING_ZIPLIST:
		zfree(o->ptr);
		break;

	default:
		assert(0);
	}
}

/*
 * 释放集合对象
 */
void freeSetObject(robj *o) {

	switch (o->encoding) {

	case REDIS_ENCODING_HT:
		dictRelease((dict*)o->ptr);
		break;

	case REDIS_ENCODING_INTSET:
		zfree(o->ptr);
		break;

	default:
		assert(NULL);
	}
}

/*
 * 释放有序集合对象
 */
void freeZsetObject(robj *o) {

	zset *zs;

	switch (o->encoding) {

	case REDIS_ENCODING_SKIPLIST:
		zs = o->ptr;
		dictRelease(zs->dict);
		zslFree(zs->zsl);
		zfree(zs);
		break;

	case REDIS_ENCODING_ZIPLIST:
		zfree(o->ptr);
		break;

	default:
		assert(NULL);
	}
}

/*
 * 释放哈希对象
 */
void freeHashObject(robj *o) {

	switch (o->encoding) {

	case REDIS_ENCODING_HT:
		dictRelease((dict*)o->ptr);
		break;

	case REDIS_ENCODING_ZIPLIST:
		zfree(o->ptr);
		break;

	default:
		assert(NULL);
		break;
	}
}

/*
 * 为对象的引用计数减一
 * 
 * 当对象的引用计数降为 0 时，释放对象。
 */
void decrRefCount(robj *o) {

	if (o->refcount <= 0) {
	   mylog("decrRefCount against refcount <= 0");
	   assert(0);
	}

	// 释放对象
	if (o->refcount == 1) {
		switch (o->type) {
		case REDIS_STRING: freeStringObject(o); break;
		case REDIS_LIST: freeListObject(o); break;
		case REDIS_SET: freeSetObject(o); break;
		case REDIS_ZSET: freeZsetObject(o); break;
		case REDIS_HASH: freeHashObject(o); break;
		default:
			mylog("Unknown object type");
			assert(0); 
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



/* 以新对象的形式，返回一个输入对象的解码版本（RAW 编码）。
 * 如果对象已经是 RAW 编码的，那么对输入对象的引用计数增一，
 * 然后返回输入对象。
 */
robj *getDecodedObject(robj *o) {
	robj *dec;

	if (sdsEncodedObject(o)) { /* sdsEncodedObject这个宏主要是判断o这个玩意里面保存的东西是否是字符 */
		incrRefCount(o); /* 如果已经是字符了,那么增加引用计数即可 */
		return o;
	}

	/* 解码对象，将对象的值从整数转换为字符串 */
	if (o->type == REDIS_STRING && o->encoding == REDIS_ENCODING_INT) {
		char buf[32];
		ll2string(buf, 32, (long)o->ptr);
		dec = createStringObject(buf, strlen(buf));
		return dec;
	}
	else {
		// todo
	}
}

robj *dupStringObject(robj *o) {
	robj *d;
	switch (o->encoding) {
	case REDIS_ENCODING_RAW:
		return createRawStringObject(o->ptr, sdslen(o->ptr));
	case REDIS_ENCODING_EMBSTR:
		return createEmbeddedStringObject(o->ptr, sdslen(o->ptr));
	case REDIS_ENCODING_INT:
		d = createObject(REDIS_STRING, NULL);
		d->encoding = REDIS_ENCODING_INT;
		d->ptr = o->ptr; /* 居然指向同一个对象 */
		return d;
	default:
		break;
	}
}

/*
 * 尝试从对象 o 中取出整数值，
 * 或者尝试将对象 o 所保存的值转换为整数值，
 * 并将这个整数值保存到 *target 中。
 *
 * 如果 o 为 NULL ，那么将 *target 设为 0 。
 *
 * 如果对象 o 中的值不是整数，并且不能转换为整数，那么函数返回 REDIS_ERR 。
 *
 * 成功取出或者成功进行转换时，返回 REDIS_OK 。
 *
 * T = O(N)
 */
int getLongLongFromObject(robj *o, long long *target) {
	long long value;
	char *eptr;

	if (o == NULL) {
		// o 为 NULL 时，将值设为 0.
		value = 0;
	}
	else {
		if (sdsEncodedObject(o)) { // 如果o是字符类型的redisObject
			errno = 0;
			value = strtoll(o->ptr, &eptr, 10); /* 调用函数试图将string转换为long long */ 
			if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' ||
				errno == ERANGE)
				return REDIS_ERR;
		}
		else if (o->encoding == REDIS_ENCODING_INT) {
			// 对于 REDIS_ENCODING_INT 编码的整数值,直接将它的值保存到 value 中
			value = (long)o->ptr;
		}
		else {
			// todo
		}
	}

	/* 保存值到指针 */
	if (target) *target = value;
	return REDIS_OK;
}


/*
 * 尝试从对象 o 中取出整数值，
 * 或者尝试将对象 o 中的值转换为整数值，
 * 并将这个得出的整数值保存到 *target 。
 *
 * 如果取出/转换成功的话，返回 REDIS_OK 。
 * 否则，返回 REDIS_ERR ，并向客户端发送一条出错回复。
 *
 * T = O(N)
 */
int getLongLongFromObjectOrReply(redisClient *c, robj *o, long long *target, const char *msg) {

	long long value;

	if (getLongLongFromObject(o, &value) != REDIS_OK) {
		if (msg != NULL) {
			addReplyError(c, (char*)msg); // 如果携带了msg,则向对方发送msg
		}
		else {
			addReplyError(c, "value is not an integer or out of range");
		}
		return REDIS_ERR;
	}

	*target = value;
	return REDIS_OK;
}

/*
 * 尝试对字符串对象进行编码,以节约内存 
 */
robj *tryObjectEncoding(robj *o) {
	long value;
	sds s = o->ptr;
	size_t len;
	// 只有在字符串的编码为RAW或者EMBSTR时才尝试进行编码
	if (!sdsEncodedObject(o)) return o;
	// 不对共享的对象进行编码
	if (o->refcount > 1) return o;

	// 对字符串进行检查,只对长度小于或者等于21字节,并且可以解释为整数的字符串进行编码
	len = sdslen(s);
	if (len <= 21 && string2l(s, len, &value)) {
		// 做了一点简化,为了不至于太复杂
		if (o->encoding == REDIS_ENCODING_RAW) sdsfree(o->ptr);
		o->encoding = REDIS_ENCODING_INT;
		o->ptr = (void *)value;
		return o;
	}
	// 字符串的长度没有超过embstr的限制
	if (len <= REDIS_ENCODING_EMBSTR_SIZE_LIMIT) {
		robj *emb;
		if (o->encoding == REDIS_ENCODING_EMBSTR) return o; // 不做任何改变
		emb = createEmbeddedStringObject(s, sdslen(s));
		decrRefCount(o); // 手动地操纵引用计数真是一个麻烦事 
		return emb;
	}
	return o;
}

/*
 * 返回字符串对象中字符串值的长度
 */
size_t stringObjectLen(robj *o) {
	assert(o->type == REDIS_STRING);

	if (sdsEncodedObject(o)) {
		return sdslen(o->ptr);
	}
	else {
		// 如果采用int编码,计算将这个值转换为字符串需要多少字节
		char buf[32];
		return ll2string(buf, 32, (long)o->ptr);
	}
}


/*
 * 尝试从对象o中取出long类型值
 * 或者尝试将对象o中的值转换为long类型值
 * 并将这个得出的整数值保存到*target上.
 * 如果取出/转换成功的话,返回REDIS_OK.
 * 否则返回REDIS_ERR,并向客户端发送一条msg出错回复.
 */
int getLongFromObjectOrReply(redisClient *c, robj *o, long long *target, const char* msg) {
	long long value;
	/* 先尝试以long long类型取出值 */
	if (getLongLongFromObjectOrReply(c, o, &value, msg) != REDIS_OK) {
		return REDIS_ERR;
	}

	/* 然后检查值是否在long类型的范围之内 */
	if (value < LONG_MIN || value > LONG_MAX) {
		if (msg != NULL)
			addReplyError(c, (char *)msg);
		else
			addReplyError(c, "value is out of range");
		return REDIS_ERR;
	}
	*target = value;
	return REDIS_OK;
}

/*
 * 根据传入的整数值,创建一个字符串对象
 * 这个字符串的对象保存的可以是INT编码的long值
 * 也可以是RAW编码的,被转换成字符串的long long值.
 */
robj *createStringObjectFromLongLong(long long value) {
	robj *o;

	// 如果value的大小在REDIS共享整数范围之内
	if (value >= 0 && value < REDIS_SHARED_INTEGERS) {
		incrRefCount(shared.integers[value]);
		o = shared.integers[value];
	}
	else { // 否则的话就要重新创建一个整数对象
		if (value >= LONG_MIN && value <= LONG_MAX) {
			o = createObject(REDIS_STRING, NULL);
			o->encoding = REDIS_ENCODING_INT;
			o->ptr = (void *)((long)value);
		}
		else {
			o = createObject(REDIS_STRING, sdsfromlonglong(value));
		}
	}
	return o;
}

/*
 * 创建一个ZIPLIST编码的哈希对象
 */
robj* createHashObject(void) {
	unsigned char *zl = ziplistNew();
	robj *o = createObject(REDIS_HASH, zl);
	o->encoding = REDIS_ENCODING_ZIPLIST;
	return o;
}

/*
 * 创建一个ZIPLIST编码的列表对象
 */
robj *createZiplistObject(void) {
	unsigned char *zl = ziplistNew();
	robj *o = createObject(REDIS_LIST, zl);
	o->encoding = REDIS_ENCODING_ZIPLIST;
	return o;
}

/*
 * 创建一个INTSET编码的集合对象
 */
robj* createIntsetObject(void) {
	intset *is = intsetNew();
	robj *o = createObject(REDIS_SET, is);
	o->encoding = REDIS_ENCODING_INTSET;
	return o;
}

/*
 * 检查对象 o 中的值能否表示为 long long 类型：
 *
 *  - 可以则返回 REDIS_OK ，并将 long long 值保存到 *llval 中。
 *
 *  - 不可以则返回 REDIS_ERR
 */
int isObjectRepresentableAsLongLong(robj *o, long long *llval) {

	/* INT 编码的 long 值总是能保存为 long long */
	if (o->encoding == REDIS_ENCODING_INT) {
		if (llval) *llval = (long)o->ptr;
		return REDIS_OK;
		/* 如果是字符串的话，那么尝试将它转换为 long long */
	}
	else {
		return string2ll(o->ptr, sdslen(o->ptr), llval) ? REDIS_OK : REDIS_ERR;
	}
}

/*
 * 尝试从对象中取出 double 值
 *
 *  - 转换成功则将值保存在 *target 中，函数返回 REDIS_OK
 *
 *  - 否则，函数返回 REDIS_ERR
 */
int getDoubleFromObject(robj *o, double *target) {
	double value;
	char *eptr;

	if (o == NULL) {
		value = 0;
	}
	else {
		assert(o->type == REDIS_STRING);
		/* 尝试从字符串中转换 double 值 */
		if (sdsEncodedObject(o)) {
			errno = 0;
			value = strtod(o->ptr, &eptr);
			if (isspace(((char*)o->ptr)[0]) ||
				eptr[0] != '\0' ||
				(errno == ERANGE &&
				(value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
				errno == EINVAL ||
				isnan(value))
				return REDIS_ERR;
		}
		else if (o->encoding == REDIS_ENCODING_INT) { /* INT 编码 */
			value = (long)o->ptr;

		}
		else {
			assert(0);
		}
	}
	/* 返回值 */
	*target = value;
	return REDIS_OK;
}

/*
 * 创建一个 SKIPLIST 编码的有序集合
 */
robj *createZsetObject(void) {

	zset *zs = zmalloc(sizeof(*zs));

	robj *o;

	zs->dict = dictCreate(&zsetDictType, NULL);
	zs->zsl = zslCreate();

	o = createObject(REDIS_ZSET, zs);

	o->encoding = REDIS_ENCODING_SKIPLIST;

	return o;
}

/*
 * 创建一个 ZIPLIST 编码的有序集合
 */
robj* createZsetZiplistObject(void) {
	unsigned char *zl = ziplistNew(); /* ziplist貌似是压缩链表 */
	robj *o = createObject(REDIS_ZSET, zl);
	o->encoding = REDIS_ENCODING_ZIPLIST;
	return o;
}

/*
 * 根据 flags 的值，决定是使用 strcmp() 或者 strcoll() 来对比字符串对象。
 *
 * 注意，因为字符串对象可能实际上保存的是整数值，
 * 如果出现这种情况，那么函数先将整数转换为字符串，
 * 然后再对比两个字符串，
 * 这种做法比调用 getDecodedObject() 更快
 *
 * 当 flags 为 REDIS_COMPARE_BINARY 时，
 * 对比以二进制安全的方式进行。
 */

#define REDIS_COMPARE_BINARY (1<<0)
#define REDIS_COMPARE_COLL (1<<1)


int compareStringObjectsWithFlags(robj *a, robj *b, int flags) {
	assert( a->type == REDIS_STRING && b->type == REDIS_STRING);

	char bufa[128], bufb[128], *astr, *bstr;
	size_t alen, blen, minlen;

	if (a == b) return 0;

	/* 指向字符串值，并在有需要时，将整数转换为字符串 a */
	if (sdsEncodedObject(a)) {
		astr = a->ptr;
		alen = sdslen(astr);
	}
	else {
		alen = ll2string(bufa, sizeof(bufa), (long)a->ptr);
		astr = bufa;
	}

	/* 同样处理字符串 b */
	if (sdsEncodedObject(b)) {
		bstr = b->ptr;
		blen = sdslen(bstr);
	}
	else {
		blen = ll2string(bufb, sizeof(bufb), (long)b->ptr);
		bstr = bufb;
	}
	/* 对比 */
	if (flags & REDIS_COMPARE_COLL) {
		return strcoll(astr, bstr);
	}
	else {
		int cmp;
		minlen = (alen < blen) ? alen : blen;
		cmp = memcmp(astr, bstr, minlen);
		if (cmp == 0) return alen - blen;
		return cmp;
	}
}


/* Wrapper for compareStringObjectsWithFlags() using binary comparison. */
int compareStringObjects(robj *a, robj *b) {
	return compareStringObjectsWithFlags(a, b, REDIS_COMPARE_BINARY);
}

/*
 * 如果两个对象的值在字符串的形式上相等，那么返回 1 ， 否则返回 0 。
 *
 * 这个函数做了相应的优化，所以比 (compareStringObject(a, b) == 0) 更快一些。
 */
int equalStringObjects(robj *a, robj *b) {

	/* 对象的编码为 INT ，直接对比值
	 * 这里避免了将整数值转换为字符串，所以效率更高 */
	if (a->encoding == REDIS_ENCODING_INT &&
		b->encoding == REDIS_ENCODING_INT) {
		/* If both strings are integer encoded just check if the stored
		* long is the same. */
		return a->ptr == b->ptr;

		// 进行字符串对象
	}
	else {
		return compareStringObjects(a, b) == 0;
	}
}

/* Note: this function is defined into object.c since here it is where it
 * belongs but it is actually designed to be used just for INCRBYFLOAT */
/*
 * 根据传入的 long double 值，为它创建一个字符串对象
 *
 * 对象将 long double 转换为字符串来保存
 */
robj *createStringObjectFromLongDouble(long double value) {
	char buf[256];
	int len;

	/* We use 17 digits precision since with 128 bit floats that precision
	* after rounding is able to represent most small decimal numbers in a way
	* that is "non surprising" for the user (that is, most small decimal
	* numbers will be represented in a way that when converted back into
	* a string are exactly the same as what the user typed.) */
	// 使用 17 位小数精度，这种精度可以在大部分机器上被 rounding 而不改变
	len = snprintf(buf, sizeof(buf), "%.17Lf", value);

	/* Now remove trailing zeroes after the '.' */
	// 移除尾部的 0 
	// 比如 3.1400000 将变成 3.14
	// 而 3.00000 将变成 3
	if (strchr(buf, '.') != NULL) {
		char *p = buf + len - 1;
		while (*p == '0') {
			p--;
			len--;
		}
		// 如果不需要小数点，那么移除它
		if (*p == '.') len--;
	}

	// 创建对象
	return createStringObject(buf, len);
}

/*
 * 创建一个 LINKEDLIST 编码的列表对象
 */
robj *createListObject(void) {

	list *l = listCreate();

	robj *o = createObject(REDIS_LIST, l);

	listSetFreeMethod(l, decrRefCountVoid);

	o->encoding = REDIS_ENCODING_LINKEDLIST;

	return o;
}
