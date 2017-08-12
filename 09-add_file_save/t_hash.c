#include "t_hash.h"
#include "dict.h"
#include "db.h"
#include "ziplist.h"
#include "t_string.h"
#include "networking.h"
#include "object.h"
#include "util.h"
#include <math.h>

/*============================ Variable and Function Declaration ======================== */
extern struct sharedObjectsStruct shared;
extern struct redisServer server;
extern struct dictType hashDictType;
robj *getDecodedObject(robj *o);


/*
 * 将给定field及其value从hash表中删除
 * 删除成功返回1,因为域不存在而造成的删除失败返回0
 */
int hashTypeDelete(robj *o, robj *field) {
	int deleted = 0;
	// 从ziplist中删除
	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *zl, *fptr;
		field = getDecodedObject(field);
		// todo
	}
}

/*
 * 返回哈希表field-value对的数目
 */
unsigned long hashTypeLength(robj *o) {
	unsigned long length = ULONG_MAX;
	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		// ziplist中,每个field-value对都需要使用两个节点来保存
		length = ziplistLen(o->ptr) / 2;
	}
	else if (o->encoding == REDIS_ENCODING_HT) {
		length = dictSize((dict *)o->ptr);
	}
	else {
		assert(0);
	}
	return length;
}

/*
 * hdel删除一个hash对象
 */
void hdelCommand(redisClient *c) {
	robj *o;
	int j, deleted = 0, keyremoved = 0;

	// 取出对象
	if (((o = lookupKeyWriteOrReply(c, c->argv[1], shared.czero)) == NULL) ||
		checkType(c, o, REDIS_HASH)) return;
	// 删除指定域值对
	for (j = 2; j < c->argc; j++) {
		if (hashTypeDelete(o, c->argv[j])) {
			// 成功删除一个域值对时进行计数
			deleted++;
			// 如果哈希已经为空,那么删除这个对象
			if (hashTypeLength(o) == 0) {
				dbDelete(c->db, c->argv[1]);
				keyremoved = 1;
				break;
			}
		}
	}
	addReplyLongLong(c, deleted);
}


/* 从 ziplist 编码的 hash 中取出和 field 相对应的值。
 *
 * 参数：
 *  field   域
 *  vstr    值是字符串时，将它保存到这个指针
 *  vlen    保存字符串的长度
 *  ll      值是整数时，将它保存到这个指针
 *
 * 查找失败时，函数返回 - 1 。
 * 查找成功时，返回 0 。
 */
int hashTypeGetFromZiplist(robj *o, robj *field,
	unsigned char **vstr,
	unsigned int *vlen,
	long long *vll)
{
	unsigned char *zl, *fptr = NULL, *vptr = NULL;
	int ret;

	assert(o->encoding == REDIS_ENCODING_ZIPLIST);

	field = getDecodedObject(field);
	// 遍历ziplist,查找域的位置
	zl = o->ptr;
	fptr = ziplistIndex(zl, ZIPLIST_HEAD);
	if (fptr != NULL) {
		// 定位包含域的节点
		fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
		if (fptr != NULL) {
			vptr = ziplistNext(zl, fptr);
			assert(vptr != NULL);
		}
	}
	decrRefCount(field);
	// 从ziplist节点中取出值
	if (vptr != NULL) {
		ret = ziplistGet(vptr, vstr, vlen, vll);
		assert(ret);
		return 0;
	}
	return -1; // 表示没有找到
}

/*
 * 从REDIS_ENCODING_HT编码的hash中取出和field相对于的值
 * 成功找到值时返回0,没有找到返回-1.
 */
int hashTypeGetFromHashTable(robj *o, robj *field, robj **value) {
	dictEntry *de;
	assert(o->encoding == REDIS_ENCODING_HT);
	// 在字典中查找域/键
	de = dictFind(o->ptr, field);
	// 键不存在
	if (de == NULL) return -1;
	*value = dictGetVal(de);
	return 0;
}

/*
 * 检查给定域feild是否存在于hash对象o中.
 * 存在返回1,不存在返回0.
 */
int hashTypeExists(robj *o, robj *field) {
	// 检查ziplist
	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *vstr = NULL;
		unsigned int vlen = UINT_MAX;
		long long vll = LLONG_MAX;
		if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;
	}
	else if (o->encoding == REDIS_ENCODING_HT) {
		robj *aux;
		if (hashTypeGetFromHashTable(o, field, &aux) == 0) return 1;
	}
	else {
		assert(0);
	}
	return 0;
}

void hexistsCommand(redisClient *c) {
	robj *o;
	// 取出哈希对象
	if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.czero)) == NULL ||
		checkType(c, o, REDIS_HASH)) return;
	// 检查给定域是否存在
	addReply(c, hashTypeExists(o, c->argv[2]) ? shared.cone : shared.czero);
}

/*
 * 按key在数据库中查找并返回想对应的哈希对象.
 * 如果对象不存在,那么创建一个新哈希对象并返回.
 */
robj *hashTypeLookupWriteOrCreate(redisClient *c, robj *key) {
	robj *o = lookupKeyWrite(c->db, key);
	if (o == NULL) {
		o = createHashObject();
		dbAdd(c->db, key, o); // 添加一个新的hash对象
	}
	else { // 对象存在,检查类型
		if (o->type != REDIS_HASH) {
			addReply(c, shared.wrongtypeerr);
			return NULL;
		}
	}
	// 返回对象
	return o;
}

/*
 * 创建一个哈希类型的迭代器
 * hashTypeIterator 类型定义在 redis.h
 *
 * 复杂度：O(1)
 *
 * 返回值：
 *  hashTypeIterator
 */
hashTypeIterator *hashTypeInitIterator(robj *subject) {
	hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
	hi->subject = subject;
	// 记录编码
	hi->encoding = subject->encoding;
	// 以ziplist的方式初始化迭代器
	if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
		hi->fptr = NULL;
		hi->vptr = NULL;
	}
	else if (hi->encoding == REDIS_ENCODING_HT) { // 以字典的方式初始化迭代器
		hi->di = dictGetIterator(subject->ptr);
	}
	else
		assert(0);
}

/*
 * 从 ziplist 编码的哈希中，取出迭代器指针当前指向节点的域或值。
 */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
	unsigned char **vstr,
	unsigned int *vlen,
	long long *vll)
{
	int ret;
	assert(hi->encoding == REDIS_ENCODING_ZIPLIST);
	
	// 取出键
	if (what & REDIS_HASH_KEY) {
		ret = ziplistGet(hi->fptr, vstr, vlen, vll);
		assert(ret);
	}
	else { // 取出值
		ret = ziplistGet(hi->vptr, vstr, vlen, vll);
		assert(ret);
	}
}

/*
 * 根据迭代器的指针，从字典编码的哈希中取出所指向节点的 field 或者 value 。
 */
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst) {
	assert(hi->encoding == REDIS_ENCODING_HT);
	if (what & REDIS_HASH_KEY) { // 取出键
		*dst = dictGetKey(hi->de);
	}
	else { // 取出值
		*dst = dictGetVal(hi->de);
	}
}
/*
 * 一个非 copy-on-write 友好，但是层次更高的 hashTypeCurrent() 函数，
 * 这个函数返回一个增加了引用计数的对象，或者一个新对象.
 * 当使用完返回对象之后，调用者需要对对象执行 decrRefCount().
 */
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what) {
	robj *dst;
	if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *vstr = NULL;
		unsigned int vlen = UINT_MAX;
		long long vll = LLONG_MAX;

		// 取出键或值
		hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
		// 创建键或值的对象
		if (vstr) {
			dst = createStringObject((char*)vstr, vlen);
		}
		else {
			dst = createStringObjectFromLongLong(vll);
		}
	}
	else if (hi->encoding == REDIS_ENCODING_HT) { /* 字典 */
		// 取出键或者值
		hashTypeCurrentFromHashTable(hi, what, &dst);
		incrRefCount(dst);
	}
	else {
		assert(0);
	}
	return dst;
}

/* 
 * 获取哈希中的下一个节点，并将它保存到迭代器。
 *
 * 如果获取成功，返回 REDIS_OK ，
 *
 * 如果已经没有元素可获取（为空，或者迭代完毕），那么返回 REDIS_ERR 。
 */
int hashTypeNext(hashTypeIterator *hi) {
	if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char* zl;
		unsigned char *fptr, *vptr;
		zl = hi->subject->ptr;
		fptr = hi->fptr;
		vptr = hi->vptr;
		// 第一次执行时,初始化指针
		if (fptr == NULL) {
			assert(vptr == NULL);
			fptr = ziplistIndex(zl, 0);
		}
		else { // 获取下一个迭代节点
			assert(vptr != NULL);
			fptr = ziplistNext(zl, vptr);
		}
		// 迭代完毕,或者ziplist为空
		if (fptr == NULL) return REDIS_ERR;
		/* 记录值的指针 */
		vptr = ziplistNext(zl, fptr);
		assert(vptr != NULL);
		hi->fptr = fptr;
		hi->vptr = vptr;
	}
	else if (hi->encoding == REDIS_ENCODING_HT) {
		if ((hi->de = dictNext(hi->di)) == NULL) return REDIS_ERR;
	}
	else {
		assert(0);
	}
	return REDIS_OK;
}

/*
 * 释放迭代器
 */
void hashTypeReleaseIterator(hashTypeIterator *hi) {
	// 释放字典迭代器
	if (hi->encoding == REDIS_ENCODING_HT) {
		dictReleaseIterator(hi->di);
	}
	// 释放ziplist迭代器
	zfree(hi);
}

/*
 * 将一个ziplist编码的哈希对象o转换成其他编码
 */
void hashTypeConvertZiplist(robj *o, int enc) {
	assert(o->encoding == REDIS_ENCODING_ZIPLIST);

	// 如果输入是ZIPLIST,那么不做动作
	if (enc == REDIS_ENCODING_ZIPLIST) {

	}
	else if (enc == REDIS_ENCODING_HT) {
		hashTypeIterator *hi;
		dict *dict;
		int ret;

		// 创建哈希迭代器
		hi = hashTypeInitIterator(o);
		// 创建空白的新字典
		dict = dictCreate(&hashDictType, NULL);
		// 遍历整个ziplist
		while (hashTypeNext(hi) != REDIS_ERR) {
			robj *field, *value;
			// 取出ziplist里的键
			field = hashTypeCurrentObject(hi, REDIS_HASH_KEY);
			field = tryObjectEncoding(field);
			// 取出ziplist里的值
			value = hashTypeCurrentObject(hi, REDIS_HASH_VALUE);
			value = tryObjectEncoding(value);
			// 将键值对添加到字典中
			ret = dictAdd(dict, field, value);
			if (ret != DICT_OK) {
				assert(0);
			}
		}
		// 释放ziplist的迭代器
		hashTypeReleaseIterator(hi);
		// 释放对象原来的ziplist
		zfree(o->ptr);
		// 更新哈希的编码和值对象
		o->encoding = REDIS_ENCODING_HT;
		o->ptr = dict;
	}
	else {
		assert(0);
	}
}

/*
 * 对哈希对象o的编码方式进行转换,目前只支持将ZIPLIST编码转换成HT编码
 */
void hashTypeConvert(robj *o, int enc) {
	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		hashTypeConvertZiplist(o, enc);
	}
	else {
		assert(0);
	}
}


/*
 * 对 argv 数组中的多个对象进行检查，
 * 看是否需要将对象的编码从 REDIS_ENCODING_ZIPLIST 转换成 REDIS_ENCODING_HT
 * 注意程序只检查字符串值，因为它们的长度可以在常数时间内取得。
 */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
	int i;
	// 如果对象不是ziplist编码,那么直接返回
	if (o->encoding != REDIS_ENCODING_ZIPLIST) return;
	// 检查所有输入对象,看它们的字符串值是否超过了指定长度
	char *str1 = argv[2]->ptr;
	for (i = start; i <= end; i++) {
		if (sdsEncodedObject(argv[i]) &&
			sdslen(argv[i]->ptr) > server.hash_max_ziplist_value) {
			// 将对象的编码转换为REDIS_ENCODING_HT
			hashTypeConvert(o, REDIS_ENCODING_HT);
			break;
		}
	}
}

/*
 * 将给定的 field-value 对添加到 hash 中，
 * 如果 field 已经存在，那么删除旧的值，并关联新值。
 *
 * 这个函数负责对 field 和 value 参数进行引用计数自增。
 *
 * 返回 0 表示元素已经存在，这次函数调用执行的是更新操作。
 *
 * 返回 1 则表示函数执行的是新添加操作。
 */
int hashTypeSet(robj *o, robj *field, robj *value) {
	int update = 0;

	// 添加到 ziplist
	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *zl, *fptr, *vptr;

		// 解码成字符串或者数字
		field = getDecodedObject(field);
		value = getDecodedObject(value);

		// 遍历整个 ziplist, 尝试查找并更新 field （如果它已经存在的话）
		zl = o->ptr;
		fptr = ziplistIndex(zl, ZIPLIST_HEAD);
		if (fptr != NULL) {
			// 定位到域 field
			fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
			if (fptr != NULL) {
				// 定位到域的值
				vptr = ziplistNext(zl, fptr);
				assert(vptr != NULL);

				// 标识这次操作为更新操作
				update = 1;

				// 删除旧的键值对
				zl = ziplistDelete(zl, &vptr);

				/* 添加新的键值对 */
				zl = ziplistInsert(zl, vptr, value->ptr, sdslen(value->ptr));
			}
		}

		// 如果这不是更新操作，那么这就是一个添加操作
		if (!update) {
			// 将新的 field-value 对推入到 ziplist 的末尾
			zl = ziplistPush(zl, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL);
			zl = ziplistPush(zl, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);
		}

		// 更新对象指针
		o->ptr = zl;

		// 释放临时对象
		decrRefCount(field);
		decrRefCount(value);

		// 检查在添加操作完成之后，是否需要将 ZIPLIST 编码转换成 HT 编码
		if (hashTypeLength(o) > server.hash_max_ziplist_entries)
			hashTypeConvert(o, REDIS_ENCODING_HT);
	}
	else if (o->encoding == REDIS_ENCODING_HT) {	// 添加到字典

		/* 添加或替换键值对到字典
		 * 添加返回 1 ，替换返回 0 */
		if (dictReplace(o->ptr, field, value)) { /* Insert */
			incrRefCount(field);
		}
		else { /* Update */
			update = 1;
		}
		incrRefCount(value);
	}
	else {
		assert(0);
	}

	// 更新/添加指示变量
	return update;
}


/*
 * 当subject的编码为REDIS_ENCODING_HT时,
 * 尝试对对象o1和o2进行编码,以节省更多内存.
 */
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2) {
	if (subject->encoding == REDIS_ENCODING_HT) {
		if (o1) *o1 = tryObjectEncoding(*o1);
		if (o2) *o2 = tryObjectEncoding(*o2);
	}
}

void hsetCommand(redisClient *c) {
	int update;
	robj *o;
	char *str1 = c->argv[1]->ptr;
	/* 取出或者新创建哈希对象 */
	if ((o = hashTypeLookupWriteOrCreate(c, c->argv[1])) == NULL) return;
	/* 如果需要的话,转换哈希对象的编码 */
	hashTypeTryConversion(o, c->argv, 2, 3);
	/* 编码field和value对象以节省空间 */
	hashTypeTryObjectEncoding(o, &c->argv[2], &c->argv[3]);
	/* 设置field和value到hash */
	update = hashTypeSet(o, c->argv[2], c->argv[3]);

	server.dirty++; /* 将服务器设为脏 */
	/* 返回状态,显示field-value对是新添加还是更新 */
	addReply(c, update ? shared.czero : shared.cone);
}

/* Higher level function of hashTypeGet*() that always returns a Redis
 * object (either new or with refcount incremented), so that the caller
 * can retain a reference or call decrRefCount after the usage.
 *
 * The lower level function can prevent copy on write so it is
 * the preferred way of doing read operations.
 *
 * 多态 GET 函数，从 hash 中取出域 field 的值，并返回一个值对象。
 *
 * 找到返回值对象，没找到返回 NULL 。
 */
robj *hashTypeGetObject(robj *o, robj *field) {
	robj *value = NULL;

	// 从 ziplist 中取出值
	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *vstr = NULL;
		unsigned int vlen = UINT_MAX;
		long long vll = LLONG_MAX;

		if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) {
			// 创建值对象
			if (vstr) {
				value = createStringObject((char*)vstr, vlen);
			}
			else {
				value = createStringObjectFromLongLong(vll);
			}
		}

		// 从字典中取出值
	}
	else if (o->encoding == REDIS_ENCODING_HT) {
		robj *aux;

		if (hashTypeGetFromHashTable(o, field, &aux) == 0) {
			incrRefCount(aux);
			value = aux;
		}

	}
	else {
		mylog("Unknown hash encoding");
		assert(0);
	}

	// 返回值对象，或者 NULL
	return value;
}

/*
 * 辅助函数:将哈希域field的值添加到回复中
 */
static void addHashFieldToReply(redisClient *c, robj *o, robj *field) {
	int ret;
	if (o == NULL) { /* 对象不存在 */
		addReply(c, shared.nullbulk);
		return;
	}
	/* ziplist编码 */
	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *vstr = NULL;
		unsigned int vlen = UINT_MAX;
		long long vll = LLONG_MAX;
		/* 取出值 */
		ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
		if (ret < 0) {
			addReply(c, shared.nullbulk);
		}
		else {
			if (vstr) {
				addReplyBulkCBuffer(c, vstr, vlen);
			}
			else {
				addReplyBulkLongLong(c, vll);
			}
		}
	}
	else if (o->encoding == REDIS_ENCODING_HT) {
		robj *value;
		/* 取出值 */
		ret = hashTypeGetFromHashTable(o, field, &value);
		if (ret < 0) {
			addReply(c, shared.nullbulk);
		}
		else {
			addReplyBulk(c, value);
		}
	}
	else
		assert(0);
}

void hgetCommand(redisClient *c) {
	robj *o;

	char *str1 = c->argv[1]->ptr;
	if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk)) == NULL ||
		checkType(c, o, REDIS_HASH)) return;
	/* 取出并返回域的值 */
	addHashFieldToReply(c, o, c->argv[2]);
}

/*
 * 从迭代器当前指向的节点取出哈希的field或value
 */
static void addHashIteratorCursorToReply(redisClient *c, hashTypeIterator *hi, int what) {
	if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *vstr = NULL;
		unsigned int vlen = UINT_MAX;
		long long vll = LLONG_MAX;

		hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
		if (vstr) {
			addReplyBulkCBuffer(c, vstr, vlen);
		}
		else {
			addReplyBulkLongLong(c, vll);
		}
	}
	else if (hi->encoding == REDIS_ENCODING_HT) {
		robj *value;

		hashTypeCurrentFromHashTable(hi, what, &value);
		addReplyBulk(c, value);
	}
	else
		assert(0);
}

void genericHgetallCommand(redisClient *c, int flags) {
	robj *o;
	hashTypeIterator *hi;
	int multiplier = 0;
	int length, count = 0;

	/* 取出哈希对象 */
	if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.emptymultibulk)) == NULL
		|| checkType(c, o, REDIS_HASH)) return;

	if (flags & REDIS_HASH_KEY) multiplier++;
	if (flags & REDIS_HASH_VALUE) multiplier++;
	length = hashTypeLength(o) * multiplier;

	addReplyMultiBulkLen(c, length);
	/* 迭代节点,并取出元素 */
	hi = hashTypeInitIterator(o);
	while (hashTypeNext(hi) != REDIS_ERR) {
		/* 取出键 */
		if (flags & REDIS_HASH_KEY) {
			addHashIteratorCursorToReply(c, hi, REDIS_HASH_KEY);
			count++;
		}
		/* 取出值 */
		if (flags & REDIS_HASH_VALUE) {
			addHashIteratorCursorToReply(c, hi, REDIS_HASH_VALUE);
			count++;
		}
	}
	/* 释放迭代器 */
	hashTypeReleaseIterator(hi);
	assert(count == length);
}

void hgetallCommand(redisClient *c) {
	genericHgetallCommand(c, REDIS_HASH_KEY | REDIS_HASH_VALUE);
}

void hmgetCommand(redisClient *c) {
	robj *o;
	int i;
	/* 取出哈希对象 */
	o = lookupKeyRead(c->db, c->argv[1]);
	/* 对象存在，检查类型 */
	if (o != NULL && o->type != REDIS_HASH) {
		addReply(c, shared.wrongtypeerr);
		return;
	}
	/* 获取多个 field 的值 */
	addReplyMultiBulkLen(c, c->argc - 2);
	for (i = 2; i < c->argc; i++) {
		addHashFieldToReply(c, o, c->argv[i]);
	}
}

void hmsetCommand(redisClient *c) {
	int i;
	robj *o;

	/* field-value 参数必须成对出现 */
	if ((c->argc % 2) == 1) {
		addReplyError(c, "wrong number of arguments for HMSET");
		return;
	}

	/* 取出或新创建哈希对象 */
	if ((o = hashTypeLookupWriteOrCreate(c, c->argv[1])) == NULL) return;

	/* 如果需要的话，转换哈希对象的编码 */
	hashTypeTryConversion(o, c->argv, 2, c->argc - 1);

	/* 遍历并设置所有 field-value 对 */
	for (i = 2; i < c->argc; i += 2) {
		/* 编码 field-value 对，以节约空间 */
		hashTypeTryObjectEncoding(o, &c->argv[i], &c->argv[i + 1]);
		/* 设置 */
		hashTypeSet(o, c->argv[i], c->argv[i + 1]);
	}

	server.dirty++; /* 将服务器设为脏 */
	/* 向客户端发送回复 */
	addReply(c, shared.ok);
}

void hincrbyCommand(redisClient *c) {
	long long value, incr, oldvalue;
	robj *o, *current, *new;

	/* 取出 incr 参数的值，并创建对象 */
	if (getLongLongFromObjectOrReply(c, c->argv[3], &incr, NULL) != REDIS_OK) return;
	/* 取出或新创建哈希对象 */
	if ((o = hashTypeLookupWriteOrCreate(c, c->argv[1])) == NULL) return;
	/* 取出 field 的当前值 */
	if ((current = hashTypeGetObject(o, c->argv[2])) != NULL) {
		/* 取出值的整数表示 */
		if (getLongLongFromObjectOrReply(c, current, &value,
			"hash value is not an integer") != REDIS_OK) {
			decrRefCount(current);
			return;
		}
		decrRefCount(current);
	}
	else {
		/* 如果值当前不存在，那么默认为 0 */
		value = 0;
	}

	/* 检查计算是否会造成溢出 */
	oldvalue = value;
	if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN - oldvalue)) ||
		(incr > 0 && oldvalue > 0 && incr > (LLONG_MAX - oldvalue))) {
		addReplyError(c, "increment or decrement would overflow");
		return;
	}

	/* 计算结果 */
	value += incr;
	/* 为结果创建新的值对象 */
	new = createStringObjectFromLongLong(value);
	/* 编码值对象 */
	hashTypeTryObjectEncoding(o, &c->argv[2], NULL);
	/* 关联键和新的值对象，如果已经有对象存在，那么用新对象替换它 */
	hashTypeSet(o, c->argv[2], new);
	decrRefCount(new);

	/* 将计算结果用作回复 */
	addReplyLongLong(c, value);
}

void hlenCommand(redisClient *c) {
	robj *o;

	/* 取出哈希对象 */
	if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.czero)) == NULL ||
		checkType(c, o, REDIS_HASH)) return;

	/* 回复 */
	addReplyLongLong(c, hashTypeLength(o));
}

void hkeysCommand(redisClient *c) {
	genericHgetallCommand(c, REDIS_HASH_KEY);
}

void hvalsCommand(redisClient *c) {
	genericHgetallCommand(c, REDIS_HASH_VALUE);
}
