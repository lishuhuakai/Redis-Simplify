#include "intset.h"
#include "ziplist.h"
#include "dict.h"
#include "db.h"
#include "ziplist.h"
#include "t_string.h"
#include "networking.h"
#include "object.h"
#include "util.h"
#include "t_set.h"
#include <math.h>
/*
 * 命令的类型
 */
#define REDIS_OP_UNION 0
#define REDIS_OP_DIFF 1
#define REDIS_OP_INTER 2

/*============================ Variable and Function Declaration ======================== */
extern struct sharedObjectsStruct shared;
extern struct redisServer server;
extern struct dictType setDictType;

/*
 * 创建并返回一个多态集合迭代器
 *
 * setTypeIterator 定义在 redis.h
 */
setTypeIterator *setTypeInitIterator(robj *subject) {

	setTypeIterator *si = zmalloc(sizeof(setTypeIterator));

	/* 指向被迭代的对象 */
	si->subject = subject;

	// 记录对象的编码
	si->encoding = subject->encoding;

	/* HT */
	if (si->encoding == REDIS_ENCODING_HT) {
		/* 字典迭代器 */
		si->di = dictGetIterator(subject->ptr);
	}
	else if (si->encoding == REDIS_ENCODING_INTSET) { /* INTSET */
		/* 索引 */
		si->ii = 0;
	}
	else {
		assert(0);
	}

	/* 返回迭代器 */
	return si;
}

/* 
 * setTypeNext 的非 copy-on-write 友好版本，
 * 总是返回一个新的、或者已经增加过引用计数的对象。
 *
 * 调用者在使用完对象之后，应该对对象调用 decrRefCount() 。
 *
 * 这个函数应该在非 copy-on-write 时调用。
 */
robj *setTypeNextObject(setTypeIterator *si) {
	int64_t intele;
	robj *objele;
	int encoding;

	/* 取出元素 */
	encoding = setTypeNext(si, &objele, &intele);
	/* 总是为元素创建对象 */
	switch (encoding) {
	/* 已为空 */
	case -1:    return NULL;
	/* INTSET 返回一个整数值，需要为这个值创建对象 */
	case REDIS_ENCODING_INTSET:
		return createStringObjectFromLongLong(intele);
	/* HT 本身已经返回对象了，只需执行 incrRefCount() */
	case REDIS_ENCODING_HT:
		incrRefCount(objele);
		return objele;
	default:
		assert(0);
	}

	return NULL; /* just to suppress warnings */
}



/* 
 * 从非空集合中随机取出一个元素。
 *
 * 如果集合的编码为 intset ，那么将元素指向 int64_t 指针 llele 。
 * 如果集合的编码为 HT ，那么将元素对象指向对象指针 objele 。
 *
 * 函数的返回值为集合的编码方式，通过这个返回值可以知道那个指针保存了元素的值。
 *
 * 因为被返回的对象是没有被增加引用计数的，
 * 所以这个函数是对 copy-on-write 友好的。
 */
int setTypeRandomElement(robj *setobj, robj **objele, int64_t*llele) {
	if (setobj->encoding == REDIS_ENCODING_HT) {
		dictEntry *de = dictGetRandomKey(setobj->ptr);
		*objele = dictGetKey(de);
	}
	else if (setobj->encoding == REDIS_ENCODING_INTSET) {
		*llele = intsetRandom(setobj->ptr);
	}
	else
		assert(0);
	return setobj->encoding;
}

/*
 * 释放迭代器
 */
void setTypeReleaseIterator(setTypeIterator *si) {

	if (si->encoding == REDIS_ENCODING_HT)
		dictReleaseIterator(si->di);

	zfree(si);
}

/* 
 * 取出被迭代器指向的当前集合元素。
 *
 * 因为集合即可以编码为 intset ，也可以编码为哈希表，
 * 所以程序会根据集合的编码，选择将值保存到那个参数里：
 *
 *  - 当编码为 intset 时，元素被指向到 llobj 参数
 *
 *  - 当编码为哈希表时，元素被指向到 eobj 参数
 *
 * 并且函数会返回被迭代集合的编码，方便识别。
 *
 * 当集合中的元素全部被迭代完毕时，函数返回 -1 。
 *
 * 因为被返回的对象是没有被增加引用计数的，
 * 所以这个函数是对 copy-on-write 友好的。
 */
int setTypeNext(setTypeIterator *si, robj **objele, int64_t *llele) {

	/* 从字典中取出对象 */
	if (si->encoding == REDIS_ENCODING_HT) {

		/* 更新迭代器 */
		dictEntry *de = dictNext(si->di);

		/* 字典已迭代完 */
		if (de == NULL) return -1;

		/* 返回节点的键（集合的元素） */
		*objele = dictGetKey(de);
	}
	else if (si->encoding == REDIS_ENCODING_INTSET) {
		if (!intsetGet(si->subject->ptr, si->ii++, llele))
			return -1;
	}
	/* 返回编码 */
	return si->encoding;
}


/*
 * 多态 remove 操作
 *
 * 删除成功返回 1 ，因为元素不存在而导致删除失败返回 0 。
 */
int setTypeRemove(robj *setobj, robj *value) {
	long long llval;
	if (setobj->encoding == REDIS_ENCODING_HT) {
		/* 从字典中删除键 */
		if (dictDelete(setobj->ptr, value) == DICT_OK) {
			/* 字典暂时就不用缩减大小了. */
			return 1;
		}
	}
	else if (setobj->encoding == REDIS_ENCODING_INTSET) {
		/* 如果对象的值可以编码成整数的话,那么尝试从intset中移除元素 */
		if (isObjectRepresentableAsLongLong(value, &llval) == REDIS_OK) {
			int success;
			setobj->ptr = intsetRemove(setobj->ptr, llval, &success);
			if (success) return 1;
		}

	}
	else
		assert(0);
	return 0;
}

/*
 * 集合多态size函数
 */
unsigned long setTypeSize(robj *subject) {
	if (subject->encoding == REDIS_ENCODING_HT) {
		return dictSize((dict*)subject->ptr);
	}
	else if (subject->encoding == REDIS_ENCODING_INTSET) {
		return intsetLen((intset *)subject->ptr);
	}
	else {
		assert(0);
	}
}

/*
 * 创建一个 SET 编码的集合对象
 */
robj *createSetObject(void) {
	dict *d = dictCreate(&setDictType, NULL);
	robj *o = createObject(REDIS_SET, d);
	o->encoding = REDIS_ENCODING_HT;
	return o;
}

/* 
 *
 * 返回一个可以保存值 value 的集合。
 *
 * 当对象的值可以被编码为整数时，返回 intset ，
 * 否则，返回普通的哈希表。
 */

robj *setTypeCreate(robj *value) {
	if (isObjectRepresentableAsLongLong(value, NULL) == REDIS_OK)
		return createIntsetObject();
	return createSetObject();
}


/*
 * 将集合对象 setobj 的编码转换为 REDIS_ENCODING_HT 。
 *
 * 新创建的结果字典会被预先分配为和原来的集合一样大。
 */

void setTypeConvert(robj *setobj, int enc) {

	setTypeIterator *si;

	/* 确认类型和编码正确 */
	assert(setobj->type == REDIS_SET && setobj->encoding == REDIS_ENCODING_INTSET);

	if (enc == REDIS_ENCODING_HT) {
		int64_t intele;
		/* 创建新字典 */
		dict *d = dictCreate(&setDictType, NULL);
		robj *element;

		/* 预先扩展空间 */
		dictExpand(d, intsetLen(setobj->ptr));

		/* 遍历集合，并将元素添加到字典中 */
		si = setTypeInitIterator(setobj);
		while (setTypeNext(si, NULL, &intele) != -1) {
			element = createStringObjectFromLongLong(intele);
			assert(dictAdd(d, element, NULL) == DICT_OK);
		}
		setTypeReleaseIterator(si);

		/* 更新集合的编码 */
		setobj->encoding = REDIS_ENCODING_HT;
		zfree(setobj->ptr);
		/* 更新集合的值对象 */
		setobj->ptr = d;
	}
	else {
		assert(0);
	}
}

/*
 * 多态 add 操作
 *
 * 添加成功返回 1 ，如果元素已经存在，返回 0 。
 */
int setTypeAdd(robj *subject, robj *value) {
	long long llval;

	/* 字典 */
	if (subject->encoding == REDIS_ENCODING_HT) {
		/* 将value作为键,NULL作为值,将元素添加到字典中 */
		if (dictAdd(subject->ptr, value, NULL) == DICT_OK) {
			incrRefCount(value);
			return 1;
		}
	}
	else if (subject->encoding == REDIS_ENCODING_INTSET) {
		/* 如果对象的值可以编码为整数的话，那么将对象的值添加到 intset 中 */
		if (isObjectRepresentableAsLongLong(value, &llval) == REDIS_OK) {
			uint8_t success = 0;
			subject->ptr = intsetAdd(subject->ptr, llval, &success);
			if (success) {
				/* 添加成功
				 * 检查集合在添加新元素之后是否需要转换为字典 */
				if (intsetLen(subject->ptr) > server.set_max_intset_entries)
					setTypeConvert(subject, REDIS_ENCODING_HT);
				return 1;
			}

		}
		else {
			/* 如果对象的值不能编码为整数，那么将集合从 intset 编码转换为 HT 编码
			* 然后再执行添加操作 */
			setTypeConvert(subject, REDIS_ENCODING_HT);
			dictAdd(subject->ptr, value, NULL);
			incrRefCount(value);
			return 1;
		}
	}
	else
		assert(0);
	return 0;
}

/*
 * 多态ismember操作
 */
int setTypeIsMember(robj *subject, robj *value) {
	long long llval;
	if (subject->encoding == REDIS_ENCODING_HT) {
		return dictFind((dict *)subject->ptr, value) != NULL;
	}
	else if (subject->encoding == REDIS_ENCODING_INTSET) {
		if (isObjectRepresentableAsLongLong(value, &llval) == REDIS_OK) {
			return intsetFind((intset*)subject->ptr, llval);
		}
	}
	else
		assert(NULL);
}


void saddCommand(redisClient *c) {
	robj *set;
	int j, added = 0;
	/* 取出集合对象 */
	set = lookupKeyWrite(c->db, c->argv[1]);
	/* 对象不存在,创建一个新的,并将它关联到数据库 */
	if (set == NULL) {
		set = setTypeCreate(c->argv[2]);
		dbAdd(c->db, c->argv[1], set);
	}
	else { /* 对象存在,检查类型 */
		if (set->type != REDIS_SET) {
			addReply(c, shared.wrongtypeerr);
			return;
		}
	}

	/* 将所有的输入元素添加到集合中 */
	for (j = 2; j < c->argc; j++) {
		/* 只有元素未存在于集合时,才算一次成功添加 */
		if (setTypeAdd(set, c->argv[j])) added++;
	}

	/* 如果有至少一个元素被成功添加,那么执行以下程序 */
	if (added) {
		// todo
	}

	server.dirty += added; /* 将数据库设为脏 */
	addReplyLongLong(c, added);
}

/*
 * 计算集合 s1 的基数和集合 s2 的基数之差
 */
int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
	return setTypeSize(*(robj**)s1) - setTypeSize(*(robj**)s2);
}

void sinterGenericCommand(redisClient *c, robj **setkeys, unsigned long setnum, robj *dstkey) {
	/* 这里的dstkey用于存储最终的结果,如果它不为NULL的话 */
	robj **sets = zmalloc(sizeof(robj *) * setnum); /* 集合数组 */
	setTypeIterator *si; /* 迭代器 */
	robj *eleobj, *dstset = NULL;
	int64_t intobj;
	void *replylen = NULL;
	unsigned long j, cardinality = 0;
	int encoding;

	for (j = 0; j < setnum; j++) {
		/*
		* 取出对象,第一次执行时,取出的是dest集合
		* 之后执行时,取出的都是source集合
		*/
		robj *setobj = dstkey ?
			lookupKeyWrite(c->db, setkeys[j]) :
			lookupKeyRead(c->db, setkeys[j]);
		/* 对象不存在,放弃执行,进行清理 */
		if (!setobj) {
			zfree(sets);
			if (dstkey) {
				if (dbDelete(c->db, dstkey)) {
					// todo
				}
				addReply(c, shared.czero);
			}
			else {
				addReply(c, shared.emptymultibulk);
			}
			return;
		}
		/* 检查对象的类型 */
		if (checkType(c, setobj, REDIS_SET)) {
			zfree(sets);
			return;
		}
		/* 将数组指针指向集合对象 */
		sets[j] = setobj;
	}
	/* 按基数对集合进行排序,按照set的元素个数从小到大排序，这样提升算法的效率 */
	qsort(sets, setnum, sizeof(robj*), qsortCompareSetsByCardinality);
	/* 因为不知道结果集合有多少个元素，所有没有办法直接设置回复的数量
	 * 这里使用了一个小技巧，直接使用一个 BUFF 列表，
	 * 然后将之后的回复都添加到列表中 */
	if (!dstkey) { /* 话说,dstkey是干什么的? */
		replylen = addDeferredMultiBulkLength(c); /* 返回的实际上是一个链表的节点 */
	}
	else {
		/* If we have a target key where to store the resulting set
		* create this key with an empty set inside */
		dstset = createIntsetObject();
	}
	/* 遍历基数最小的第一个集合(提高效率)
	 * 并将它的元素和所有其他集合进行对比
	 * 如果有至少一个集合不包含这个元素，那么这个元素不属于交集 */
	si = setTypeInitIterator(sets[0]);
	while ((encoding = setTypeNext(si, &eleobj, &intobj)) != -1) {
		/* 遍历其他集合，检查元素是否在这些集合中存在 */
		for (j = 1; j < setnum; j++) {
			/* 跳过第一个集合，因为它是结果集的起始值 */
			if (sets[j] == sets[0]) continue;

			/* 元素的编码为 INTSET 
			 * 在其他集合中查找这个对象是否存在 */
			if (encoding == REDIS_ENCODING_INTSET) {
				/* intset with intset is simple... and fast */
				if (sets[j]->encoding == REDIS_ENCODING_INTSET &&
					!intsetFind((intset*)sets[j]->ptr, intobj))
				{ /* 没有找到,直接丢弃这个元素 */
					break;
					/* in order to compare an integer with an object we
					* have to use the generic function, creating an object
					* for this */
				}
				else if (sets[j]->encoding == REDIS_ENCODING_HT) {
					eleobj = createStringObjectFromLongLong(intobj);
					if (!setTypeIsMember(sets[j], eleobj)) {
						decrRefCount(eleobj);
						break;
					}
					decrRefCount(eleobj);
				}
				
			}
			else if (encoding == REDIS_ENCODING_HT) {/* 元素的编码为 字典, 需要判断在其他集合中这个对象是否存在 */
				/* Optimization... if the source object is integer
				* encoded AND the target set is an intset, we can get
				* a much faster path. */
				if (eleobj->encoding == REDIS_ENCODING_INT &&
					sets[j]->encoding == REDIS_ENCODING_INTSET &&
					!intsetFind((intset*)sets[j]->ptr, (long)eleobj->ptr))
				{
					break;
					/* else... object to object check is easy as we use the
					* type agnostic API here. */
				}
				else if (!setTypeIsMember(sets[j], eleobj)) {
					break;
				}
			}
		}
		/* 如果所有集合都带有目标元素的话，那么执行以下代码 */
		if (j == setnum) {
			/* SINTER 命令，直接返回结果集元素 */
			if (!dstkey) {
				
				if (encoding == REDIS_ENCODING_HT) {
					char *str1 = eleobj->ptr;
					addReplyBulk(c, eleobj);
				}
				else
					addReplyBulkLongLong(c, intobj);
				cardinality++;
			}
			else { /* SINTERSTORE 命令，将结果添加到结果集中 */
				if (encoding == REDIS_ENCODING_INTSET) {
					eleobj = createStringObjectFromLongLong(intobj);
					setTypeAdd(dstset, eleobj);
					decrRefCount(eleobj); /* 增加引用 */
				}
				else {
					setTypeAdd(dstset, eleobj);
				}
			}
		}
	}
	setTypeReleaseIterator(si); /* 释放资源 */

	/* SINTERSTORE 命令，将结果集关联到数据库 */
	if (dstkey) {
		/* 将结果存到数据库中,如果结果不为空的话. */
		
		int deleted = dbDelete(c->db, dstkey); /* 删除现在可能有的 dstkey */

		/* 如果结果集非空，那么将它关联到数据库中 */
		if (setTypeSize(dstset) > 0) {
			dbAdd(c->db, dstkey, dstset);
			addReplyLongLong(c, setTypeSize(dstset));
		}
		else {
			decrRefCount(dstset);
			addReply(c, shared.czero);
		}
	}
	else { /* SINTER 命令，回复结果集的基数 */
		setDeferredMultiBulkLength(c, replylen, cardinality);
	}
	zfree(sets);
}

void sinterCommand(redisClient *c) { /* 这个命令貌似是取出集合的所有元素 */
	sinterGenericCommand(c, c->argv + 1, c->argc - 1, NULL);
}

void sinterstoreCommand(redisClient *c) {
	sinterGenericCommand(c, c->argv + 2, c->argc - 2, c->argv[1]);
}

void sismemberCommand(redisClient *c) {
	robj *set;

	/* 取出集合对象 */
	if ((set = lookupKeyReadOrReply(c, c->argv[1], shared.czero)) == NULL ||
		checkType(c, set, REDIS_SET)) return;

	/* 编码输入元素 */
	c->argv[2] = tryObjectEncoding(c->argv[2]);

	/* 检查是否存在 */
	if (setTypeIsMember(set, c->argv[2]))
		addReply(c, shared.cone);
	else
		addReply(c, shared.czero);
}

void scardCommand(redisClient *c) { /* 要获取集合元素的个数 */
	robj *o;

	/* 取出集合 */
	if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.czero)) == NULL ||
		checkType(c, o, REDIS_SET)) return;
	/* 返回集合的基数 */
	addReplyLongLong(c, setTypeSize(o));
}

void spopCommand(redisClient *c) {
	robj *set, *ele;
	int64_t llele;
	int encoding;

	/* 取出集合 */
	if ((set = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk)) == NULL ||
		checkType(c, set, REDIS_SET)) return;

	/* 从集合中随机取出一个元素 */
	encoding = setTypeRandomElement(set, &ele, &llele);

	if (encoding == REDIS_ENCODING_INTSET) {
		ele = createStringObjectFromLongLong(llele);
		set->ptr = intsetRemove(set->ptr, llele, NULL);
	}
	else {
		char *str1 = ele->ptr;
		incrRefCount(ele);
		setTypeRemove(set, ele);
	}
	decrRefCount(ele);

	/* 返回回复 */
	addReplyBulk(c, ele);

	/* 如果集合已经为空,那么从数据库中删除它 */
	if (setTypeSize(set) == 0) {
		dbDelete(c->db, c->argv[1]);
	}

	server.dirty++; /* 将数据库设为脏 */
}

void smoveCommand(redisClient *c) {
	robj *srcset, *dstset, *ele;
	/* 取出源集合 */
	srcset = lookupKeyWrite(c->db, c->argv[1]);
	/* 取出目标集合 */
	dstset = lookupKeyWrite(c->db, c->argv[2]);
	/* 编码元素 */
	ele = c->argv[3] = tryObjectEncoding(c->argv[3]);
	/* 如果源集合不存在,直接返回 */
	if (srcset == NULL) {
		addReply(c, shared.czero);
		return;
	}
	/*
	* 如果源集合的类型错误,或者目标集合存在,但是类型错误,那么直接返回
	*/
	if (checkType(c, srcset, REDIS_SET) ||
		(dstset && checkType(c, dstset, REDIS_SET))) return;

	/*
	* 如果源集合和目标集合相等,那么直接返回
	*/
	if (srcset == dstset) {
		addReply(c, shared.cone);
		return;
	}

	/*
	* 从源集合中移除目标元素
	* 如果目标元素不存在于集合中,那么直接返回
	*/
	if (!setTypeRemove(srcset, ele)) {
		addReply(c, shared.czero);
		return;
	}

	/*
	* 如果源集合已经为空,那么将它从数据库中删除
	*/
	if (setTypeSize(srcset) == 0) {
		dbDelete(c->db, c->argv[1]);
	}

	server.dirty++; /* 将数据库设为脏 */

	/*
	* 如果目标集合不存在,那么创建它
	*/
	if (!dstset) {
		dstset = setTypeCreate(ele);
		dbAdd(c->db, c->argv[2], dstset);
	}

	/*
	* 将元素添加到目标集合
	*/
	if (setTypeAdd(dstset, ele)) {
		server.dirty++;
		// todo
	}
	addReply(c, shared.cone);

}

/*
 * 计算集合 s2 的基数减去集合 s1 的基数之差
 */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
	robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;

	return  (o2 ? setTypeSize(o2) : 0) - (o1 ? setTypeSize(o1) : 0);
}

void sunionDiffGenericCommand(redisClient *c, robj **setkeys, int setnum, robj *dstkey, int op) {
	/* 集合数组 */
	robj **sets = zmalloc(sizeof(robj*)*setnum);

	setTypeIterator *si;
	robj *ele, *dstset = NULL;
	int j, cardinality = 0;
	int diff_algo = 1;

	/* 取出所有集合对象，并添加到集合数组中 */
	for (j = 0; j < setnum; j++) {
		robj *setobj = dstkey ?
			lookupKeyWrite(c->db, setkeys[j]) :
			lookupKeyRead(c->db, setkeys[j]);

		/* 不存在的集合当作 NULL 来处理 */
		if (!setobj) {
			sets[j] = NULL;
			continue;
		}

		/* 有对象不是集合，停止执行，进行清理 */
		if (checkType(c, setobj, REDIS_SET)) {
			zfree(sets);
			return;
		}
		/* 记录对象 */
		sets[j] = setobj;
	}

	/*
	* 选择使用那个算法来执行计算
	*
	* 算法 1 的复杂度为 O(N*M) ，其中 N 为第一个集合的基数，
	* 而 M 则为其他集合的数量。
	*
	* 算法 2 的复杂度为 O(N) ，其中 N 为所有集合中的元素数量总数。
	*
	* 程序通过考察输入来决定使用那个算法
	*/
	if (op == REDIS_OP_DIFF && sets[0]) {
		long long algo_one_work = 0, algo_two_work = 0;

		/* 遍历所有集合 */
		for (j = 0; j < setnum; j++) {
			if (sets[j] == NULL) continue;

			/* 计算setnum乘以sets[0]的基数之积 */
			algo_one_work += setTypeSize(sets[0]);
			/* 计算所有集合的基数之和 */
			algo_two_work += setTypeSize(sets[j]);
		}
		/* 算法1的常数比较低,优先考虑算法1 */
		algo_one_work /= 2;
		diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

		if (diff_algo == 1 && setnum > 1) {
			/* 如果使用的是算法 1 ，那么最好对 sets[0] 以外的其他集合进行排序
			 * 这样有助于优化算法的性能 */
			qsort(sets + 1, setnum - 1, sizeof(robj *),
				qsortCompareSetsByRevCardinality);
		}
	}
	/*
	* 使用一个临时集合来保存结果集，如果程序执行的是 SUNIONSTORE 命令，
	* 那么这个结果将会成为将来的集合值对象。
	*/
	dstset = createIntsetObject();

	/* 执行的是并集计算 */
	if (op == REDIS_OP_UNION) {
		/* 遍历所有元素,将元素添加到结果集里面就可以了 */
		for (j = 0; j < setnum; j++) {
			if (!sets[j]) continue;
			si = setTypeInitIterator(sets[j]);
			while ((ele = setTypeNextObject(si)) != NULL) {
				/*setTypeAdd 只在集合不存在时，才会将元素添加到集合，并返回 1 */
				if (setTypeAdd(dstset, ele)) cardinality++;
				decrRefCount(ele);
			}
			setTypeReleaseIterator(si);
		}
	}
	else if (op == REDIS_OP_DIFF && sets[0] && diff_algo == 1) {
		/*
		* 执行的是差集计算,并且使用了算法1
		* 差集算法 1 ：
		*
		* 程序遍历 sets[0] 集合中的所有元素，
		* 并将这个元素和其他集合的所有元素进行对比，
		* 只有这个元素不存在于其他所有集合时，
		* 才将这个元素添加到结果集。
		*
		* 这个算法执行最多 N*M 步， N 是第一个集合的基数，
		* 而 M 是其他集合的数量。
		*/
		si = setTypeInitIterator(sets[0]);
		while ((ele = setTypeNextObject(si)) != NULL) {
			/* 检查元素在其他集合是否存在 */
			for (j = 1; j < setnum; j++) {
				if (!sets[j]) continue; /* no key is an empty set. */
				if (sets[j] == sets[0]) break; /* same set! */
				if (setTypeIsMember(sets[j], ele)) break;
			}
			/* 只有元素在所有其他集合中都不存在时，才将它添加到结果集中 */
			if (j == setnum) {
				/* There is no other set with this element. Add it. */
				setTypeAdd(dstset, ele);
				cardinality++;
			}

			decrRefCount(ele);
		}
		setTypeReleaseIterator(si);
	}
	else if (op == REDIS_OP_DIFF && sets[0] && diff_algo == 2) {
		/*
		* 执行的是差集计算，并且使用算法 2
		* 差集算法 2 ：
		*
		* 将 sets[0] 的所有元素都添加到结果集中，
		* 然后遍历其他所有集合，将相同的元素从结果集中删除。
		*
		* 算法复杂度为 O(N) ，N 为所有集合的基数之和。
		*/
		for (j = 0; j < setnum; j++) {
			if (!sets[j]) continue; /* non existing keys are like empty sets */

			si = setTypeInitIterator(sets[j]);
			while ((ele = setTypeNextObject(si)) != NULL) {
				/* sets[0] 时，将所有元素添加到集合 */
				if (j == 0) {
					if (setTypeAdd(dstset, ele)) cardinality++;
				}
				else { /* 不是 sets[0] 时，将所有集合从结果集中移除 */
					if (setTypeRemove(dstset, ele)) cardinality--;
				}
				decrRefCount(ele);
			}
			setTypeReleaseIterator(si);
			if (cardinality == 0) break;
		}
	}
	/* 执行的是 SDIFF 或者 SUNION 
	 * 打印结果集中的所有元素 */
	if (!dstkey) {
		addReplyMultiBulkLen(c, cardinality);
		/* 遍历并回复结果集中的元素 */
		si = setTypeInitIterator(dstset);
		while ((ele = setTypeNextObject(si)) != NULL) {
			addReplyBulk(c, ele);
			decrRefCount(ele);
		}
		setTypeReleaseIterator(si);
		decrRefCount(dstset);
	}
	else { /* 执行的是 SDIFFSTORE 或者 SUNIONSTORE */
		/* If we have a target key where to store the resulting set
		* create this key with the result set inside */
		/* 现删除现在可能有的 dstkey */
		int deleted = dbDelete(c->db, dstkey);

		/* 如果结果集不为空，将它关联到数据库中 */
		if (setTypeSize(dstset) > 0) {
			dbAdd(c->db, dstkey, dstset);
			/* 返回结果集的基数 */
			addReplyLongLong(c, setTypeSize(dstset));
		}
		else { /* 结果集为空 */
			decrRefCount(dstset);
			/* 返回 0 */ 
			addReply(c, shared.czero);
		}
		server.dirty++;
	}
	zfree(sets);
}

void sunionCommand(redisClient *c) {  /* 求集合的交集 */
	sunionDiffGenericCommand(c, c->argv + 1, c->argc - 1, NULL, REDIS_OP_UNION);
}

void sunionstoreCommand(redisClient *c) {
	sunionDiffGenericCommand(c, c->argv + 2, c->argc - 2, c->argv[1], REDIS_OP_UNION);
}

void sdiffCommand(redisClient *c) {
	sunionDiffGenericCommand(c, c->argv + 1, c->argc - 1, NULL, REDIS_OP_DIFF);
}

void sdiffstoreCommand(redisClient *c) {
	sunionDiffGenericCommand(c, c->argv + 2, c->argc - 2, c->argv[1], REDIS_OP_DIFF);
}