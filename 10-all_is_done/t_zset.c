#include "redis.h"
#include "ziplist.h"
#include "intset.h"
#include <math.h>
#include <assert.h>
#include "dict.h"
#include "db.h"
#include "ziplist.h"
#include "t_string.h"
#include "networking.h"
#include "object.h"
#include "util.h"
#include "t_zset.h"
#include <math.h>

/*============================ Variable and Function Declaration ======================== */
extern struct sharedObjectsStruct shared;
extern struct redisServer server;
extern struct dictType zsetDictType;

zskiplist *zslCreate(void);
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, robj *ele, double score);
zskiplistNode *zslCreateNode(int level, double score, robj *obj);
double zzlGetScore(unsigned char *sptr);
int compareStringObjects(robj *a, robj *b);

static int zslValueGteMin(double value, zrangespec *spec);
static int zslValueLteMax(double value, zrangespec *spec);

/* 
 * 将 eptr 中的元素和 cstr 进行对比。
 *
 * 相等返回 0 ，
 * 不相等并且 eptr 的字符串比 cstr 大时，返回正整数。
 * 不相等并且 eptr 的字符串比 cstr 小时，返回负整数。
 */
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen) {
	unsigned char *vstr;
	unsigned int vlen;
	long long vlong;
	unsigned char vbuf[32];
	int minlen, cmp;

	/* 取出节点中的字符串值，以及它的长度 */
	assert(ziplistGet(eptr, &vstr, &vlen, &vlong));
	if (vstr == NULL) {
		/* Store string representation of long long in buf. */
		vlen = ll2string((char*)vbuf, sizeof(vbuf), vlong);
		vstr = vbuf;
	}

	/* 对比 */
	minlen = (vlen < clen) ? vlen : clen;
	cmp = memcmp(vstr, cstr, minlen);
	if (cmp == 0) return vlen - clen;
	return cmp;
}

/* 
 * 从跳跃表 zsl 中删除包含给定节点 score 并且带有指定对象 obj 的节点。
 *
 * T_wrost = O(N^2), T_avg = O(N log N)
 */
int zslDelete(zskiplist *zsl, double score, robj *obj) {
	zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
	int i;

	/* 遍历跳跃表，查找目标节点，并记录所有沿途节点 */
	x = zsl->header;
	for (i = zsl->level - 1; i >= 0; i--) {

		/* 遍历跳跃表的复杂度为 T_wrost = O(N), T_avg = O(log N) */
		while (x->level[i].forward &&
			(x->level[i].forward->score < score ||
				/* 比对分值 */
			(x->level[i].forward->score == score &&
				/* 比对对象，T = O(N) */
				compareStringObjects(x->level[i].forward->obj, obj) < 0)))

			/* 沿着前进指针移动 */
			x = x->level[i].forward;

		/* 记录沿途节点 */
		update[i] = x;
	}

	/*
	* 检查找到的元素 x ，只有在它的分值和对象都相同时，才将它删除。
	*/
	x = x->level[0].forward;
	if (x && score == x->score && equalStringObjects(x->obj, obj)) {
		zslDeleteNode(zsl, x, update);
		zslFreeNode(x);
		return 1;
	}
	else {
		return 0; /* not found */
	}
	return 0; /* not found */
}



/* 
 * 内部删除函数，
 * 被 zslDelete 、 zslDeleteRangeByScore 和 zslDeleteByRank 等函数调用。
 *
 * T = O(1)
 */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
	int i;
	/* 更新所有和被删除节点 x 有关的节点的指针，解除它们之间的关系 */
	for (i = 0; i < zsl->level; i++) {
		if (update[i]->level[i].forward == x) {
			update[i]->level[i].span += x->level[i].span - 1;
			update[i]->level[i].forward = x->level[i].forward;
		}
		else {
			update[i]->level[i].span -= 1;
		}
	}

	/* 更新被删除节点 x 的前进和后退指针 */
	if (x->level[0].forward) {
		x->level[0].forward->backward = x->backward;
	}
	else {
		zsl->tail = x->backward;
	}

	/* 更新跳跃表最大层数（只在被删除节点是跳跃表中最高的节点时才执行） */
	while (zsl->level > 1 && zsl->header->level[zsl->level - 1].forward == NULL)
		zsl->level--;

	/* 跳跃表节点计数器减一 */
	zsl->length--;
}


/*
 * 释放给定的跳跃表节点
 *
 * T = O(1)
 */
void zslFreeNode(zskiplistNode *node) {

	decrRefCount(node->obj);

	zfree(node);
}

/* 
 * 根据 eptr 和 sptr ，移动它们分别指向下个成员和下个分值。
 *
 * 如果后面已经没有元素，那么两个指针都被设为 NULL 。
 */
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
	unsigned char *next_eptr, *next_sptr;
	assert(*eptr != NULL && *sptr != NULL);

	/* 指向下个成员 */
	next_eptr = ziplistNext(zl, *sptr);
	if (next_eptr != NULL) {
		/* 指向下个分值 */
		next_sptr = ziplistNext(zl, next_eptr);
		assert(next_sptr != NULL);
	}
	else {
		/* No next entry. */
		next_sptr = NULL;
	}

	*eptr = next_eptr;
	*sptr = next_sptr;
}


/* 
 * 返回一个随机值，用作新跳跃表节点的层数。
 *
 * 返回值介乎 1 和 ZSKIPLIST_MAXLEVEL 之间（包含 ZSKIPLIST_MAXLEVEL），
 * 根据随机算法所使用的幂次定律，越大的值生成的几率越小。
 *
 * T = O(N)
 */
int zslRandomLevel(void) {
	int level = 1;
	while ((random() & 0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
		level += 1;
	return (level<ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}


/*
 * 创建一个成员为 obj ，分值为 score 的新节点，
 * 并将这个新节点插入到跳跃表 zsl 中。
 *
 * 函数的返回值为新节点。
 *
 * T_wrost = O(N^2), T_avg = O(N log N)
 */
zskiplistNode *zslInsert(zskiplist *zsl, double score, robj *obj) {
	zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
	unsigned int rank[ZSKIPLIST_MAXLEVEL];
	int i, level;
	assert(!isnan(score));

	/* 在各个层查找节点的插入位置 */
	x = zsl->header; /* 指向表头 */
	for (i = zsl->level - 1; i >= 0; i--) {
		/* 如果 i 不是 zsl->level-1 层
		 * 那么 i 层的起始 rank 值为 i+1 层的 rank 值
		 * 各个层的 rank 值一层层累积
		 * 最终 rank[0] 的值加一就是新节点的前置节点的排位
		 * rank[0] 会在后面成为计算 span 值和 rank 值的基础 */
		rank[i] = i == (zsl->level - 1) ? 0 : rank[i + 1];
		/* 沿着前进指针遍历跳跃表 */
		while (x->level[i].forward &&
			(x->level[i].forward->score < score ||
				/* 对比分值 */
			(x->level[i].forward->score == score &&
				compareStringObjects(x->level[i].forward->obj, obj) < 0))) {
			/* 记录沿途跨越了多少个节点 */
			rank[i] += x->level[i].span;
			/* 移动至下一指针 */
			x = x->level[i].forward;
		}
		/* 记录将要和新节点相连接的节点 */
		update[i] = x;
	}
	/* 
	* zslInsert() 的调用者会确保同分值且同成员的元素不会出现，
	* 所以这里不需要进一步进行检查，可以直接创建新元素。
	*/
	/* 获取一个随机值作为新节点的层数 */
	level = zslRandomLevel();

	/* 如果新节点的层数比表中其他节点的层数都要大
	* 那么初始化表头节点中未使用的层，并将它们记录到 update 数组中
	* 将来也指向新节点 */
	if (level > zsl->level) {
		/* 初始化未使用层 */
		for (i = zsl->level; i < level; i++) {
			rank[i] = 0;
			update[i] = zsl->header;
			update[i]->level[i].span = zsl->length;
		}

		/* 更新表中节点最大层数 */
		zsl->level = level;
	}

	/* 创建新节点 */
	x = zslCreateNode(level, score, obj);

	/* 将前面记录的指针指向新节点，并做相应的设置 */
	for (i = 0; i < level; i++) {

		/* 设置新节点的 forward 指针 */
		x->level[i].forward = update[i]->level[i].forward;

		/* 将沿途记录的各个节点的 forward 指针指向新节点 */
		update[i]->level[i].forward = x;

		/* 计算新节点跨越的节点数量 */
		x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);

		/* 更新新节点插入之后，沿途节点的 span 值
		 * 其中的 +1 计算的是新节点 */
		update[i]->level[i].span = (rank[0] - rank[i]) + 1;
	}

	/* 未接触的节点的 span 值也需要增一，这些节点直接从表头指向新节点 */
	for (i = level; i < zsl->level; i++) {
		update[i]->level[i].span++;
	}

	/* 设置新节点的后退指针 */
	x->backward = (update[0] == zsl->header) ? NULL : update[0];
	if (x->level[0].forward)
		x->level[0].forward->backward = x;
	else
		zsl->tail = x;

	/* 跳跃表的节点计数增一 */
	zsl->length++;
	return x;
}


/*
 * 创建一个层数为 level 的跳跃表节点，
 * 并将节点的成员对象设置为 obj ，分值设置为 score 。
 *
 * 返回值为新创建的跳跃表节点
 *
 * T = O(1)
 */
zskiplistNode *zslCreateNode(int level, double score, robj *obj) {
	/* 分配空间 */
	zskiplistNode *zn = zmalloc(sizeof(*zn) + level * sizeof(struct zskiplistLevel));

	/* 设置属性 */
	zn->score = score;
	zn->obj = obj;
	return zn;
}

/*
 * 返回跳跃表包含的元素的数量
 */
unsigned int zzlLength(unsigned char *zl) {
	return ziplistLen(zl) / 2;
}

/*
 * 将跳跃表对象zobj的底层编码转换为encoding
 */
void zsetConvert(robj *zobj, int encoding) {
	zset *zs;
	zskiplistNode *node, *next;
	robj *ele;
	double score;

	if (zobj->encoding == encoding) return;
	
	if (zobj->encoding == REDIS_ENCODING_ZIPLIST) { /* 从ZIPLIST编码转换为SKIPLIST编码 */
		unsigned char *zl = zobj->ptr;
		unsigned char *eptr, *sptr;
		unsigned char *vstr;
		unsigned int vlen;
		long long vlong;

		if (encoding != REDIS_ENCODING_SKIPLIST) assert(0);

		/* 创建有序集合结构 */
		zs = zmalloc(sizeof(*zs)); /* 有序集合 */
		zs->dict = dictCreate(&zsetDictType, NULL); /* 字典 */
		zs->zsl = zslCreate(); /* 跳跃表 */

		// 有序集合在 ziplist 中的排列：
		//
		// | member-1 | score-1 | member-2 | score-2 | ... |
		//

		/* 指向ziplist中首个节点(保存该元素) */
		eptr = ziplistIndex(zl, 0);
		assert(eptr != NULL);
		/* 指向ziplist中的第二个节点(保存该元素的分值) */
		sptr = ziplistNext(zl, eptr);
		assert(sptr != NULL);

		/* 遍历所有的ziplist节点,并将元素的成员和分值添加到有序集合中 */
		while (eptr != NULL) {
			/* 取出分值 */
			score = zzlGetScore(sptr);
			/* 取出成员 */
			ziplistGet(eptr, &vstr, &vlen, &vlong);
			if (vstr == NULL) /* 存储的是整数值 */
				ele = createStringObjectFromLongLong(vlong);
			else /* 存储的是string类型 */
				ele = createStringObject((char *)vstr, vlen);
			/* 将成员和分值分别关联到跳跃表和字典中 */
			node = zslInsert(zs->zsl, score, ele);
			assert(dictAdd(zs->dict, ele, &node->score) == DICT_OK);
			incrRefCount(ele);
			/* 移动指针,指向下个元素 */
			zzlNext(zl, &eptr, &sptr);
		}
		/* 释放原来的ziplist */
		zfree(zobj->ptr);
		/* 更新对象的值,以及编码方式 */
		zobj->ptr = zs;
		zobj->encoding = REDIS_ENCODING_SKIPLIST;
	}
	else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) { /* 从SKIPLIST转换为ZIPLIST编码 */
		/* 新的ziplist */
		unsigned char *zl = ziplistNew();

		if (encoding != REDIS_ENCODING_ZIPLIST) assert(0);

		/* 指向跳跃表 */
		zs = zobj->ptr;

		/* 先释放字典,因为只需要跳跃表就可以遍历整个有序集合了 */
		dictRelease(zs->dict);

		/* 指向跳跃表的首个节点 */
		zfree(zs->zsl->header);
		zfree(zs->zsl);

		/* 遍历跳跃表,取出里面的元素,并将他们添加到ziplist */
		while (node) {
			/* 取出编码后的值对象 */
			ele = getDecodedObject(node->obj);
			/* 添加元素到ziplist */
			zl = zzlInsertAt(zl, NULL, ele, node->score);
			decrRefCount(ele);

			/* 沿着跳跃表的第0层前进 */
			next = node->level[0].forward;
			zslFreeNode(node);
			node = next;
		}
		/* 释放跳跃表 */
		zfree(zs);
		/* 更新对象的值,以及对象的编码方式 */
		zobj->ptr = zl;
		zobj->encoding = REDIS_ENCODING_ZIPLIST;
	}
	else
		assert(0);
}


/*
 * 从 ziplist 中删除 eptr 所指定的有序集合元素（包括成员和分值）
 */
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr) {
	unsigned char *p = eptr;

	zl = ziplistDelete(zl, &p);
	zl = ziplistDelete(zl, &p);
	return zl;
}

/*
 * 将带有给定成员和分值的新节点插入到 eptr 所指向的节点的前面，
 * 如果 eptr 为 NULL ，那么将新节点插入到 ziplist 的末端。
 *
 * 函数返回插入操作完成之后的 ziplist
 */
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, robj *ele, double score) {
	unsigned char *sptr;
	char scorebuf[128];
	int scorelen;
	size_t offset;
	/* 计算分值的字节长度 */
	scorelen = d2string(scorebuf, sizeof(scorebuf), score);

	if (eptr == NULL) { /* 插入到表尾,或者空表 */
		// | member-1 | score-1 | member-2 | score-2 | ... | member-N | score-N |
		/* 先推入元素 */
		zl = ziplistPush(zl, ele->ptr, sdslen(ele->ptr), ZIPLIST_TAIL);
		/* 然后推入分值 */
		zl = ziplistPush(zl, (unsigned char*)scorebuf, scorelen, ZIPLIST_TAIL);
	}
	else { /* 插入到某个节点的前面 */
		/* 插入成员 */
		offset = eptr - zl;
		zl = ziplistInsert(zl, eptr, ele->ptr, sdslen(ele->ptr));
	}
	return zl;
}

/*
 * 取出sptr指向节点所保存的有序集合元素的分值
 */
double zzlGetScore(unsigned char *sptr) {
	unsigned char *vstr;
	unsigned int vlen;
	long long vlong;
	char buf[128];
	double score;

	assert(sptr != NULL);
	/* 取出节点值 */
	assert(ziplistGet(sptr, &vstr, &vlen, &vlong));

	if (vstr) {
		/* 字符串转double */
		memcpy(buf, vstr, vlen);
		buf[vlen] = '\0';
		score = strtod(buf, NULL);
	}
	else {
		score = vlong;
	}
	return score;
}



/* 
 * 将 ele 成员和它的分值 score 添加到 ziplist 里面
 *
 * ziplist 里的各个节点按 score 值从小到大排列
 *
 * 这个函数假设 elem 不存在于有序集中
 */
unsigned char *zzlInsert(unsigned char *zl, robj *ele, double score) {
	/* 指向 ziplist 第一个节点（也即是有序集的 member 域） */
	unsigned char *eptr = ziplistIndex(zl, 0), *sptr;
	double s;

	/* 解码值 */
	ele = getDecodedObject(ele);
	/* 遍历整个ziplist */
	while (eptr != NULL) {
		/* 取出分值 */
		sptr = ziplistNext(zl, eptr);
		assert(sptr != NULL);
		s = zzlGetScore(sptr);
		if (s > score) {
			/* 遇到第一个 score 值比输入 score 大的节点
			* 将新节点插入在这个节点的前面，
			* 让节点在 ziplist 里根据 score 从小到大排列 */ 
			zl = zzlInsertAt(zl, eptr, ele, score);
			break;
		}
		else if (s == score) {
			/* 如果输入 score 和节点的 score 相同
			 * 那么根据 member 的字符串位置来决定新节点的插入位置 */
			if (zzlCompareElements(eptr, ele->ptr, sdslen(ele->ptr)) > 0) {
				zl = zzlInsertAt(zl, eptr, ele, score);
				break;
			}
		}
		/* 输入 score 比节点的 score 值要大
		 * 移动到下一个节点 */
		eptr = ziplistNext(zl, sptr);
	}
	/* Push on tail of list when it was not yet inserted. */
	if (eptr == NULL)
		zl = zzlInsertAt(zl, NULL, ele, score);

	decrRefCount(ele);
	return zl;
}


/*
 * 创建并返回一个新的跳跃表
 * T = O(1)
 */
zskiplist *zslCreate(void) {
	int j;
	zskiplist *zsl;
	/* 分配空间 */
	zsl = zmalloc(sizeof(*zsl));
	/* 设置高度和起始层数 */
	zsl->length = 0;
	zsl->level = 1;

	/* 初始化表头节点 */
	zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL, 0, NULL);
	for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
		zsl->header->level[j].forward = NULL;
		zsl->header->level[j].span = 0;
	}
	zsl->header->backward = NULL;

	/* 设置表尾 */
	zsl->tail = NULL;
	return zsl;
}

/*
 * 从 ziplist 编码的有序集合中查找 ele 成员，并将它的分值保存到 score 。
 *
 * 寻找成功返回指向成员 ele 的指针，查找失败返回 NULL 。
 */
unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score) {
	/* 定位到首个元素 */
	unsigned char *eptr = ziplistIndex(zl, 0), *sptr;
	/* 解码成员 */
	ele = getDecodedObject(ele);
	/* 遍历整个ziplist,查找元素 */
	while (eptr != NULL) {
		/* 指向分量 */
		sptr = ziplistNext(zl, eptr);
		assert(sptr != NULL);
		/* 对比成员 */
		if (ziplistCompare(eptr, ele->ptr, sdslen(ele->ptr))) {
			/* 成员匹配,取出分值 */
			if (score != NULL) *score = zzlGetScore(sptr);
			decrRefCount(ele);
			return eptr;
		}

		eptr = ziplistNext(zl, sptr);
	}
	decrRefCount(ele);
	/* 没有找到 */
	return NULL;
}


/*
 * 尝试从对象 o 中取出 double 值：
 *
 *  - 如果尝试失败的话，就返回指定的回复 msg 给客户端，函数返回 REDIS_ERR 。
 *
 *  - 取出成功的话，将值保存在 *target 中，函数返回 REDIS_OK 。
 */
int getDoubleFromObjectOrReply(redisClient *c, robj *o, double *target, const char *msg) {

	double value;

	if (getDoubleFromObject(o, &value) != REDIS_OK) {
		if (msg != NULL) {
			addReplyError(c, (char*)msg);
		}
		else {
			addReplyError(c, "value is not a valid float");
		}
		return REDIS_ERR;
	}

	*target = value;
	return REDIS_OK;
}

/* This generic command implements both ZADD and ZINCRBY. */
void zaddGenericCommand(redisClient *c, int incr) {
	static char *nanerr = "resulting score is not a number (NaN)";

	robj *key = c->argv[1];
	robj *ele;
	robj *zobj;
	robj *curobj;
	double score = 0, *scores = NULL, curscore = 0.0;
	int j, elements = (c->argc - 2) / 2;
	int added = 0, updated = 0;

	/* 输入的 score - member 参数必须是成对出现的 */
	if (c->argc % 2) {
		addReply(c, shared.syntaxerr);
		return;
	}

	/* 取出所有输入的 score 分值 */
	scores = zmalloc(sizeof(double)*elements);
	for (j = 0; j < elements; j++) {
		if (getDoubleFromObjectOrReply(c, c->argv[2 + j * 2], &scores[j], NULL)
			!= REDIS_OK) goto cleanup;
	}

	/* 取出有序集合对象 */
	zobj = lookupKeyWrite(c->db, key);
	if (zobj == NULL) {
		/* 有序集合不存在，创建新有序集合 */
		if (server.zset_max_ziplist_entries == 0 ||
			server.zset_max_ziplist_value < sdslen(c->argv[3]->ptr))
		{
			zobj = createZsetObject();
		}
		else {
			zobj = createZsetZiplistObject();
		}
		/* 关联对象到数据库 */
		dbAdd(c->db, key, zobj);
	}
	else {
		/* 对象存在，检查类型 */
		if (zobj->type != REDIS_ZSET) {
			addReply(c, shared.wrongtypeerr);
			goto cleanup;
		}
	}

	/* 处理所有元素 */
	for (j = 0; j < elements; j++) {
		score = scores[j];

		if (zobj->encoding == REDIS_ENCODING_ZIPLIST) { /* 有序集合为 ziplist 编码 */
			unsigned char *eptr;
			/* 查找成员 */
			ele = c->argv[3 + j * 2];
			if ((eptr = zzlFind(zobj->ptr, ele, &curscore)) != NULL) {
				/* 成员已存在 */
				if (incr) { /* ZINCRYBY 命令时使用 */
					score += curscore;
					if (isnan(score)) {
						addReplyError(c, nanerr);
						goto cleanup;
					}
				}

				/* 执行 ZINCRYBY 命令时，
				 * 或者用户通过 ZADD 修改成员的分值时执行 */
				if (score != curscore) {
					/* 删除已有元素 */
					zobj->ptr = zzlDelete(zobj->ptr, eptr);
					/* 重新插入元素 */
					zobj->ptr = zzlInsert(zobj->ptr, ele, score);
					/* 计数器 */
					server.dirty++;
					updated++;
				}
			}
			else {
				/* Optimize: check if the element is too large or the list
				* becomes too long *before* executing zzlInsert. */
				/* 元素不存在，直接添加 */
				zobj->ptr = zzlInsert(zobj->ptr, ele, score);

				/* 查看元素的数量，
				 * 看是否需要将 ZIPLIST 编码转换为有序集合 */
				if (zzlLength(zobj->ptr) > server.zset_max_ziplist_entries)
					zsetConvert(zobj, REDIS_ENCODING_SKIPLIST);

				/* 查看新添加元素的长度
				 * 看是否需要将 ZIPLIST 编码转换为有序集合 */
				if (sdslen(ele->ptr) > server.zset_max_ziplist_value)
					zsetConvert(zobj, REDIS_ENCODING_SKIPLIST);
				server.dirty++;
				added++;
			}
		}
		else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) { /* 有序集合为 SKIPLIST 编码 */
			zset *zs = zobj->ptr;
			zskiplistNode *znode;
			dictEntry *de;

			ele = c->argv[3 + j * 2] = tryObjectEncoding(c->argv[3 + j * 2]); /* 编码对象 */

			de = dictFind(zs->dict, ele); /* 查看成员是否存在 */
			if (de != NULL) { /* 成员存在 */
				curobj = dictGetKey(de); /* 取出成员 */
				curscore = *(double*)dictGetVal(de); /* 取出分值 */

				if (incr) { /* ZINCRYBY 时执行 */
					score += curscore;
					if (isnan(score)) {
						addReplyError(c, nanerr);
						/* Don't need to check if the sorted set is empty
						* because we know it has at least one element. */
						goto cleanup;
					}
				}

				/* 执行 ZINCRYBY 命令时，
				 * 或者用户通过 ZADD 修改成员的分值时执行 */
				if (score != curscore) {
					/* 删除原有元素 */
					zslDelete(zs->zsl, curscore, curobj);

					/* 重新插入元素 */
					znode = zslInsert(zs->zsl, score, curobj);
					incrRefCount(curobj); /* Re-inserted in skiplist. */

					/* 更新字典的分值指针 */
					dictGetVal(de) = &znode->score; /* Update score ptr. */
					server.dirty++;
					updated++;
				}
			}
			else {

				/* 元素不存在，直接添加到跳跃表 */
				znode = zslInsert(zs->zsl, score, ele);
				incrRefCount(ele); /* Inserted in skiplist. */

				/* 将元素关联到字典 */
				assert(dictAdd(zs->dict, ele, &znode->score) == DICT_OK);
				incrRefCount(ele); /* Added to dictionary. */
				server.dirty++;
				added++;
			}
		}
		else {
			assert(0);
		}
	}
	if (incr) /* ZINCRBY */
		addReplyDouble(c, score);
	else /* ZADD */
		addReplyLongLong(c, added);

cleanup:
	zfree(scores);
	if (added || updated) {
		signalModifiedKey(c->db, key);
	}
}


void zaddCommand(redisClient *c) {
	zaddGenericCommand(c, 0);
}

void zincrbyCommand(redisClient *c) {
	zaddGenericCommand(c, 1);
}

unsigned int zsetLength(robj *zobj) {
	int length = -1;

	if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
		length = zzlLength(zobj->ptr);

	}
	else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
		length = ((zset*)zobj->ptr)->zsl->length;

	}
	else {
		assert(0);
	}

	return length;
}


void zcardCommand(redisClient *c) { /* 返回有序集 key 的基数 */
	robj *key = c->argv[1];
	robj *zobj;

	/* 取出有序集合 */
	if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
		checkType(c, zobj, REDIS_ZSET)) return;

	/* 返回集合基数 */
	addReplyLongLong(c, zsetLength(zobj));
}

/*
 * 对 min 和 max 进行分析，并将区间的值保存在 spec 中。
 *
 * 分析成功返回 REDIS_OK ，分析出错导致失败返回 REDIS_ERR 。
 *
 * T = O(N)
 */
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
	char *eptr;

	/* 默认为闭区间 */
	spec->minex = spec->maxex = 0;

	if (min->encoding == REDIS_ENCODING_INT) {
		/* min 的值为整数，开区间 */
		spec->min = (long)min->ptr;
	}
	else {
		/* min 对象为字符串，分析 min 的值并决定区间 */
		if (((char*)min->ptr)[0] == '(') {
			spec->min = strtod((char*)min->ptr + 1, &eptr);
			if (eptr[0] != '\0' || isnan(spec->min)) return REDIS_ERR;
			spec->minex = 1;
		}
		else {
			spec->min = strtod((char*)min->ptr, &eptr);
			if (eptr[0] != '\0' || isnan(spec->min)) return REDIS_ERR;
		}
	}

	if (max->encoding == REDIS_ENCODING_INT) {
		/* max 的值为整数，开区间 */
		spec->max = (long)max->ptr;
	}
	else {
		/* max 对象为字符串，分析 max 的值并决定区间 */
		if (((char*)max->ptr)[0] == '(') {
			spec->max = strtod((char*)max->ptr + 1, &eptr);
			if (eptr[0] != '\0' || isnan(spec->max)) return REDIS_ERR;
			spec->maxex = 1;
		}
		else {
			spec->max = strtod((char*)max->ptr, &eptr);
			if (eptr[0] != '\0' || isnan(spec->max)) return REDIS_ERR;
		}
	}
	return REDIS_OK;
}

/* 
 * 如果给定的 ziplist 有至少一个节点符合 range 中指定的范围，
 * 那么函数返回 1 ，否则返回 0 。
 */
int zzlIsInRange(unsigned char *zl, zrangespec *range) {
	unsigned char *p;
	double score;

	/* Test for ranges that will always be empty. */
	if (range->min > range->max ||
		(range->min == range->max && (range->minex || range->maxex)))
		return 0;

	/* 取出 ziplist 中的最大分值，并和 range 的最大值对比 */
	p = ziplistIndex(zl, -1); /* Last score. */
	if (p == NULL) return 0; /* Empty sorted set */
	score = zzlGetScore(p);
	if (!zslValueGteMin(score, range))
		return 0;

	/* 取出 ziplist 中的最小值，并和 range 的最小值进行对比 */
	p = ziplistIndex(zl, 1); /* First score. */
	assert(p != NULL);
	score = zzlGetScore(p);
	if (!zslValueLteMax(score, range))
		return 0;

	/* ziplist 有至少一个节点符合范围 */
	return 1;
}

/*
 * 检测给定值 value 是否小于（或小于等于）范围 spec 中的 max 项。
 *
 * 返回 1 表示 value 小于等于 max 项，否则返回 0 。
 *
 * T = O(1)
 */
static int zslValueLteMax(double value, zrangespec *spec) {
	return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/*
 * 检测给定值 value 是否大于（或大于等于）范围 spec 中的 min 项。
 *
 * 返回 1 表示 value 大于等于 min 项，否则返回 0 。
 *
 * T = O(1)
 */
static int zslValueGteMin(double value, zrangespec *spec) {
	return spec->minex ? (value > spec->min) : (value >= spec->min);
}

/*
 * 返回第一个 score 值在给定范围内的节点
 *
 * 如果没有节点的 score 值在给定范围，返回 NULL 。
 */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range) {
	/* 从表头开始遍历 */
	unsigned char *eptr = ziplistIndex(zl, 0), *sptr;
	double score;

	if (!zzlIsInRange(zl, range)) return NULL;

	/* 分值在 ziplist 中是从小到大排列的, 从表头向表尾遍历 */
	while (eptr != NULL) {
		sptr = ziplistNext(zl, eptr);
		assert(sptr != NULL);

		score = zzlGetScore(sptr);
		if (zslValueGteMin(score, range)) {
			/* 遇上第一个符合范围的分值，返回它的节点指针 */
			if (zslValueLteMax(score, range))
				return eptr;
			return NULL;
		}

		eptr = ziplistNext(zl, sptr);
	}

	return NULL;
}

/* 
 * 查找包含给定分值和成员对象的节点在跳跃表中的排位。
 *
 * 如果没有包含给定分值和成员对象的节点，返回 0 ，否则返回排位。
 *
 * 注意，因为跳跃表的表头也被计算在内，所以返回的排位以 1 为起始值。
 *
 * T_wrost = O(N), T_avg = O(log N)
 */
unsigned long zslGetRank(zskiplist *zsl, double score, robj *o) {
	zskiplistNode *x;
	unsigned long rank = 0;
	int i;

	/* 遍历整个跳跃表 */
	x = zsl->header;
	for (i = zsl->level - 1; i >= 0; i--) {

		/* 遍历节点并对比元素 */
		while (x->level[i].forward &&
			(x->level[i].forward->score < score ||
				/* 比对分值 */
			(x->level[i].forward->score == score &&
				/* 比对成员对象 */
				compareStringObjects(x->level[i].forward->obj, o) <= 0))) {

			/* 累积跨越的节点数量 */
			rank += x->level[i].span;

			/* 沿着前进指针遍历跳跃表 */
			x = x->level[i].forward;
		}
		/* 必须确保不仅分值相等，而且成员对象也要相等 */
		if (x->obj && equalStringObjects(x->obj, o)) {
			return rank;
		}
	}

	/* 没找到 */
	return 0;
}

/*
 * 如果给定的分值范围包含在跳跃表的分值范围之内，
 * 那么返回 1 ，否则返回 0 。
 *
 * T = O(1)
 */
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
	zskiplistNode *x;
	/* 先排除总为空的范围值 */
	if (range->min > range->max ||
		(range->min == range->max && (range->minex || range->maxex)))
		return 0;

	/* 检查最大分值 */
	x = zsl->tail;
	if (x == NULL || !zslValueGteMin(x->score, range))
		return 0;

	/* 检查最小分值 */
	x = zsl->header->level[0].forward;
	if (x == NULL || !zslValueLteMax(x->score, range))
		return 0;

	return 1;
}
/* 
 * 返回 zsl 中第一个分值符合 range 中指定范围的节点。
 *
 * 如果 zsl 中没有符合范围的节点，返回 NULL 。
 *
 * T_wrost = O(N), T_avg = O(log N)
 */
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range) {
	zskiplistNode *x;
	int i;

	/* If everything is out of range, return early. */
	if (!zslIsInRange(zsl, range)) return NULL;

	/* 遍历跳跃表，查找符合范围 min 项的节点 */
	x = zsl->header;
	for (i = zsl->level - 1; i >= 0; i--) {
		/* Go forward while *OUT* of range. */
		while (x->level[i].forward &&
			!zslValueGteMin(x->level[i].forward->score, range))
			x = x->level[i].forward;
	}

	/* This is an inner range, so the next node cannot be NULL. */
	x = x->level[0].forward;
	assert(x != NULL);

	/* 检查节点是否符合范围的 max 项 */
	if (!zslValueLteMax(x->score, range)) return NULL;
	return x;
}

/*
 * 返回 zsl 中最后一个分值符合 range 中指定范围的节点。
 *
 * 如果 zsl 中没有符合范围的节点，返回 NULL 。
 *
 * T_wrost = O(N), T_avg = O(log N)
 */
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range) {
	zskiplistNode *x;
	int i;
	/* 先确保跳跃表中至少有一个节点符合 range 指定的范围，
	 * 否则直接失败 */
	if (!zslIsInRange(zsl, range)) return NULL;

	/* 遍历跳跃表，查找符合范围 max 项的节点 */
	x = zsl->header;
	for (i = zsl->level - 1; i >= 0; i--) {
		/* Go forward while *IN* range. */
		while (x->level[i].forward &&
			zslValueLteMax(x->level[i].forward->score, range))
			x = x->level[i].forward;
	}

	/* This is an inner range, so this node cannot be NULL. */
	assert(x != NULL);

	/* 检查节点是否符合范围的 min 项 */
	if (!zslValueGteMin(x->score, range)) return NULL;

	/* 返回节点 */
	return x;
}


void zcountCommand(redisClient *c) {
	/* zcount key min max 返回有序集key中(默认包括score值等于min或max),score值在min和max之间的成员的数量 */
	robj *key = c->argv[1];
	robj *zobj;
	zrangespec range;
	int count = 0;

	/* 分析并读入范围参数 */
	if (zslParseRange(c->argv[2], c->argv[3], &range) != REDIS_OK) {
		addReplyError(c, "min or max is not a float");
		return;
	}

	/* 取出有序集合 */
	if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
		checkType(c, zobj, REDIS_ZSET)) return;

	if (zobj->encoding == REDIS_ENCODING_ZIPLIST) { /* 如果底层的编码是压缩表的话 */
		unsigned char *zl = zobj->ptr;
		unsigned char *eptr, *sptr;
		double score;

		/* 指向指定范围内第一个元素的成员 */
		eptr = zzlFirstInRange(zl, &range);

		/* 没有任何元素在这个范围内，直接返回 */
		if (eptr == NULL) {
			addReply(c, shared.czero);
			return;
		}

		/* First element is in range */
		/* 取出分值 */
		sptr = ziplistNext(zl, eptr);
		score = zzlGetScore(sptr);
		assert(zslValueLteMax(score, &range));

		/* Iterate over elements in range */
		/* 遍历范围内的所有元素 */
		while (eptr) {

			score = zzlGetScore(sptr);

			/* 如果分值不符合范围，跳出 */
			if (!zslValueLteMax(score, &range)) {
				break;
			}
			else { /* 分值符合范围，增加 count 计数器 */
				count++;
				zzlNext(zl, &eptr, &sptr); /* 然后指向下一个元素 */
			}
		}

	}
	else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) { /* 如果底层编码是跳跃表的话 */
		zset *zs = zobj->ptr;
		zskiplist *zsl = zs->zsl;
		zskiplistNode *zn;
		unsigned long rank;

		/* 指向指定范围内第一个元素 */
		zn = zslFirstInRange(zsl, &range);

		/* 如果有至少一个元素在范围内，那么执行以下代码 */
		if (zn != NULL) {
			/* 确定范围内第一个元素的排位 */
			rank = zslGetRank(zsl, zn->score, zn->obj);
			count = (zsl->length - (rank - 1));
			/* 指向指定范围内的最后一个元素 */
			zn = zslLastInRange(zsl, &range);

			/* 如果范围内的最后一个元素不为空，那么执行以下代码 */
			if (zn != NULL) {
				/* 确定范围内最后一个元素的排位 */
				rank = zslGetRank(zsl, zn->score, zn->obj);
				/* 这里计算的就是第一个和最后一个两个元素之间的元素数量,（包括这两个元素） */
				count -= (zsl->length - rank);
			}
		}
	}
	else 
		assert(0);
	addReplyLongLong(c, count);
}

void zrankGenericCommand(redisClient *c, int reverse) {
	robj *key = c->argv[1];
	robj *ele = c->argv[2];
	robj *zobj;
	unsigned long llen;
	unsigned long rank;

	/* 有序集合 */
	if ((zobj = lookupKeyReadOrReply(c, key, shared.nullbulk)) == NULL ||
		checkType(c, zobj, REDIS_ZSET)) return;
	/* 元素数量 */
	llen = zsetLength(zobj);

	sdsEncodedObject(ele);

	if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *zl = zobj->ptr;
		unsigned char *eptr, *sptr;

		eptr = ziplistIndex(zl, 0);
		assert(eptr != NULL);
		sptr = ziplistNext(zl, eptr);
		assert( sptr != NULL);

		/* 计算排名 */
		rank = 1;
		while (eptr != NULL) {
			if (ziplistCompare(eptr, ele->ptr, sdslen(ele->ptr)))
				break;
			rank++;
			zzlNext(zl, &eptr, &sptr);
		}

		if (eptr != NULL) {
			/* ZRANK 还是 ZREVRANK ？ */
			if (reverse)
				addReplyLongLong(c, llen - rank);
			else
				addReplyLongLong(c, rank - 1);
		}
		else {
			addReply(c, shared.nullbulk);
		}

	}
	else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
		zset *zs = zobj->ptr;
		zskiplist *zsl = zs->zsl;
		dictEntry *de;
		double score;

		/* 从字典中取出元素 */
		ele = c->argv[2] = tryObjectEncoding(c->argv[2]);
		de = dictFind(zs->dict, ele);
		if (de != NULL) {
			/* 取出元素的分值 */
			score = *(double*)dictGetVal(de);
			/* 在跳跃表中计算该元素的排位 */
			rank = zslGetRank(zsl, score, ele);
			/* ZRANK 还是 ZREVRANK ？ */
			if (reverse)
				addReplyLongLong(c, llen - rank);
			else
				addReplyLongLong(c, rank - 1);
		}
		else {
			addReply(c, shared.nullbulk);
		}

	}
	else {
		assert(NULL);
	}
}

void zrankCommand(redisClient *c) {
	/* zrank key member 返回有序集 key 中成员 member 的排名 */
	zrankGenericCommand(c, 0);
}

void zscoreCommand(redisClient *c) {
	robj *key = c->argv[1];
	robj *zobj;
	double score;

	if ((zobj = lookupKeyReadOrReply(c, key, shared.nullbulk)) == NULL ||
		checkType(c, zobj, REDIS_ZSET)) return;

	if (zobj->encoding == REDIS_ENCODING_ZIPLIST) { /* ziplist */
		/* 取出元素 */
		if (zzlFind(zobj->ptr, c->argv[2], &score) != NULL)
			/* 回复分值 */
			addReplyDouble(c, score);
		else
			addReply(c, shared.nullbulk);
	} 
	else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) { /* SKIPLIST */
		zset *zs = zobj->ptr;
		dictEntry *de;

		c->argv[2] = tryObjectEncoding(c->argv[2]);
		/* 直接从字典中取出并返回分值 */
		de = dictFind(zs->dict, c->argv[2]);
		if (de != NULL) {
			score = *(double*)dictGetVal(de);
			addReplyDouble(c, score);
		}
		else {
			addReply(c, shared.nullbulk);
		}
	}
	else {
		assert(NULL);
	}
}

/*
 * DIRTY 常量用于标识在下次迭代之前要进行清理。
 *
 * 当 DIRTY 常量作用于 long long 值时，该值不需要被清理。
 *
 * 因为它表示 ell 已经持有一个 long long 值，
 * 或者已经将一个对象转换为 long long 值。
 *
 * 当转换成功时， OPVAL_VALID_LL 被设置。
 */
#define OPVAL_DIRTY_ROBJ 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

/*
 * 用于保存从迭代器里取得的值的结构
 */
typedef struct {

	int flags;

	unsigned char _buf[32]; /* Private buffer. */

							/* 可以用于保存 member 的几个类型 */
	robj *ele;
	unsigned char *estr;
	unsigned int elen;
	long long ell;

	/* 分值 */
	double score;

} zsetopval;

/* 类型别名 */
typedef union _iterset iterset;
typedef union _iterzset iterzset;

/*
 * 多态集合迭代器：可迭代集合或者有序集合
 */
typedef struct {

	/* 被迭代的对象 */
	robj *subject;

	/* 对象的类型 */
	int type; /* Set, sorted set */

	/* 编码 */
	int encoding;

	/* 权重 */
	double weight;

	union {
		/* 集合迭代器 */
		union _iterset {
			/* intset 迭代器 */
			struct {
				/* 被迭代的 intset */
				intset *is;
				/* 当前节点索引 */
				int ii;
			} is;
			/* 字典迭代器 */
			struct {
				/* 被迭代的字典 */
				dict *dict;
				/* 字典迭代器 */
				dictIterator *di;
				/* 当前字典节点 */
				dictEntry *de;
			} ht;
		} set;

		/* 有序集合迭代器. */
		union _iterzset {
			/* ziplist 迭代器 */
			struct {
				/* 被迭代的 ziplist */
				unsigned char *zl;
				/* 当前成员指针和当前分值指针 */
				unsigned char *eptr, *sptr;
			} zl;
			/* zset 迭代器 */
			struct {
				/* 被迭代的 zset */
				zset *zs;
				/* 当前跳跃表节点 */
				zskiplistNode *node;
			} sl;
		} zset;
	} iter;
} zsetopsrc;

/*
 * 根据 aggregate 参数的值，决定如何对 *target 和 val 进行聚合计算。
 */
inline static void zunionInterAggregate(double *target, double val, int aggregate) {
	if (aggregate == REDIS_AGGR_SUM) { /* 求和 */
		*target = *target + val;
		
		/* 检查是否溢出 */
		if (isnan(*target)) *target = 0.0;
	}
	else if (aggregate == REDIS_AGGR_MIN) { /* 求两者小数 */
		*target = val < *target ? val : *target;
	}
	else if (aggregate == REDIS_AGGR_MAX) { /* 求两者大数 */
		*target = val > *target ? val : *target;

	}
	else {
		/* safety net */
		assert(NULL);
	}
}

/*
 * 从 val 中取出 long long 值。
 */
int zuiLongLongFromValue(zsetopval *val) {

	if (!(val->flags & OPVAL_DIRTY_LL)) {

		/* 打开标识 DIRTY LL */
		val->flags |= OPVAL_DIRTY_LL;

		/* 从对象中取值 */
		if (val->ele != NULL) {
			/* 从 INT 编码的字符串中取出整数 */
			if (val->ele->encoding == REDIS_ENCODING_INT) {
				val->ell = (long)val->ele->ptr;
				val->flags |= OPVAL_VALID_LL;
			}
			else if (sdsEncodedObject(val->ele)) { /* 从未编码的字符串中转换整数 */
				if (string2ll(val->ele->ptr, sdslen(val->ele->ptr), &val->ell))
					val->flags |= OPVAL_VALID_LL;
			}
			else {
				assert(NULL);
			}
		}
		else if (val->estr != NULL) { /* 从 ziplist 节点中取值 */
			/* 将节点值（一个字符串）转换为整数 */
			if (string2ll((char*)val->estr, val->elen, &val->ell))
				val->flags |= OPVAL_VALID_LL;

		}
		else {
			/* 总是打开 VALID LL 标识 */
			val->flags |= OPVAL_VALID_LL;
		}
	}
	/* 检查 VALID LL 标识是否已打开 */
	return val->flags & OPVAL_VALID_LL;
}



/*
 * 根据 val 中的值，创建对象
 */
robj *zuiObjectFromValue(zsetopval *val) {

	if (val->ele == NULL) {

		/* 从 long long 值中创建对象 */
		if (val->estr != NULL) {
			val->ele = createStringObject((char*)val->estr, val->elen);
		}
		else {
			val->ele = createStringObjectFromLongLong(val->ell);
		}

		/* 打开 ROBJ 标识 */
		val->flags |= OPVAL_DIRTY_ROBJ;
	}

	/* 返回值对象 */
	return val->ele;
}


/*
 * 从 val 中取出字符串
 */
int zuiBufferFromValue(zsetopval *val) {

	if (val->estr == NULL) {
		if (val->ele != NULL) {
			if (val->ele->encoding == REDIS_ENCODING_INT) {
				val->elen = ll2string((char*)val->_buf, sizeof(val->_buf), (long)val->ele->ptr);
				val->estr = val->_buf;
			}
			else if (sdsEncodedObject(val->ele)) {
				val->elen = sdslen(val->ele->ptr);
				val->estr = val->ele->ptr;
			}
			else {
				assert(NULL);
			}
		}
		else {
			val->elen = ll2string((char*)val->_buf, sizeof(val->_buf), val->ell);
			val->estr = val->_buf;
		}
	}

	return 1;
}

/*
 * 在迭代器指定的对象中查找给定元素
 *
 * 找到返回 1 ，否则返回 0 。
 */
int zuiFind(zsetopsrc *op, zsetopval *val, double *score) {

	if (op->subject == NULL)
		return 0;

	if (op->type == REDIS_SET) { /* 集合 */
		/* 成员为整数，分值为 1.0 */
		if (op->encoding == REDIS_ENCODING_INTSET) {
			if (zuiLongLongFromValue(val) &&
				intsetFind(op->subject->ptr, val->ell))
			{
				*score = 1.0;
				return 1;
			}
			else {
				return 0;
			}
		}
		else if (op->encoding == REDIS_ENCODING_HT) { /* 成为为对象，分值为 1.0 */
			dict *ht = op->subject->ptr;
			zuiObjectFromValue(val);
			if (dictFind(ht, val->ele) != NULL) {
				*score = 1.0;
				return 1;
			}
			else {
				return 0;
			}
		}
		else {
			assert(NULL);
		}
	}
	else if (op->type == REDIS_ZSET) { /* 有序集合 */
		/* 取出对象 */
		zuiObjectFromValue(val);

		if (op->encoding == REDIS_ENCODING_ZIPLIST) {

			/* 取出成员和分值 */
			if (zzlFind(op->subject->ptr, val->ele, score) != NULL) {
				/* Score is already set by zzlFind. */
				return 1;
			}
			else {
				return 0;
			}
		}
		else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
			zset *zs = op->subject->ptr;
			dictEntry *de;

			/* 从字典中查找成员对象 */
			if ((de = dictFind(zs->dict, val->ele)) != NULL) {
				/* 取出分值 */
				*score = *(double*)dictGetVal(de);
				return 1;
			}
			else {
				return 0;
			}
		}
		else {
			assert(NULL);
		}
	}
	else {
		assert(NULL);
	}
}

int zuiLength(zsetopsrc *op);
/*
 * 对比两个被迭代对象的基数
 */
int zuiCompareByCardinality(const void *s1, const void *s2) {
	return zuiLength((zsetopsrc*)s1) - zuiLength((zsetopsrc*)s2);
}

/*
 * 初始化迭代器
 */
void zuiInitIterator(zsetopsrc *op) {
	if (op->subject == NULL) /* 迭代对象为空，无动作 */
		return;

	if (op->type == REDIS_SET) { /* 迭代集合 */

		iterset *it = &op->iter.set;

		if (op->encoding == REDIS_ENCODING_INTSET) { /* 迭代 intset */
			it->is.is = op->subject->ptr;
			it->is.ii = 0;
		}
		else if (op->encoding == REDIS_ENCODING_HT) { /* 迭代字典 */
			it->ht.dict = op->subject->ptr;
			it->ht.di = dictGetIterator(op->subject->ptr);
			it->ht.de = dictNext(it->ht.di);

		}
		else {
			assert(NULL);
		}
	}
	else if (op->type == REDIS_ZSET) { /* 迭代有序集合 */

		iterzset *it = &op->iter.zset;

		/* 迭代 ziplist */
		if (op->encoding == REDIS_ENCODING_ZIPLIST) {
			it->zl.zl = op->subject->ptr;
			it->zl.eptr = ziplistIndex(it->zl.zl, 0);
			if (it->zl.eptr != NULL) {
				it->zl.sptr = ziplistNext(it->zl.zl, it->zl.eptr);
				assert(it->zl.sptr != NULL);
			}
		}
		else if (op->encoding == REDIS_ENCODING_SKIPLIST) { /* 迭代跳跃表 */
			it->sl.zs = op->subject->ptr;
			it->sl.node = it->sl.zs->zsl->header->level[0].forward;
		}
		else {
			assert(NULL);
		}
	}
	else { /* 未知对象类型 */
		assert(NULL);
	}
}

/*
 * 清空迭代器
 */
void zuiClearIterator(zsetopsrc *op) {

	if (op->subject == NULL)
		return;

	if (op->type == REDIS_SET) {

		iterset *it = &op->iter.set;

		if (op->encoding == REDIS_ENCODING_INTSET) {
			REDIS_NOTUSED(it); /* skip */

		}
		else if (op->encoding == REDIS_ENCODING_HT) {
			dictReleaseIterator(it->ht.di);

		}
		else {
			assert(NULL);
		}

	}
	else if (op->type == REDIS_ZSET) {
		iterzset *it = &op->iter.zset;
		if (op->encoding == REDIS_ENCODING_ZIPLIST) {
			REDIS_NOTUSED(it); /* skip */
		}
		else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
			REDIS_NOTUSED(it); /* skip */
		}
		else {
			assert(NULL);
		}
	}
	else {
		assert(NULL);
	}
}

/*
 * 返回正在被迭代的元素的长度
 */
int zuiLength(zsetopsrc *op) {

	if (op->subject == NULL)
		return 0;

	if (op->type == REDIS_SET) {
		if (op->encoding == REDIS_ENCODING_INTSET) {
			return intsetLen(op->subject->ptr);
		}
		else if (op->encoding == REDIS_ENCODING_HT) {
			dict *ht = op->subject->ptr;
			return dictSize(ht);
		}
		else {
			assert(NULL);
		}

	}
	else if (op->type == REDIS_ZSET) {

		if (op->encoding == REDIS_ENCODING_ZIPLIST) {
			return zzlLength(op->subject->ptr);
		}
		else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
			zset *zs = op->subject->ptr;
			return zs->zsl->length;
		}
		else {
			assert(NULL);
		}
	}
	else {
		assert(NULL);
	}
}

/* 
 * 检查迭代器当前指向的元素是否合法，如果是的话，将它保存到传入的 val 结构中，
 * 然后将迭代器的当前指针指向下一元素，函数返回 1 。
 *
 * 如果当前指向的元素不合法，那么说明对象已经迭代完毕，函数返回 0 。
 */
int zuiNext(zsetopsrc *op, zsetopval *val) {

	if (op->subject == NULL)
		return 0;

	/* 对上次的对象进行清理 */
	if (val->flags & OPVAL_DIRTY_ROBJ)
		decrRefCount(val->ele);

	/* 清零 val 结构 */
	memset(val, 0, sizeof(zsetopval));

	/* 迭代集合 */
	if (op->type == REDIS_SET) {

		iterset *it = &op->iter.set;

		/* ziplist 编码的集合 */
		if (op->encoding == REDIS_ENCODING_INTSET) {
			int64_t ell;

			/* 取出成员 */
			if (!intsetGet(it->is.is, it->is.ii, &ell))
				return 0;
			val->ell = ell;
			/* 分值默认为 1.0 */
			val->score = 1.0;

			/* Move to next element. */
			it->is.ii++;
		}
		else if (op->encoding == REDIS_ENCODING_HT) {
			/* 已为空？ */
			if (it->ht.de == NULL)
				return 0;

			/* 取出成员 */
			val->ele = dictGetKey(it->ht.de);
			/* 分值默认为 1.0 */
			val->score = 1.0;

			/* Move to next element. */
			it->ht.de = dictNext(it->ht.di);
		}
		else {
			assert(NULL);
		}
	}
	else if (op->type == REDIS_ZSET) {
		iterzset *it = &op->iter.zset;
		/* ziplist 编码的有序集合 */
		if (op->encoding == REDIS_ENCODING_ZIPLIST) {
			/* 为空？ */
			if (it->zl.eptr == NULL || it->zl.sptr == NULL)
				return 0;
			/* 取出成员 */
			assert(ziplistGet(it->zl.eptr, &val->estr, &val->elen, &val->ell));
			/* 取出分值 */
			val->score = zzlGetScore(it->zl.sptr);

			/* Move to next element. */
			zzlNext(it->zl.zl, &it->zl.eptr, &it->zl.sptr);
		}
		else if (op->encoding == REDIS_ENCODING_SKIPLIST) {

			if (it->sl.node == NULL)
				return 0;

			val->ele = it->sl.node->obj;
			val->score = it->sl.node->score;

			/* Move to next element. */
			it->sl.node = it->sl.node->level[0].forward;
		}
		else {
			assert(NULL);
		}
	}
	else {
		assert(NULL);
	}

	return 1;
}

void zunionInterGenericCommand(redisClient *c, robj *dstkey, int op) {
	int i, j;
	long long setnum;
	int aggregate = REDIS_AGGR_SUM;
	zsetopsrc *src;
	zsetopval zval;
	robj *tmp;
	unsigned int maxelelen = 0;
	robj *dstobj;
	zset *dstzset;
	zskiplistNode *znode;
	int touched = 0;

	/* 取出要处理的有序集合的个数 setnum */
	if ((getLongFromObjectOrReply(c, c->argv[2], &setnum, NULL) != REDIS_OK))
		return;

	if (setnum < 1) {
		addReplyError(c,
			"at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
		return;
	}
	/* setnum 参数和传入的 key 数量不相同，出错 */
	if (setnum > c->argc - 3) {
		addReply(c, shared.syntaxerr);
		return;
	}

	/* 为每个输入 key 创建一个迭代器 */
	src = zcalloc(sizeof(zsetopsrc) * setnum);
	for (i = 0, j = 3; i < setnum; i++, j++) {

		/* 取出 key 对象 */
		robj *obj = lookupKeyWrite(c->db, c->argv[j]);

		/* 创建迭代器 */
		if (obj != NULL) {
			if (obj->type != REDIS_ZSET && obj->type != REDIS_SET) {
				zfree(src);
				addReply(c, shared.wrongtypeerr);
				return;
			}

			src[i].subject = obj;
			src[i].type = obj->type;
			src[i].encoding = obj->encoding;	
		}
		else { /* 不存在的对象设为 NULL */
			src[i].subject = NULL;
		}
		/* 默认权重为 1.0 */
		src[i].weight = 1.0;
	}
	/* 分析并读入可选参数 */
	if (j < c->argc) {
		int remaining = c->argc - j;

		while (remaining) {
			if (remaining >= (setnum + 1) && !strcasecmp(c->argv[j]->ptr, "weights")) {
				j++; remaining--;
				/* 权重参数 */
				for (i = 0; i < setnum; i++, j++, remaining--) {
					if (getDoubleFromObjectOrReply(c, c->argv[j], &src[i].weight,
						"weight value is not a float") != REDIS_OK)
					{
						zfree(src);
						return;
					}
				}

			}
			else if (remaining >= 2 && !strcasecmp(c->argv[j]->ptr, "aggregate")) {
				j++; remaining--;
				/* 聚合方式 */
				if (!strcasecmp(c->argv[j]->ptr, "sum")) {
					aggregate = REDIS_AGGR_SUM;
				}
				else if (!strcasecmp(c->argv[j]->ptr, "min")) {
					aggregate = REDIS_AGGR_MIN;
				}
				else if (!strcasecmp(c->argv[j]->ptr, "max")) {
					aggregate = REDIS_AGGR_MAX;
				}
				else {
					zfree(src);
					addReply(c, shared.syntaxerr);
					return;
				}
				j++; remaining--;

			}
			else {
				zfree(src);
				addReply(c, shared.syntaxerr);
				return;
			}
		}
	}

	/* 对所有集合进行排序，以减少算法的常数项 */
	qsort(src, setnum, sizeof(zsetopsrc), zuiCompareByCardinality);

	/* 创建结果集对象 */
	dstobj = createZsetObject();
	dstzset = dstobj->ptr;
	memset(&zval, 0, sizeof(zval));

	if (op == REDIS_OP_INTER) {
		/* 只处理非空集合 */
		if (zuiLength(&src[0]) > 0) {
			/* 遍历基数最小的 src[0] 集合 */
			zuiInitIterator(&src[0]);
			while (zuiNext(&src[0], &zval)) {
				double score, value;

				/* 计算加权分值 */
				score = src[0].weight * zval.score;
				if (isnan(score)) score = 0;

				/* 将 src[0] 集合中的元素和其他集合中的元素做加权聚合计算 */
				for (j = 1; j < setnum; j++) {
					/* 如果当前迭代到的 src[j] 的对象和 src[0] 的对象一样，
					* 那么 src[0] 出现的元素必然也出现在 src[j]
					* 那么我们可以直接计算聚合值，
					* 不必进行 zuiFind 去确保元素是否出现
					* 这种情况在某个 key 输入了两次，
					* 并且这个 key 是所有输入集合中基数最小的集合时会出现 */
					if (src[j].subject == src[0].subject) {
						value = zval.score*src[j].weight;
						zunionInterAggregate(&score, value, aggregate);

						/* 如果能在其他集合找到当前迭代到的元素的话
						 * 那么进行聚合计算 */
					}
					else if (zuiFind(&src[j], &zval, &value)) {
						value *= src[j].weight;
						zunionInterAggregate(&score, value, aggregate);

						/* 如果当前元素没出现在某个集合，那么跳出 for 循环
						 * 处理下个元素 */
					}
					else {
						break;
					}
				}

				/* 只在交集元素出现时，才执行以下代码 */
				if (j == setnum) {
					/* 取出值对象 */
					tmp = zuiObjectFromValue(&zval);
					/* 加入到有序集合中 */
					znode = zslInsert(dstzset->zsl, score, tmp);
					incrRefCount(tmp); /* added to skiplist */
								
					dictAdd(dstzset->dict, tmp, &znode->score);
					incrRefCount(tmp); /* 加入到字典中*/

					/* 更新字符串对象的最大长度 */
					if (sdsEncodedObject(tmp)) {
						if (sdslen(tmp->ptr) > maxelelen)
							maxelelen = sdslen(tmp->ptr);
					}
				}
			}
			zuiClearIterator(&src[0]);
		}
	}
	else if (op == REDIS_OP_UNION) {

		/* 遍历所有输入集合 */
		for (i = 0; i < setnum; i++) {

			/* 跳过空集合 */
			if (zuiLength(&src[i]) == 0)
				continue;

			/* 遍历所有集合元素 */
			zuiInitIterator(&src[i]);
			while (zuiNext(&src[i], &zval)) {
				double score, value;

				/* 跳过已处理元素 */
				if (dictFind(dstzset->dict, zuiObjectFromValue(&zval)) != NULL)
					continue;

				/* 初始化分值 */
				score = src[i].weight * zval.score;
				/* 溢出时设为 0 */
				if (isnan(score)) score = 0;

				for (j = (i + 1); j < setnum; j++) {
					/* 当前元素的集合和被迭代集合一样
					* 所以同一个元素必然出现在 src[j] 和 src[i]
					* 程序直接计算它们的聚合值
					* 而不必使用 zuiFind 来检查元素是否存在 */
					if (src[j].subject == src[i].subject) {
						value = zval.score*src[j].weight;
						zunionInterAggregate(&score, value, aggregate);
					}
					else if (zuiFind(&src[j], &zval, &value)) { /* 检查成员是否存在 */
						value *= src[j].weight;
						zunionInterAggregate(&score, value, aggregate);
					}
				}

				/* 取出成员 */
				tmp = zuiObjectFromValue(&zval);
				/* 插入并集元素到跳跃表 */
				znode = zslInsert(dstzset->zsl, score, tmp);
				incrRefCount(zval.ele); 
				/* 添加元素到字典 */
				dictAdd(dstzset->dict, tmp, &znode->score);
				incrRefCount(zval.ele);

				/* 更新字符串最大长度 */
				if (sdsEncodedObject(tmp)) {
					if (sdslen(tmp->ptr) > maxelelen)
						maxelelen = sdslen(tmp->ptr);
				}
			}
			zuiClearIterator(&src[i]);
		}
	}
	else {
		mylog("%s", "Unknown operator");
		assert(0);
	}

	/* 删除已存在的 dstkey ，等待后面用新对象代替它 */
	if (dbDelete(c->db, dstkey)) {
		signalModifiedKey(c->db, dstkey);
		server.dirty++;
		touched = 1;
		// todo
	}

	/* 如果结果集合的长度不为 0 */
	if (dstzset->zsl->length) {
		/* 看是否需要对结果集合进行编码转换 */
		if (dstzset->zsl->length <= server.zset_max_ziplist_entries &&
			maxelelen <= server.zset_max_ziplist_value)
			zsetConvert(dstobj, REDIS_ENCODING_ZIPLIST);

		/* 将结果集合关联到数据库 */
		dbAdd(c->db, dstkey, dstobj);

		/* 回复结果集合的长度 */
		addReplyLongLong(c, zsetLength(dstobj));

		if (touched)  signalModifiedKey(c->db, dstkey);
		server.dirty++;
	}
	else  { /* 结果集为空 */
		decrRefCount(dstobj);
		addReply(c, shared.czero);
	}

	zfree(src);
}

void zunionstoreCommand(redisClient *c) { /* 求并集 */
	zunionInterGenericCommand(c, c->argv[1], REDIS_OP_UNION);
}

void zinterstoreCommand(redisClient *c) { /* 求交集 */
	zunionInterGenericCommand(c, c->argv[1], REDIS_OP_INTER);
}

/*
 * 释放给定跳跃表，以及表中的所有节点
 *
 * T = O(N)
 */
void zslFree(zskiplist *zsl) {

	zskiplistNode *node = zsl->header->level[0].forward, *next;

	/* 释放表头 */
	zfree(zsl->header);

	/* 释放表中所有节点 */
	while (node) {

		next = node->level[0].forward;

		zslFreeNode(node);

		node = next;
	}

	/* 释放跳跃表结构 */
	zfree(zsl);
}