#include "redis.h"
#include "ziplist.h"
#include "t_list.h"
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
void decrRefCountVoid(void *o);
robj *getDecodedObject(robj *o);


/*
 * 返回entry结构当前所保存的列表节点
 * 如果entry没有记录任何节点,那么返回NULL
 */
robj *listTypeGet(listTypeEntry *entry) {
	listTypeIterator *li = entry->li;

	robj *value = NULL;
	/* 根据索引,从ZIPLIST中取出节点的值 */
	if (li->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *vstr;
		unsigned int vlen;
		long long vlong;
		assert(entry->zi != NULL);
		if (ziplistGet(entry->zi, &vstr, &vlen, &vlong)) {
			if (vstr)
				value = createStringObject((char *)vstr, vlen);
			else
				value = createStringObjectFromLongLong(vlong);
		}
		else if (li->encoding == REDIS_ENCODING_LINKEDLIST) {
			assert(entry->ln != NULL);
			value = listNodeValue(entry->ln);
			incrRefCount(value);
		}
		else
			assert(0);
	}
	return value;
}

/*
 * 返回列表的节点数量
 */
unsigned long listTypeLength(robj *subject) {
	if (subject->encoding == REDIS_ENCODING_ZIPLIST)
		return ziplistLen(subject->ptr);
	else if (subject->encoding == REDIS_ENCODING_LINKEDLIST)
		return listLength((list*)subject->ptr);
	else
		assert(0);
}

/*-----------------------------------------------------------------------------
* List Commands
*----------------------------------------------------------------------------*/

/* 
 * 创建并返回一个列表迭代器。
 *
 * 参数 index 决定开始迭代的列表索引。
 *
 * 参数 direction 则决定了迭代的方向。
 *
 * listTypeIterator 于 redis.h 文件中定义。
 */
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction) {

	listTypeIterator *li = zmalloc(sizeof(listTypeIterator));

	li->subject = subject;

	li->encoding = subject->encoding;

	li->direction = direction;
	if (li->encoding == REDIS_ENCODING_ZIPLIST) { /* ZIPLIST */
		li->zi = ziplistIndex(subject->ptr, index);
	}
	else if (li->encoding == REDIS_ENCODING_LINKEDLIST) { /* 双端链表 */
		li->ln = listIndex(subject->ptr, index);
	}
	else
		assert(0);
	return li;
}

/*
 * 释放迭代器
 */
void listTypeReleaseIterator(listTypeIterator *li) {
	zfree(li);
}

/*
 * 使用entry结构记录迭代器当前指向的节点,并将迭代器的指针移动到下一个元素
 */
int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
	assert(li->subject->encoding == li->encoding);
	entry->li = li;
	if (li->encoding == REDIS_ENCODING_ZIPLIST) {
		/* 记录当前节点到entry */
		entry->zi = li->zi;
		if (entry->zi != NULL) {
			if (li->direction == REDIS_TAIL)
				li->zi = ziplistNext(li->subject->ptr, li->zi);
			else
				li->zi = ziplistPrev(li->subject->ptr, li->zi);
			return 1;
		}
	}
	else if (li->encoding == REDIS_ENCODING_LINKEDLIST) {
		/* 记录当前节点到entry */
		entry->ln = li->ln;
		/* 移动迭代器的指针 */
		if (entry->ln != NULL) {
			if (li->direction == REDIS_TAIL)
				li->ln = li->ln->next;
			else
				li->ln = li->ln->prev;
			return 1;
		}
	}
	else
		assert(0);
	/* 列表元素已经全部迭代完毕 */
	return 0;
}

/*
 * 将类表的底层编码从ziplist转换成双端链表
 */
void listTypeConvert(robj *subject, int enc) {
	listTypeIterator *li;
	listTypeEntry entry;
	/* 转换成双端链表 */
	if (enc = REDIS_ENCODING_LINKEDLIST) {
		list *l = listCreate();
		listSetFreeMethod(l, decrRefCountVoid);

		/* 遍历ziplist,并将里面的值全部添加到双端链表中 */
		li = listTypeInitIterator(subject, 0, REDIS_TAIL);
		while (listTypeNext(li, &entry)) listAddNodeTail(l, listTypeGet(&entry));
		listTypeReleaseIterator(li);
		/* 更新编码 */
		subject->encoding = REDIS_ENCODING_LINKEDLIST;
		/* 释放原来的ziplist */
		zfree(subject->ptr);
		/* 更新对象值指针 */
		subject->ptr = l;
	}
	else
		assert(0);
}
/* 
 * 对输入值 value 进行检查，看是否需要将 subject 从 ziplist 转换为双端链表，
 * 以便保存值 value 。
 *
 * 函数只对 REDIS_ENCODING_RAW 编码的 value 进行检查，
 * 因为整数编码的值不可能超长。
 */
void listTypeTryConversion(robj *subject, robj *value) {

	/* 确保 subject 为 ZIPLIST 编码 */
	if (subject->encoding != REDIS_ENCODING_ZIPLIST) return;

	if (sdsEncodedObject(value) &&
		/* 看字符串是否过长 */
		sdslen(value->ptr) > server.list_max_ziplist_value)
		/* 将编码转换为双端链表 */
		listTypeConvert(subject, REDIS_ENCODING_LINKEDLIST);
}

/* 
 * 将给定元素添加到列表的表头或表尾。
 *
 * 参数 where 决定了新元素添加的位置：
 *
 *  - REDIS_HEAD 将新元素添加到表头
 *
 *  - REDIS_TAIL 将新元素添加到表尾
 *
 * 调用者无须担心 value 的引用计数，因为这个函数会负责这方面的工作。
 */
void listTypePush(robj *subject, robj *value, int where) {
	/* 是否需要转换编码？ */
	listTypeTryConversion(subject, value);

	if (subject->encoding == REDIS_ENCODING_ZIPLIST &&
		ziplistLen(subject->ptr) >= server.list_max_ziplist_entries)
		listTypeConvert(subject, REDIS_ENCODING_LINKEDLIST);

	if (subject->encoding == REDIS_ENCODING_ZIPLIST) {
		int pos = (where == REDIS_HEAD) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
		/* 取出对象的值，因为 ZIPLIST 只能保存字符串或整数 */
		value = getDecodedObject(value);
		subject->ptr = ziplistPush(subject->ptr, value->ptr, sdslen(value->ptr), pos);
		decrRefCount(value);
	}
	else if (subject->encoding == REDIS_ENCODING_LINKEDLIST) { /* 双端链表 */
		if (where == REDIS_HEAD) {
			listAddNodeHead(subject->ptr, value);
		}
		else {
			listAddNodeTail(subject->ptr, value);
		}
		incrRefCount(value);
	}
	else 
		assert(0);
}

void pushGenericCommand(redisClient *c, int where) {

	int j, waiting = 0, pushed = 0;
	/* 取出列表对象 */
	robj *lobj = lookupKeyWrite(c->db, c->argv[1]);
	/* 如果列表对象不存在，那么可能有客户端在等待这个键的出现 */
	int may_have_waiting_clients = (lobj == NULL);

	if (lobj && lobj->type != REDIS_LIST) {
		addReply(c, shared.wrongtypeerr);
		return;
	}

	/* 遍历所有输入值，并将它们添加到列表中 */
	for (j = 2; j < c->argc; j++) {

		/* 编码值 */
		c->argv[j] = tryObjectEncoding(c->argv[j]);

		/* 如果列表对象不存在，那么创建一个，并关联到数据库 */
		if (!lobj) {
			lobj = createZiplistObject();
			dbAdd(c->db, c->argv[1], lobj);
		}

		/* 将值推入到列表 */
		listTypePush(lobj, c->argv[j], where);
		pushed++;
	}

	/* 返回添加的节点数量 */
	addReplyLongLong(c, waiting + (lobj ? listTypeLength(lobj) : 0));

	// 如果至少有一个元素被成功推入，那么执行以下代码
	if (pushed) {
		char *event = (where == REDIS_HEAD) ? "lpush" : "rpush";
	}
}

void lpushCommand(redisClient *c) {
	pushGenericCommand(c, REDIS_HEAD);
}

void rpushCommand(redisClient *c) {
	pushGenericCommand(c, REDIS_TAIL);
}

/*
 * 从列表的表头或表尾中弹出一个元素。
 *
 * 参数 where 决定了弹出元素的位置：
 *
 *  - REDIS_HEAD 从表头弹出
 *
 *  - REDIS_TAIL 从表尾弹出
 */
robj *listTypePop(robj *subject, int where) {
	robj *value = NULL;
	
	if (subject->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *p;
		unsigned char *vstr;
		unsigned int vlen;
		long long vlong;

		/* 决定弹出元素的位置 */
		int pos = (where == REDIS_HEAD) ? 0 : -1;
		p = ziplistIndex(subject->ptr, pos);
		if (ziplistGet(p, &vstr, &vlen, &vlong)) {
			/* 为被弹出的元素创建对象 */
			if (vstr) {
				value = createStringObject((char*)vstr, vlen);
			}
			else {
				value = createStringObjectFromLongLong(vlong);
			}
			/* 从 ziplist 中删除被弹出元素 */
			subject->ptr = ziplistDelete(subject->ptr, &p);
		}
	}
	else if (subject->encoding == REDIS_ENCODING_LINKEDLIST) {
		list *list = subject->ptr;
		listNode *ln;
		if (where == REDIS_HEAD) {
			ln = listFirst(list);
		}
		else {
			ln = listLast(list);
		}
		/* 删除被弹出的节点 */
		if (ln != NULL) {
			value = listNodeValue(ln);
			incrRefCount(value);
			listDelNode(list, ln);
		}
	}
	else
		assert(0);
	return value;
}


void popGenericCommand(redisClient *c, int where) {
	/* 取出列表对象 */
	robj *o = lookupKeyWriteOrReply(c, c->argv[1], shared.nullbulk);
	if (o == NULL || checkType(c, o, REDIS_LIST)) return;
	/* 根据弹出的元素是否为空,决定后续动作 */
	robj *value = listTypePop(o, where);
	if (value == NULL) {
		addReply(c, shared.nullbulk);
	}
	else {
		char *event = (where == REDIS_HEAD) ? "lpop" : "rpop";
		char* str1 = value->ptr;
		addReplyBulk(c, value);
		decrRefCount(value);
		if (listTypeLength(o) == 0) {
			dbDelete(c->db, c->argv[1]);
		}
	}
}

void lpopCommand(redisClient *c) {
	popGenericCommand(c, REDIS_HEAD);
}

void rpopCommand(redisClient *c) {
	popGenericCommand(c, REDIS_TAIL);
}


void lindexCommand(redisClient *c) {
	robj *o = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk);
	if (o == NULL || checkType(c, o, REDIS_LIST)) return;
	long long index;
	robj *value = NULL;

	/* 取出整数值对象 index */
	if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != REDIS_OK))
		return;

	/* 根据索引，遍历 ziplist ，直到指定位置 */
	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *p;
		unsigned char *vstr;
		unsigned int vlen;
		long long vlong;

		p = ziplistIndex(o->ptr, index);

		if (ziplistGet(p, &vstr, &vlen, &vlong)) {
			if (vstr) {
				value = createStringObject((char*)vstr, vlen);
			}
			else {
				value = createStringObjectFromLongLong(vlong);
			}
			addReplyBulk(c, value);
			decrRefCount(value);
		}
		else {
			addReply(c, shared.nullbulk);
		}
	}
	else if (o->encoding == REDIS_ENCODING_LINKEDLIST) { /* 根据索引，遍历双端链表，直到指定位置 */
		listNode *ln = listIndex(o->ptr, index);
		if (ln != NULL) {
			value = listNodeValue(ln);
			addReplyBulk(c, value);
		}
		else {
			addReply(c, shared.nullbulk);
		}
	}
	else 
		assert(0);
}

void llenCommand(redisClient *c) {
	robj *o = lookupKeyReadOrReply(c, c->argv[1], shared.czero);
	if (o == NULL || checkType(c, o, REDIS_LIST)) return;
	addReplyLongLong(c, listTypeLength(o));
}

void lsetCommand(redisClient *c) {
	/* 取出列表对象 */
	robj *o = lookupKeyWriteOrReply(c, c->argv[1], shared.nokeyerr);
	if (o == NULL || checkType(c, o, REDIS_LIST)) return;
	long long index;
	/* 取出值对象 */
	robj *value = (c->argv[3] = tryObjectEncoding(c->argv[3]));
	/* 取出整数值对象index */
	if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != REDIS_OK))
		return;
	/* 查看保存value值是否需要转换列表的底层编码 */
	listTypeTryConversion(o, value);
	/* 设置到ziplist */
	if (o->encoding == REDIS_ENCODING_ZIPLIST) {
		unsigned char *p, *zl = o->ptr;
		/* 查找索引 */
		p = ziplistIndex(zl, index);
		if (p == NULL)
			addReply(c, shared.outofrangeerr);
		else {
			/* 删除现有的值 */
			o->ptr = ziplistDelete(o->ptr, &p);
			/* 插入新值到指定索引 */
			value = getDecodedObject(value);
			o->ptr = ziplistInsert(o->ptr, p, value->ptr, sdslen(value->ptr));
			decrRefCount(value);

			addReply(c, shared.ok);
		}
	}
	else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
		listNode *ln = listIndex(o->ptr, index);

		if (ln == NULL) {
			addReply(c, shared.outofrangeerr);
		}
		else {
			/* 删除旧值对象 */
			decrRefCount((robj*)listNodeValue(ln));
			/* 指向新对象 */
			listNodeValue(ln) = value;
			incrRefCount(value);

			addReply(c, shared.ok);
		}
	}
	else
		assert(0);
}