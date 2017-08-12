/* 这个文件主要用于实现事务的机制. */
#include "redis.h"
#include "multi.h"
#include "networking.h"
#include "ziplist.h"
#include "t_list.h"
#include "dict.h"
#include "db.h"
#include "ziplist.h"
#include "t_string.h"
#include "networking.h"
#include "object.h"
#include "util.h"

extern struct sharedObjectsStruct shared;
extern struct redisServer server;
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


void multiCommand(redisClient *c) { /* 开启事务的功能 */
	/* 不能在事务中嵌套事务 */
	if (c->flags & REDIS_MULTI) {
		addReplyError(c, "MULTI calls can not be nested");
		return;
	}

	/* 打开事务 FLAG */
	c->flags |= REDIS_MULTI;
	addReply(c, shared.ok);
}

/* 
 * 释放所有事务状态相关的资源
 */
void freeClientMultiState(redisClient *c) {
	int j;

	/* 遍历事务队列 */
	for (j = 0; j < c->mstate.count; j++) {
		int i;
		multiCmd *mc = c->mstate.commands + j;

		/* 释放所有命令参数 */
		for (i = 0; i < mc->argc; i++)
			decrRefCount(mc->argv[i]);

		/* 释放参数数组本身 */
		zfree(mc->argv);
	}

	/* 释放事务队列 */
	zfree(c->mstate.commands);
}

/* 
 * 将一个新命令添加到事务队列中
 */
void queueMultiCommand(redisClient *c) {
	multiCmd *mc;
	int j;

	/* 为新数组元素分配空间 */
	c->mstate.commands = zrealloc(c->mstate.commands,
		sizeof(multiCmd)*(c->mstate.count + 1));

	/* 指向新元素 */
	mc = c->mstate.commands + c->mstate.count;

	/* 设置事务的命令、命令参数数量，以及命令的参数 */
	mc->cmd = c->cmd;
	mc->argc = c->argc;
	mc->argv = zmalloc(sizeof(robj*)*c->argc);
	memcpy(mc->argv, c->argv, sizeof(robj*)*c->argc);
	for (j = 0; j < c->argc; j++)
		incrRefCount(mc->argv[j]);
	/* 事务命令数量计数器增一 */
	c->mstate.count++;
}

typedef struct watchedKey {
	/* 被监视的键 */
	robj *key;
	/* 键所在的数据库 */
	redisDb *db;
} watchedKey;


/* 
 * 取消客户端对所有键的监视。
 *
 * 清除客户端事务状态的任务由调用者执行。
 */
void unwatchAllKeys(redisClient *c) {
	listIter li;
	listNode *ln;

	/* 没有键被监视，直接返回 */
	if (listLength(c->watched_keys) == 0) return;

	/* 遍历链表中所有被客户端监视的键 */
	listRewind(c->watched_keys, &li);
	while ((ln = listNext(&li))) {
		list *clients;
		watchedKey *wk;

		/* 从数据库的 watched_keys 字典的 key 键中
		 * 删除链表里包含的客户端节点 */
		wk = listNodeValue(ln);
		/* 取出客户端链表 */
		clients = dictFetchValue(wk->db->watched_keys, wk->key);
		assert(clients != NULL);
		/* 删除链表中的客户端节点 */
		listDelNode(clients, listSearchKey(clients, c));

		/* 如果链表已经被清空，那么删除这个键 */
		if (listLength(clients) == 0)
			dictDelete(wk->db->watched_keys, wk->key);

		/* 从链表中移除 key 节点 */
		listDelNode(c->watched_keys, ln);

		decrRefCount(wk->key);
		zfree(wk);
	}
}


void discardTransaction(redisClient *c) {
	/* 重置事务状态 */
	freeClientMultiState(c);
	initClientMultiState(c);
	/* 屏蔽事务状态 */
	c->flags &= ~(REDIS_MULTI | REDIS_DIRTY_CAS | REDIS_DIRTY_EXEC);
	/* 取消对所有键的监视 */
	unwatchAllKeys(c);
}

/* 
 * 向 AOF 文件传播 MULTI 命令。
 */
void execCommandPropagateMulti(redisClient *c) {
	robj *multistring = createStringObject("MULTI", 5);
	propagate(server.multiCommand, c->db->id, &multistring, 1,
		REDIS_PROPAGATE_AOF);
	decrRefCount(multistring);
}

void execCommand(redisClient *c) { /* 相当于提交事务,要么干,要么全部撤销 */
	int j;
	robj **orig_argv;
	int orig_argc;
	struct redisCommand *orig_cmd;
	int must_propagate = 0; /* Need to propagate MULTI/EXEC to AOF / slaves? */

	/* 客户端没有执行事务 */
	if (!(c->flags & REDIS_MULTI)) {
		addReplyError(c, "EXEC without MULTI");
		return;
	}

	/* 
	* 检查是否需要阻止事务执行，因为：
	*
	* 1) Some WATCHed key was touched.
	*    有被监视的键已经被修改了
	*
	* 2) There was a previous error while queueing commands.
	*    命令在入队时发生错误
	*    （注意这个行为是 2.6.4 以后才修改的，之前是静默处理入队出错命令）
	*
	* 第一种情况返回多个批量回复的空对象
	* 而第二种情况则返回一个 EXECABORT 错误
	*/
	if (c->flags & (REDIS_DIRTY_CAS | REDIS_DIRTY_EXEC)) {
		addReply(c, c->flags & REDIS_DIRTY_EXEC ? shared.execaborterr :
			shared.nullmultibulk);
		/* 取消事务 */
		discardTransaction(c);
		goto handle_monitor;
	}

	/* Exec all the queued commands */
	/* 已经可以保证安全性了，取消客户端对所有键的监视 */
	unwatchAllKeys(c); /* Unwatch ASAP otherwise we'll waste CPU cycles */

	/* 因为事务中的命令在执行时可能会修改命令和命令的参数
	 * 所以为了正确地传播命令，需要现备份这些命令和参数 */
	orig_argv = c->argv;
	orig_argc = c->argc;
	orig_cmd = c->cmd;

	addReplyMultiBulkLen(c, c->mstate.count);

	/* 执行事务中的命令 */
	for (j = 0; j < c->mstate.count; j++) {

		/* 因为 Redis 的命令必须在客户端的上下文中执行
		 * 所以要将事务队列中的命令、命令参数等设置给客户端 */
		c->argc = c->mstate.commands[j].argc;
		c->argv = c->mstate.commands[j].argv;
		c->cmd = c->mstate.commands[j].cmd;


		/*
		* 当遇上第一个写命令时，传播 MULTI 命令。
		*
		* 这可以确保服务器和 AOF 文件的数据一致性。
		*/
		if (!must_propagate && !(c->cmd->flags & REDIS_CMD_READONLY)) {

			/* 传播 MULTI 命令 */
			execCommandPropagateMulti(c);

			/* 计数器，只发送一次 */
			must_propagate = 1;
		}

		/* 执行命令 */
		call(c, REDIS_CALL_FULL);

		/* 因为执行后命令、命令参数可能会被改变
		 * 比如 SPOP 会被改写为 SREM
		 * 所以这里需要更新事务队列中的命令和参数
		 * 确保附属节点和 AOF 的数据一致性 */
		c->mstate.commands[j].argc = c->argc;
		c->mstate.commands[j].argv = c->argv;
		c->mstate.commands[j].cmd = c->cmd;
	}

	/* 还原命令、命令参数 */
	c->argv = orig_argv;
	c->argc = orig_argc;
	c->cmd = orig_cmd;

	/* 清理事务状态 */
	discardTransaction(c);

	/* Make sure the EXEC command will be propagated as well if MULTI
	* was already propagated. */
	/* 将服务器设为脏，确保 EXEC 命令也会被传播 */
	if (must_propagate) server.dirty++;

handle_monitor:
	return;
}

/*
 * 让客户端 c 监视给定的键 key
 */
void watchForKey(redisClient *c, robj *key) {
	list *clients = NULL;
	listIter li;
	listNode *ln;
	watchedKey *wk;

	/* 检查 key 是否已经保存在 watched_keys 链表中，
	 * 如果是的话，直接返回 */
	listRewind(c->watched_keys, &li);
	while ((ln = listNext(&li))) {
		wk = listNodeValue(ln);
		if (wk->db == c->db && equalStringObjects(key, wk->key))
			return; /* Key already watched */
	}

	/* 键不存在于 watched_keys ，添加它 */

	// 以下是一个 key 不存在于字典的例子：
	// before :
	// {
	//  'key-1' : [c1, c2, c3],
	//  'key-2' : [c1, c2],
	// }
	// after c-10086 WATCH key-1 and key-3:
	// {
	//  'key-1' : [c1, c2, c3, c-10086],
	//  'key-2' : [c1, c2],
	//  'key-3' : [c-10086]
	// }

	/* 检查 key 是否存在于数据库的 watched_keys 字典中 */
	clients = dictFetchValue(c->db->watched_keys, key);
	/* 如果不存在的话，添加它 */
	if (!clients) {
		/* 值为链表 */
		clients = listCreate();
		/* 关联键值对到字典 */
		dictAdd(c->db->watched_keys, key, clients);
		incrRefCount(key);
	}
	/* 将客户端添加到链表的末尾 */
	listAddNodeTail(clients, c);

	// 将新 watchedKey 结构添加到客户端 watched_keys 链表的表尾
	// 以下是一个添加 watchedKey 结构的例子
	// before:
	// [
	//  {
	//   'key': 'key-1',
	//   'db' : 0
	//  }
	// ]
	// after client watch key-123321 in db 0:
	// [
	//  {
	//   'key': 'key-1',
	//   'db' : 0
	//  }
	//  ,
	//  {
	//   'key': 'key-123321',
	//   'db': 0
	//  }
	// ]
	wk = zmalloc(sizeof(*wk));
	wk->key = key;
	wk->db = c->db;
	incrRefCount(key);
	listAddNodeTail(c->watched_keys, wk); /* 添加到尾部 */
}

/* 
 * “触碰”一个键，如果这个键正在被某个/某些客户端监视着，
 * 那么这个/这些客户端在执行 EXEC 时事务将失败。
 */
void touchWatchedKey(redisDb *db, robj *key) {
	list *clients;
	listIter li;
	listNode *ln;
	 
	/* 字典为空，没有任何键被监视 */
	if (dictSize(db->watched_keys) == 0) return;

	/* 获取所有监视这个键的客户端 */
	clients = dictFetchValue(db->watched_keys, key);
	if (!clients) return;

	/* Mark all the clients watching this key as REDIS_DIRTY_CAS */
	/* Check if we are already watching for this key */
	/* 遍历所有客户端，打开他们的 REDIS_DIRTY_CAS 标识 */
	listRewind(clients, &li);
	while ((ln = listNext(&li))) {
		redisClient *c = listNodeValue(ln);

		c->flags |= REDIS_DIRTY_CAS;
	}
}

void watchCommand(redisClient *c) {
	int j;
	/* 不能在事务开始后执行 */
	if (c->flags & REDIS_MULTI) {
		addReplyError(c, "WATCH inside MULTI is not allowed");
		return;
	}

	/* 监视输入的任意个键 */
	for (j = 1; j < c->argc; j++)
		watchForKey(c, c->argv[j]);
	addReply(c, shared.ok);
}

void unwatchCommand(redisClient *c) {
	/* 取消客户端对所有键的监视 */
	unwatchAllKeys(c);
	/* 重置状态 */
	c->flags &= (~REDIS_DIRTY_CAS);
	addReply(c, shared.ok);
}

void discardCommand(redisClient *c) {

	/* 不能在客户端未进行事务状态之前使用 */
	if (!(c->flags & REDIS_MULTI)) {
		addReplyError(c, "DISCARD without MULTI");
		return;
	}
	discardTransaction(c);
	addReply(c, shared.ok);
}

/* 
 * 将事务状态设为 DIRTY_EXEC ，让之后的 EXEC 命令失败。
 *
 * 每次在入队命令出错时调用
 */
void flagTransaction(redisClient *c) {
	if (c->flags & REDIS_MULTI)
		c->flags |= REDIS_DIRTY_EXEC;
}