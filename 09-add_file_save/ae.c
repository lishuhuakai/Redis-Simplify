/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse. */

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include "ae.h"
#include "zmalloc.h"
#include "aeepoll.h"

/*
 * 删除事件处理器
 */
void aeDeleteEventLoop(aeEventLoop *eventLoop) {
	aeApiFree(eventLoop);
	zfree(eventLoop->events);
	zfree(eventLoop->fired);
	zfree(eventLoop);
}

/*
 * 设置处理事件前需要被执行的函数
 */
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
	eventLoop->beforesleep = beforesleep;
}

/*
 * 取出当前时间的秒和毫秒，
 * 并分别将它们保存到 seconds 和 milliseconds 参数中
 */
static void aeGetTime(long *seconds, long *milliseconds)
{
	struct timeval tv;

	gettimeofday(&tv, NULL);
	*seconds = tv.tv_sec;
	*milliseconds = tv.tv_usec / 1000;
}

/*
 * 初始化事件处理器状态
 */
aeEventLoop *aeCreateEventLoop(int setsize) { // 创建一个EventLoop,我只是稍微有那么点好奇,究竟这些个玩意到底是怎么实现的.
	aeEventLoop *eventLoop;
	int i;

	// 创建事件状态结构
	if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err; // 大量使用goto语句

	/* 初始化文件事件结构和已就绪文件事件结构数组 */
	eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);
	eventLoop->fired = zmalloc(sizeof(aeFiredEvent)*setsize);
	if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;
	// 设置数组大小
	eventLoop->setsize = setsize;
	// 初始化执行最近一次执行时间
	eventLoop->lastTime = time(NULL); // 获得当前的时间

	// 初始化时间事件结构
	eventLoop->timeEventHead = NULL;
	eventLoop->timeEventNextId = 0; // 这个量随着时间事件的增加而增加

	eventLoop->stop = 0;
	eventLoop->maxfd = -1;
	eventLoop->beforesleep = NULL;
	if (aeApiCreate(eventLoop) == -1) goto err;

	// Events with mask == AE_NONE are not set. So let's initialize the
	// vector with it.
	// 初始化监听事件
	for (i = 0; i < setsize; i++)
		eventLoop->events[i].mask = AE_NONE; // 表示不监听任何的事件

	// 返回事件循环
	return eventLoop;

err:
	if (eventLoop) {
		zfree(eventLoop->events);
		zfree(eventLoop->fired);
		zfree(eventLoop);
	}
	return NULL;
}

/*
 * 时间量的加法,在当前时间上增加 millliseconds, 
 * 将其记录在sec和ms两个变量中.
 */
static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms) {
	long cur_sec, cur_ms, when_sec, when_ms;
	aeGetTime(&cur_sec, &cur_ms);

	// 计算增加milliseconds之后的秒数和毫秒数
	when_sec = cur_sec + milliseconds / 1000;
 	when_ms = cur_ms + milliseconds % 1000;

	// 进位：
	// 如果 when_ms 大于等于 1000
	// 那么将 when_sec 增大一秒
	if (when_ms >= 1000) {
		when_sec++;
		when_ms -= 1000;
	}

	*sec = when_sec;
	*ms = when_ms;
}



/*
 * 创建时间事件
 */
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
	aeTimeProc *proc, void *clientData,
	aeEventFinalizerProc *finalizerProc)
{
	long long id = eventLoop->timeEventNextId++; // 更新时间计数器

	aeTimeEvent *te; // 创建时间事件结构

	te = zmalloc(sizeof(*te));
	if (te == NULL) return AE_ERR;

	te->id = id; // 设置 ID 

	aeAddMillisecondsToNow(milliseconds, &te->when_sec, &te->when_ms); // 设定处理事件的时间

	// 设置事件处理器
	te->timeProc = proc;
	te->finalizerProc = finalizerProc;
	
	te->clientData = clientData; // 设置私有数据

	
	te->next = eventLoop->timeEventHead; // 将新事件放入表头
	eventLoop->timeEventHead = te;

	return id;
}

/* 
 * 寻找里目前时间最近的时间事件
 * 因为链表是乱序的，所以查找复杂度为 O（N）
 */
static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop)
{
	aeTimeEvent *te = eventLoop->timeEventHead;
	aeTimeEvent *nearest = NULL;

	while (te) {
		if (!nearest || te->when_sec < nearest->when_sec ||
			(te->when_sec == nearest->when_sec &&
				te->when_ms < nearest->when_ms))
			nearest = te;
		te = te->next;
	}
	return nearest;
}

/*
 * 根据 mask 参数的值，监听 fd 文件的状态,
 * 当 fd 可用时，执行 proc 函数
 */
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
	aeFileProc *proc, void *clientData)
{
	if (fd >= eventLoop->setsize) { //  fd不宜过大,超越了极限,便会出错.
		errno = ERANGE; // ERANGE表示超出界限
		return AE_ERR;
	}

	if (fd >= eventLoop->setsize) return AE_ERR;

	// 取出文件事件结构
	aeFileEvent *fe = &eventLoop->events[fd];

	// 监听指定 fd 的指定事件
	if (aeApiAddEvent(eventLoop, fd, mask) == -1)
		return AE_ERR;

	// 设置文件事件类型，以及事件的处理器
	fe->mask |= mask;
	if (mask & AE_READABLE) fe->rfileProc = proc;
	if (mask & AE_WRITABLE) fe->wfileProc = proc;

	// 私有数据
	fe->clientData = clientData; // clientData是什么玩意?

	// 如果有需要，更新事件处理器的最大fd
	if (fd > eventLoop->maxfd)
		eventLoop->maxfd = fd;

	return AE_OK;
}

/*
 * 删除给定 id 的时间事件
 */
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
	aeTimeEvent *te, *prev = NULL;

	// 遍历链表
	te = eventLoop->timeEventHead;
	while (te) {

		// 发现目标事件，删除
		if (te->id == id) {

			if (prev == NULL)
				eventLoop->timeEventHead = te->next;
			else
				prev->next = te->next;

			// 执行清理处理器
			if (te->finalizerProc)
				te->finalizerProc(eventLoop, te->clientData);

			// 释放时间事件
			zfree(te);

			return AE_OK;
		}
		prev = te;
		te = te->next;
	}

	return AE_ERR; // NO event with the specified ID found
}

/* Process time events
 *
 * 处理所有已到达的时间事件
 */
static int processTimeEvents(aeEventLoop *eventLoop) {
	int processed = 0;
	aeTimeEvent *te;
	long long maxId;
	time_t now = time(NULL); // 获得当前的时间

	// 通过重置事件的运行时间,防止因时间穿插（skew）而造成的事件处理混乱
	if (now < eventLoop->lastTime) {
		te = eventLoop->timeEventHead;
		while (te) {
			te->when_sec = 0;
			te = te->next;
		}
	}
	eventLoop->lastTime = now; // 更新最后一次处理时间事件的时间

	// 遍历链表, 执行那些已经到达的事件
	te = eventLoop->timeEventHead;
	maxId = eventLoop->timeEventNextId - 1; // 最大的时间事件的编号
	while (te) {
		long now_sec, now_ms;
		long long id;

		// 获取当前时间
		aeGetTime(&now_sec, &now_ms);

		// 如果当前时间等于或等于事件的执行时间，那么说明事件已到达，执行这个事件
		if (now_sec > te->when_sec ||
			(now_sec == te->when_sec && now_ms >= te->when_ms))
		{
			int retval;

			id = te->id;
			// 执行事件处理器，并获取返回值,只要该返回值不是-1,代表它下次还要执行
			// 比较典型的redis.c中serverCron函数,它每次都返回一个固定的量,表示
			// 它会定期运行
			retval = te->timeProc(eventLoop, id, te->clientData);
			processed++;
			
			// 记录是否有需要循环执行这个事件时间
			if (retval != AE_NOMORE) {
				// retval 毫秒之后继续执行这个时间事件
				aeAddMillisecondsToNow(retval, &te->when_sec, &te->when_ms);
			}
			else {
				// 将这个事件删除
				aeDeleteTimeEvent(eventLoop, id);
			}

			// 因为执行事件之后，事件列表可能已经被改变了 
			// 因此需要将 te 放回表头，继续开始执行事件
			te = eventLoop->timeEventHead;
		}
		else {
			te = te->next;
		}
	}
	return processed;
}

/*
 * 处理所有已到达的时间事件，以及所有已就绪的文件事件。
 *
 * 如果不传入特殊 flags 的话，那么函数睡眠直到文件事件就绪，
 * 或者下个时间事件到达（如果有的话）。
 *
 * 如果 flags 为 0,那么函数不作动作,直接返回。
 * 如果 flags 包含 AE_ALL_EVENTS,所有类型的事件都会被处理。
 * 如果 flags 包含 AE_FILE_EVENTS,那么处理文件事件。
 * 如果 flags 包含 AE_TIME_EVENTS,那么处理时间事件。
 * 如果 flags 包含 AE_DONT_WAIT,
 * 那么函数在处理完所有不许阻塞的事件之后，即刻返回。
 *
 * 函数的返回值为已处理事件的数量
 */
int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
	int processed = 0, numevents;
	int j;

	// Nothing to do? return ASAP
	// AE_TIME_EVENTS代表时间事件,AE_FILE_EVENTS代表文件事件
	if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

	if (eventLoop->maxfd != -1 ||
		((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) { // 需要处理时间事件
		int j;
		aeTimeEvent *shortest = NULL;
		struct timeval tv, *tvp;

		// 获取最近的时间事件(即即将发生的事件)
		if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
			shortest = aeSearchNearestTimer(eventLoop);
		if (shortest) {
			// 如果时间事件存在的话
			// 那么根据最近可执行时间事件和现在时间的时间差来决定文件事件的阻塞时间
			long now_sec, now_ms;

			// 计算距今最近的时间事件还要多久才能达到
			// 并将该时间距保存在 tv 结构中
			// 并在下面的aeApiPoll函数中等待该事件的到来
			aeGetTime(&now_sec, &now_ms);
			tvp = &tv;
			tvp->tv_sec = shortest->when_sec - now_sec;
			if (shortest->when_ms < now_ms) {
				tvp->tv_usec = ((shortest->when_ms + 1000) - now_ms) * 1000;
				tvp->tv_sec--;
			}
			else {
				tvp->tv_usec = (shortest->when_ms - now_ms) * 1000;
			}

			// 时间差小于 0 ，说明事件已经可以执行了，将秒和毫秒设为 0 （不阻塞）
			if (tvp->tv_sec < 0) tvp->tv_sec = 0;
			if (tvp->tv_usec < 0) tvp->tv_usec = 0;
		}
		else {
			// 执行到这一步，说明没有时间事件
			// 那么根据 AE_DONT_WAIT 是否设置来决定是否阻塞，以及阻塞的时间长度
			if (flags & AE_DONT_WAIT) {
				// 设置文件事件不阻塞
				tv.tv_sec = tv.tv_usec = 0;
				tvp = &tv;
			}
			else {
				tvp = NULL; // 文件事件可以阻塞直到有事件到达为止
			}
		}

		// 处理文件事件，阻塞时间由 tvp 决定, 总之,如果有事件的话,一定要等到有事件发生才返回
		numevents = aeApiPoll(eventLoop , tvp);
		for (j = 0; j < numevents; j++) {
			// 从已就绪数组中 fired 获取已发生事件的信息,包括文件描述符fd,发生的事情mask
			aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];

			int mask = eventLoop->fired[j].mask;
			int fd = eventLoop->fired[j].fd;
			int rfired = 0;

			
			if (fe->mask & mask & AE_READABLE) { // 可读事件
				// rfired 确保读/写事件只能执行其中一个 
				rfired = 1;
				fe->rfileProc(eventLoop, fd, fe->clientData, mask);
			}
			
			if (fe->mask & mask & AE_WRITABLE) { // 写事件
				if (!rfired || fe->wfileProc != fe->rfileProc)
					fe->wfileProc(eventLoop, fd, fe->clientData, mask);
			}

			processed++;
		}
	}

	// 执行时间事件
	if (flags & AE_TIME_EVENTS)
		processed += processTimeEvents(eventLoop);

	return processed; // return the number of processed file/time events
}


/*
 * 事件处理器的主循环
 */
void aeMain(aeEventLoop *eventLoop) {

	eventLoop->stop = 0;

	while (!eventLoop->stop) {

		// 如果有需要在事件处理前执行的函数，那么运行它
		if (eventLoop->beforesleep != NULL)
			eventLoop->beforesleep(eventLoop);

		// 开始处理事件
		aeProcessEvents(eventLoop, AE_ALL_EVENTS);
	}
}

/*
 * 将fd从mask指定的监听队列中删除
 */
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask) {
	if (fd >= eventLoop->setsize) return;

	// 取出文件事件结构
	aeFileEvent *fe = &eventLoop->events[fd];

	// 如果没有设置监听的事件类型,直接返回
	if (fe->mask == AE_NONE) return;

	// 计算新掩码
	fe->mask = fe->mask & (~mask);

	if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
		// 更新最大的fd
		int j;
		for (j = eventLoop->maxfd - 1; j >= 0; j--)
			if (eventLoop->events[j].mask != AE_NONE) break;
		eventLoop->maxfd = j;
	}
	// 取消对给定fd的给定事件的监听
	aeApiDelEvent(eventLoop, fd, mask);
}