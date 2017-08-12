/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 */

#ifndef __AE_H__
#define __AE_H__

#include <time.h>

/* 事件执行状态 */
// 成功
#define AE_OK 0
// 出错
#define AE_ERR -1

/* 文件事件状态 */
// 未设置
#define AE_NONE 0
// 可读
#define AE_READABLE 1
// 可写
#define AE_WRITABLE 2

/* 时间处理器的执行 flags */
// 文件事件
#define AE_FILE_EVENTS 1
// 时间事件
#define AE_TIME_EVENTS 2
// 所有事件
#define AE_ALL_EVENTS (AE_FILE_EVENTS|AE_TIME_EVENTS)
// 不阻塞，也不进行等待
#define AE_DONT_WAIT 4

/* 决定时间事件是否要持续执行的 flag */

#define AE_NOMORE -1

/* Macros */
#define AE_NOTUSED(V) ((void) V)

/* 事件处理器状态 */
struct aeEventLoop; 

/* Types and data structures -- 事件接口 */
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

/* File event structure -- 文件事件结构 */

typedef struct aeFileEvent {

	// 监听事件类型掩码，
	// 值可以是 AE_READABLE 或 AE_WRITABLE,
	// 或者 AE_READABLE | AE_WRITABLE
	int mask;

	aeFileProc *rfileProc; // 读事件处理器

	aeFileProc *wfileProc; // 写事件处理器

	void *clientData; // 多路复用库的私有数据
	 
} aeFileEvent;

/* Time event structure -- 时间事件结构 */
typedef struct aeTimeEvent {

	long long id; // 时间事件的唯一标识符

	// 事件的到达时间
	long when_sec; // seconds
	long when_ms;  // milliseconds

	aeTimeProc *timeProc; // 事件处理函数

	aeEventFinalizerProc *finalizerProc; // 事件释放函数

	void *clientData; // 多路复用库的私有数据

	struct aeTimeEvent *next; // 指向下个时间事件结构，形成链表

} aeTimeEvent;

/* A fired event -- 已就绪事件 */
typedef struct aeFiredEvent { 

	// 已就绪文件描述符
	int fd;

	// 事件类型掩码，
	// 值可以是 AE_READABLE 或 AE_WRITABLE
	// 或者是两者的或
	int mask;

} aeFiredEvent;

/* State of an event based program -- 事件处理器的状态 */

typedef struct aeEventLoop { // 全局只需要一份EventLoop

	int maxfd;   // 目前已注册的最大描述符

	int setsize; // 目前已追踪的最大描述符

	long long timeEventNextId; // 用于生成时间事件 id

	time_t lastTime;     // 最后一次执行时间事件的时间

	aeFileEvent *events; // 已注册的文件事件

	aeFiredEvent *fired; // 已就绪的文件事件

	aeTimeEvent *timeEventHead; // 时间事件

	int stop; // 事件处理器的开关

	void *apidata; // 多路复用库的私有数据, 一般用于存放aeApiState对象的指针,
	// 而aeApiState有着epoll_events结构的一个数组

	aeBeforeSleepProc *beforesleep; // 在处理事件前要执行的函数

} aeEventLoop;

/* Prototypes */
aeEventLoop *aeCreateEventLoop(int setsize);
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask, aeFileProc *proc, void *clientData);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds, aeTimeProc *proc, void *clientData,
	aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
void aeMain(aeEventLoop *eventLoop);
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);
void aeDeleteEventLoop(aeEventLoop *eventLoop);
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);
#endif
