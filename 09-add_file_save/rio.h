#ifndef __REDIS_RIO_H
#define __REDIS_RIO_H

#include <stdio.h>
#include <stdint.h>
#include "sds.h"

/*
* RIO API 接口和状态
*/
struct _rio { /* 这个玩意非常类似于cpp中的类,所以一门语言什么的,关键还是看你怎么去使用它啦. */

	/* Backend functions.
	* Since this functions do not tolerate short writes or reads the return
	* value is simplified to: zero on error, non zero on complete success. */
	size_t(*read)(struct _rio *, void *buf, size_t len);
	size_t(*write)(struct _rio *, const void *buf, size_t len);
	off_t(*tell)(struct _rio *);

	/* The update_cksum method if not NULL is used to compute the checksum of
	* all the data that was read or written so far. The method should be
	* designed so that can be called with the current checksum, and the buf
	* and len fields pointing to the new block of data to add to the checksum
	* computation. */
	/* 校验和计算函数，每次有写入/读取新数据时都要计算一次 */
	void(*update_cksum)(struct _rio *, const void *buf, size_t len);

	/* 当前校验和 */
	uint64_t cksum;

	/* number of bytes read or written */
	size_t processed_bytes;

	size_t max_processing_chunk; /* 单次读或者写一个块最大的大小 */

								 /* Backend-specific vars. */
	union {

		struct {
			/* 缓存指针 */
			sds ptr;
			/* 偏移量 */
			off_t pos;
		} buffer;

		struct {
			/* 被打开文件的指针 */
			FILE *fp;
			/* 最近一次 fsync() 以来，写入的字节量 */
			off_t buffered;
			/* 写入多少字节之后，才会自动执行一次 fsync() */
			off_t autosync;
		} file;
	} io;
};

typedef struct _rio rio;


/*
* 将 buf 中的 len 字节写入到 r 中。
*
* 写入成功返回实际写入的字节数，写入失败返回 -1 。
*/
static inline size_t rioWrite(rio *r, const void *buf, size_t len) {
	while (len) {
		size_t bytes_to_write = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len; /* 一次要写入的数据大小 */
		if (r->update_cksum) r->update_cksum(r, buf, bytes_to_write);
		if (r->write(r, buf, bytes_to_write) == 0) /* 开始写入 */
			return 0;
		buf = (char*)buf + bytes_to_write; /* 指针后移 */
		len -= bytes_to_write;
		r->processed_bytes += bytes_to_write;
	}
	return 1; /* 如果写入成功的话,就返回1是吧. */
}

/*
* 从 r 中读取 len 字节，并将内容保存到 buf 中。
*
* 读取成功返回 1 ，失败返回 0 。
*/
static inline size_t rioRead(rio *r, void *buf, size_t len) {
	while (len) {
		size_t bytes_to_read = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
		if (r->read(r, buf, bytes_to_read) == 0)
			return 0;
		if (r->update_cksum) r->update_cksum(r, buf, bytes_to_read);
		buf = (char*)buf + bytes_to_read;
		len -= bytes_to_read;
		r->processed_bytes += bytes_to_read;
	}
	return 1;
}

/*
* 返回 r 的当前偏移量。
*/
static inline off_t rioTell(rio *r) {
	return r->tell(r);
}

void rioInitWithFile(rio *r, FILE *fp);
void rioInitWithBuffer(rio *r, sds s);

size_t rioWriteBulkCount(rio *r, char prefix, int count);
size_t rioWriteBulkString(rio *r, const char *buf, size_t len);
size_t rioWriteBulkLongLong(rio *r, long long l);
size_t rioWriteBulkDouble(rio *r, double d);

void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len);
void rioSetAutoSync(rio *r, off_t bytes);

#endif

