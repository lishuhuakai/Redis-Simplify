/* 
 * RIO 是一个可以面向流、可用于对多种不同的输入
 * （目前是文件和内存字节）进行编程的抽象。
 *
 * 比如说，RIO 可以同时对内存或文件中的 RDB 格式进行读写。
 *
 * 一个 RIO 对象提供以下方法：
 *
 *  read: read from stream.
 *        从流中读取
 *
 *  write: write to stream.
 *         写入到流中
 *
 *  tell: get the current offset.
 *        获取当前的偏移量
 *
 * 还可以通过设置 checksum 函数，计算写入或读取内容的校验和，
 * 或者为当前的校验和查询 rio 对象。
 */
#include "fmacros.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "rio.h"
#include "util.h"
#include "redis.h"
#include "crc64.h"


/*
 * 从文件 r 中读取 len 字节到 buf 中。
 *
 * 返回值为读取的字节数。
 */
static size_t rioFileRead(rio *r, void *buf, size_t len) {
	return fread(buf, len, 1, r->io.file.fp);
}

/*
 * 将长度为 len 的内容 buf 写入到文件 r 中。
 *
 * 成功返回 1 ，失败返回 0 。
 */
static size_t rioFileWrite(rio *r, const void *buf, size_t len) {
	size_t retval;

	retval = fwrite(buf, len, 1, r->io.file.fp);
	r->io.file.buffered += len;

	/* 检查写入的字节数，看是否需要执行自动 sync */
	if (r->io.file.autosync &&
		r->io.file.buffered >= r->io.file.autosync)
	{
		fflush(r->io.file.fp);
		fdatasync(fileno(r->io.file.fp)); /* 自动刷新一次 */
		r->io.file.buffered = 0;
	}
	return retval;
}

/* Returns read/write position in file.
 *
 * 返回文件当前的偏移量
 */
static off_t rioFileTell(rio *r) {
	return ftello(r->io.file.fp);
}

/*
 * 流为文件时所使用的结构
 */
static const rio rioFileIO = {
	/* 读函数 */
	rioFileRead,
	/* 写函数 */
	rioFileWrite,
	/* 偏移量函数 */
	rioFileTell,
	NULL,           /* update_checksum */
	0,              /* current checksum */
	0,              /* bytes read or written */
	0,              /* read/write chunk size */
	{ { NULL, 0 } } /* union for io-specific vars */
};

/*
 * 初始化文件流
 */
void rioInitWithFile(rio *r, FILE *fp) {
	*r = rioFileIO;
	r->io.file.fp = fp;
	r->io.file.buffered = 0;
	r->io.file.autosync = 0;
}

/*
 * 通用校验和计算函数
 */
void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len) {
	r->cksum = crc64(r->cksum, buf, len);
}

/*
 *
 * 每次通过 rio 写入 bytes 指定的字节数量时，执行一次自动的 fsync 。
 *
 * 默认情况下， bytes 被设为 0 ，表示不执行自动 fsync 。
 *
 * 这个函数是为了防止一次写入过多内容而设置的。
 *
 * 通过显示地、间隔性地调用 fsync ，
 * 可以将写入的 I/O 压力分担到多次 fsync 调用中。
 */
void rioSetAutoSync(rio *r, off_t bytes) {
	assert(r->read == rioFileIO.read);
	r->io.file.autosync = bytes;
}

/*
 * 以带 '\r\n' 后缀的形式写入字符串表示的 count 到 RIO
 *
 * 成功返回写入的数量，失败返回 0 。
 */
size_t rioWriteBulkCount(rio *r, char prefix, int count) {
	char cbuf[128];
	int clen;

	/* cbuf = prefix ++ count ++ '\r\n'
	 * 例如： *123\r\n */
	cbuf[0] = prefix;
	clen = 1 + ll2string(cbuf + 1, sizeof(cbuf) - 1, count);
	cbuf[clen++] = '\r';
	cbuf[clen++] = '\n';

	/* 写入 */
	if (rioWrite(r, cbuf, clen) == 0) return 0;

	/* 返回写入字节数 */
	return clen;
}


/* 
 * 以 "$<count>\r\n<payload>\r\n" 的形式写入二进制安全字符
 *
 * 例如 $3\r\nSET\r\n
 */
size_t rioWriteBulkString(rio *r, const char *buf, size_t len) {
	size_t nwritten;

	/* 写入 $<count>\r\n */
	if ((nwritten = rioWriteBulkCount(r, '$', len)) == 0) return 0;

	/* 写入 <payload> */
	if (len > 0 && rioWrite(r, buf, len) == 0) return 0;

	/* 写入 \r\n */
	if (rioWrite(r, "\r\n", 2) == 0) return 0;

	/* 返回写入总量 */
	return nwritten + len + 2;
}



/* 
 * 以 "$<count>\r\n<payload>\r\n" 的格式写入 long long 值
 */
size_t rioWriteBulkLongLong(rio *r, long long l) {
	char lbuf[32];
	unsigned int llen;

	/* 取出 long long 值的字符串形式
	 * 并计算该字符串的长度 */
	llen = ll2string(lbuf, sizeof(lbuf), l);

	/* 写入 $llen\r\nlbuf\r\n */
	return rioWriteBulkString(r, lbuf, llen);
}

/* 
 * 以 "$<count>\r\n<payload>\r\n" 的格式写入 double 值
 */
size_t rioWriteBulkDouble(rio *r, double d) {
	char dbuf[128];
	unsigned int dlen;

	/* 取出 double 值的字符串表示（小数点后只保留 17 位）
	 * 并计算字符串的长度 */
	dlen = snprintf(dbuf, sizeof(dbuf), "%.17g", d);

	/* 写入 $dlen\r\ndbuf\r\n */
	return rioWriteBulkString(r, dbuf, dlen);
}