#ifndef __REDIS_RDB_H
#define __REDIS_RDB_H

#include <stdio.h>
#include "rio.h"

/* TBD: include only necessary headers. */

/* The current RDB version. When the format changes in a way that is no longer
* backward compatible this number gets incremented.
*
* RDB 的版本，当新版本不向就版本兼容时，增一
*/
#define REDIS_RDB_VERSION 6

/* Defines related to the dump file format. To store 32 bits lengths for short
* keys requires a lot of space, so we check the most significant 2 bits of
* the first byte to interpreter the length:
*
* 通过读取第一字节的最高 2 位来判断长度
*
* 00|000000 => if the two MSB are 00 the len is the 6 bits of this byte
*              长度编码在这一字节的其余 6 位中
*
* 01|000000 00000000 =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
*                        长度为 14 位，当前字节 6 位，加上下个字节 8 位
*
* 10|000000 [32 bit integer] => if it's 01, a full 32 bit len will follow
*                               长度由后跟的 32 位保存
*
* 11|000000 this means: specially encoded object will follow. The six bits
*           number specify the kind of object that follows.
*           See the REDIS_RDB_ENC_* defines.
*           后跟一个特殊编码的对象。字节中的 6 位指定对象的类型。
*           查看 REDIS_RDB_ENC_* 定义获得更多消息
*
* Lenghts up to 63 are stored using a single byte, most DB keys, and may
* values, will fit inside.
*
* 一个字节（的其中 6 个字节）可以保存的最大长度是 63 （包括在内），
* 对于大多数键和值来说，都已经足够了。
*/
#define REDIS_RDB_6BITLEN 0
#define REDIS_RDB_14BITLEN 1
#define REDIS_RDB_32BITLEN 2
#define REDIS_RDB_ENCVAL 3
// 表示读取/写入错误
#define REDIS_RDB_LENERR UINT_MAX

/* When a length of a string object stored on disk has the first two bits
* set, the remaining two bits specify a special encoding for the object
* accordingly to the following defines:
*
* 当对象是一个字符串对象时，
* 最高两个位之后的两个位（第 3 个位和第 4 个位）指定了对象的特殊编码
*/
#define REDIS_RDB_ENC_INT8 0        /* 8 bit signed integer */
#define REDIS_RDB_ENC_INT16 1       /* 16 bit signed integer */
#define REDIS_RDB_ENC_INT32 2       /* 32 bit signed integer */
#define REDIS_RDB_ENC_LZF 3         /* string compressed with FASTLZ */

/* Dup object types to RDB object types. Only reason is readability (are we
* dealing with RDB types or with in-memory object types?).
*
* 对象类型在 RDB 文件中的类型
*/
#define REDIS_RDB_TYPE_STRING 0
#define REDIS_RDB_TYPE_LIST   1
#define REDIS_RDB_TYPE_SET    2
#define REDIS_RDB_TYPE_ZSET   3
#define REDIS_RDB_TYPE_HASH   4

/* Object types for encoded objects.
*
* 对象的编码方式
*/
#define REDIS_RDB_TYPE_HASH_ZIPMAP    9
#define REDIS_RDB_TYPE_LIST_ZIPLIST  10
#define REDIS_RDB_TYPE_SET_INTSET    11
#define REDIS_RDB_TYPE_ZSET_ZIPLIST  12
#define REDIS_RDB_TYPE_HASH_ZIPLIST  13

/*
* 检查给定类型是否对象
*/
#define rdbIsObjectType(t) ((t >= 0 && t <= 4) || (t >= 9 && t <= 13))

/*
* 数据库特殊操作标识符
*/
/* 以 MS 计算的过期时间 */
#define REDIS_RDB_OPCODE_EXPIRETIME_MS 252
/* 以秒计算的过期时间 */
#define REDIS_RDB_OPCODE_EXPIRETIME 253
/* 选择数据库 */
#define REDIS_RDB_OPCODE_SELECTDB   254
/* 数据库的结尾（但不是 RDB 文件的结尾）*/
#define REDIS_RDB_OPCODE_EOF        255

#define initStaticStringObject(_var,_ptr) do { \
    _var.refcount = 1; \
    _var.type = REDIS_STRING; \
    _var.encoding = REDIS_ENCODING_RAW; \
    _var.ptr = _ptr; \
} while(0);

int rdbSaveType(rio *rdb, unsigned char type);
void backgroundSaveDoneHandler(int exitcode, int bysignal);
void startLoading(FILE *fp);
void stopLoading(void);
void loadingProgress(off_t pos);
int rdbLoad(char *filename);
#endif

