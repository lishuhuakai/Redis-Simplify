#include "fmacros.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <math.h>
#include <unistd.h>
#include <sys/time.h>
#include <float.h>

#include "util.h"

#define LOG_SIZE 1024

/*
 * 一个简易的日志函数.
 */
void mlog(const char *fileName, int lineNum, const char *func, const char *log_str, ...) {
	va_list vArgList; // 定义一个va_list型的变量,这个变量是指向参数的指针. 
	char buf[LOG_SIZE];
	va_start(vArgList, log_str); // 用va_start宏初始化变量,这个宏的第二个参数是第一个可变参数的前一个参数,是一个固定的参数
	vsnprintf(buf, LOG_SIZE, log_str, vArgList); // 注意,不要漏掉前面的_
	va_end(vArgList);  // 用va_end宏结束可变参数的获取
	printf("%s:%d:%s --> %s\n", fileName, lineNum, func, buf);
}

/* 将一个字符串类型的数转换为longlong类型.
* 如果可以转换的话,返回1,否则的话,返回0
*/
int string2ll(const char *s, size_t slen, long long *value) {
	const char *p = s;
	size_t plen = 0;
	int negative = 0;
	unsigned long long v;

	if (plen == slen)
		return 0;

	/* Special case: first and only digit is 0. */
	if (slen == 1 && p[0] == '0') {
		if (value != NULL) *value = 0;
		return 1;
	}

	if (p[0] == '-') {
		negative = 1;
		p++; plen++;

		/* Abort on only a negative sign. */
		if (plen == slen)
			return 0;
	}

	/* First digit should be 1-9, otherwise the string should just be 0. */
	if (p[0] >= '1' && p[0] <= '9') {
		v = p[0] - '0';
		p++; plen++;
	}
	else if (p[0] == '0' && slen == 1) {
		*value = 0;
		return 1;
	}
	else {
		return 0;
	}

	while (plen < slen && p[0] >= '0' && p[0] <= '9') {
		if (v >(ULLONG_MAX / 10)) /* Overflow. */
			return 0;
		v *= 10;

		if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
			return 0;
		v += p[0] - '0';

		p++; plen++;
	}

	/* Return if not all bytes were used. */
	if (plen < slen)
		return 0;

	if (negative) {
		if (v >((unsigned long long)(-(LLONG_MIN + 1)) + 1)) /* Overflow. */
			return 0;
		if (value != NULL) *value = -v;
	}
	else {
		if (v > LLONG_MAX) /* Overflow. */
			return 0;
		if (value != NULL) *value = v;
	}
	return 1;
}

/* Convert a long long into a string. Returns the number of
* characters needed to represent the number, that can be shorter if passed
* buffer length is not enough to store the whole number. */
int ll2string(char *s, size_t len, long long value) {
	char buf[32], *p;
	unsigned long long v;
	size_t l;

	if (len == 0) return 0;
	v = (value < 0) ? -value : value;
	p = buf + 31; /* point to the last character */
	do {
		*p-- = '0' + (v % 10);
		v /= 10;
	} while (v);
	if (value < 0) *p-- = '-';
	p++;
	l = 32 - (p - buf);
	if (l + 1 > len) l = len - 1; /* Make sure it fits, including the nul term */
	memcpy(s, p, l);
	s[l] = '\0';
	return l;
}

/* Convert a string into a long. Returns 1 if the string could be parsed into a
* (non-overflowing) long, 0 otherwise. The value will be set to the parsed
* value when appropriate. */
int string2l(const char *s, size_t slen, long *lval) {
	long long llval;

	if (!string2ll(s, slen, &llval))
		return 0;

	if (llval < LONG_MIN || llval > LONG_MAX)
		return 0;

	*lval = (long)llval;
	return 1;
}
