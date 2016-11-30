#ifndef __REDIS_UTIL_H
#define __REDIS_UTIL_H

#include "sds.h"
void mlog(const char *fileName, int lineNum, const char *func, const char *log_str, ...);

#define mylog(formatPM, args...)\
  mlog(__FILE__, __LINE__, __FUNCTION__,  (formatPM) , ##args)
int string2ll(const char *s, size_t slen, long long *value);
int ll2string(char *s, size_t len, long long value);
int string2l(const char *s, size_t slen, long *lval);

#endif

