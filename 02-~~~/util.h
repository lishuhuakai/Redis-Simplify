#ifndef __REDIS_UTIL_H
#define __REDIS_UTIL_H

#include "sds.h"

int string2ll(const char *s, size_t slen, long long *value);
int ll2string(char *s, size_t len, long long value);
int string2l(const char *s, size_t slen, long *lval);
#endif