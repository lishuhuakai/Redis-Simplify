#ifndef __REDIS_UTIL_H
#define __REDIS_UTIL_H

#include "sds.h"

int string2ll(const char *s, size_t slen, long long *value);

#endif