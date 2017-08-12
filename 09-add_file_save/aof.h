#ifndef __AOF_H_
#define __AOF_H_

#include <unistd.h>
#include "redis.h"
#define aof_fsync fdatasync
#define redis_stat stat
#define redis_fstat fstat

/* Macro used to initialize a Redis object allocated on the stack.
* Note that this macro is taken near the structure definition to make sure
* we'll update it when the structure is changed, to avoid bugs like
* bug #85 introduced exactly in this way. */
#define initStaticStringObject(_var,_ptr) do { \
    _var.refcount = 1; \
    _var.type = REDIS_STRING; \
    _var.encoding = REDIS_ENCODING_RAW; \
    _var.ptr = _ptr; \
} while(0);

void flushAppendOnlyFile(int force);
void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc);
void backgroundRewriteDoneHandler(int exitcode, int bysignal);
int rewriteAppendOnlyFileBackground(void); 
int loadAppendOnlyFile(char *filename);
#endif /* __AOF_H_ */
