/* See endianconv.c top comments for more information */

#ifndef __ENDIANCONV_H
#define __ENDIANCONV_H

#include <stdint.h>

void memrev16(void *p);
void memrev32(void *p);
void memrev64(void *p);
uint16_t intrev16(uint16_t v);
uint32_t intrev32(uint32_t v);
uint64_t intrev64(uint64_t v);

/* variants of the function doing the actual convertion only if the target
* host is big endian */
#define memrev16ifbe(p)
#define memrev32ifbe(p)
#define memrev64ifbe(p)
#define intrev16ifbe(v) (v)
#define intrev32ifbe(v) (v)
#define intrev64ifbe(v) (v)


/* The functions htonu64() and ntohu64() convert the specified value to
* network byte ordering and back. In big endian systems they are no-ops. */
#define htonu64(v) (v)
#define ntohu64(v) (v)

#endif
