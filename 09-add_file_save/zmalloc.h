/* zmalloc - total amount of allocated memory aware version of malloc() */

#ifndef __ZMALLOC_H
#define __ZMALLOC_H

/* Double expansion needed for stringification of macro values. */
#define __xstr(s) __str(s)
#define __str(s) #s

#ifndef ZMALLOC_LIB
#define ZMALLOC_LIB "libc"
#endif

void *zmalloc(size_t size);
void *zcalloc(size_t size);
void *zrealloc(void *ptr, size_t size);
void zfree(void *ptr);
char *zstrdup(const char *s);
size_t zmalloc_used_memory(void);
void zmalloc_enable_thread_safeness(void);
void zmalloc_set_oom_handler(void (*oom_handler)(size_t));
float zmalloc_get_fragmentation_ratio(size_t rss);
size_t zmalloc_get_rss(void);
size_t zmalloc_get_private_dirty(void);
void zlibc_free(void *ptr);

size_t zmalloc_size(void *ptr);

#endif /* __ZMALLOC_H */
