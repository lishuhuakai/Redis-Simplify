/* 这个文件主要包含一些全局的字典的定义 */
#include "redis.h"

unsigned int dictSdsHash(const void *key) {
	return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

unsigned int dictSdsCaseHash(const void *key) {
	return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictSdsKeyCompare(void *privdata, const void *key1,
	const void *key2) /* 比较两个键的值是否相等,值得一提的是,两个key应该是字符串对象 */
{
	int l1, l2;
	l1 = sdslen((sds)key1); /* 获得长度 */
	l2 = sdslen((sds)key2);
	if (l1 != l2) return 0;
	return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the command lookup table and other
* places where case insensitive non binary-safe comparison is needed. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
	const void *key2)
{
	return strcasecmp(key1, key2) == 0; /* 应该是不区分大小 */
}

void dictSdsDestructor(void *privdata, void *val) { /* 设置析构函数 */
	sdsfree(val);
}

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
	dictSdsCaseHash,           /* hash function */
	NULL,                      /* key dup */
	NULL,                      /* val dup */
	dictSdsKeyCaseCompare,     /* key compare */
	dictSdsDestructor,         /* key destructor */
	NULL                       /* val destructor */
};

void dictRedisObjectDestructor(void *privdata, void *val) {
	if (val == NULL) return; /* Values of swapped out keys as set to NULL */
	decrRefCount(val); /* 减少一个引用 */
}

dictType dbDictType = {
	dictSdsHash,                /* hash function */
	NULL,                       /* key dup */
	NULL,                       /* val dup */
	dictSdsKeyCompare,          /* key compare */
	dictSdsDestructor,          /* key destructor */
	dictRedisObjectDestructor   /* val destructor */
};

/* Db->expires */
dictType keyptrDictType = {
	dictSdsHash,               /* hash function */
	NULL,                      /* key dup */
	NULL,                      /* val dup */
	dictSdsKeyCompare,         /* key compare */
	NULL,                      /* key destructor */
	NULL                       /* val destructor */
};

unsigned int dictEncObjHash(const void *key) {
	robj *o = (robj*)key;

	if (sdsEncodedObject(o)) {
		return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
	}
	else {
		if (o->encoding == REDIS_ENCODING_INT) {
			char buf[32];
			int len;

			len = ll2string(buf, 32, (long)o->ptr);
			return dictGenHashFunction((unsigned char*)buf, len);
		}
		else {
			unsigned int hash;

			o = getDecodedObject(o);
			hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
			decrRefCount(o);
			return hash;
		}
	}
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
	const void *key2)
{
	robj *o1 = (robj*)key1, *o2 = (robj*)key2;
	int cmp;

	if (o1->encoding == REDIS_ENCODING_INT &&
		o2->encoding == REDIS_ENCODING_INT)
		return o1->ptr == o2->ptr;

	o1 = getDecodedObject(o1);
	o2 = getDecodedObject(o2);
	cmp = dictSdsKeyCompare(privdata, o1->ptr, o2->ptr);
	decrRefCount(o1);
	decrRefCount(o2);
	return cmp;
}

/* Hash type hash table (note that small hashes are represented with ziplists) */
dictType hashDictType = {
	dictEncObjHash,             /* hash function */
	NULL,                       /* key dup */
	NULL,                       /* val dup */
	dictEncObjKeyCompare,       /* key compare */
	dictRedisObjectDestructor,  /* key destructor */
	dictRedisObjectDestructor   /* val destructor */
};

/* Sets type hash table */
dictType setDictType = {
	dictEncObjHash,            /* hash function */
	NULL,                      /* key dup */
	NULL,                      /* val dup */
	dictEncObjKeyCompare,      /* key compare */
	dictRedisObjectDestructor, /* key destructor */
	NULL                       /* val destructor */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
	dictEncObjHash,            /* hash function */
	NULL,                      /* key dup */
	NULL,                      /* val dup */
	dictEncObjKeyCompare,      /* key compare */
	dictRedisObjectDestructor, /* key destructor */
	NULL                       /* val destructor */
};

unsigned int dictObjHash(const void *key) {
	const robj *o = key;
	return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

int dictObjKeyCompare(void *privdata, const void *key1,
	const void *key2) {
	const robj *o1 = key1, *o2 = key2;
	return dictSdsKeyCompare(privdata, o1->ptr, o2->ptr);
}

void dictListDestructor(void *privdata, void *val) {
	DICT_NOTUSED(privdata);
	listRelease((list*)val);
}
/* Keylist hash table type has unencoded redis objects as keys and
* lists as values. It's used for blocking operations (BLPOP) and to
* map swapped keys to a list of clients waiting for this keys to be loaded. */
dictType keylistDictType = {
	dictObjHash,                /* hash function */
	NULL,                       /* key dup */
	NULL,                       /* val dup */
	dictObjKeyCompare,          /* key compare */
	dictRedisObjectDestructor,  /* key destructor */
	dictListDestructor          /* val destructor */
};
