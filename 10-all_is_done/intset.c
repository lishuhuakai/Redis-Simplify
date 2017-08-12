#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "intset.h"
#include "zmalloc.h"
#include "endianconv.h"

/*
 * intset 的编码方式
 */
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))

/* 
 * 返回适用于传入值 v 的编码方式
 *
 * T = O(1)
 */
static uint8_t _intsetValueEncoding(int64_t v) {
	if (v < INT32_MIN || v > INT32_MAX)
		return INTSET_ENC_INT64;
	else if (v < INT16_MIN || v > INT16_MAX)
		return INTSET_ENC_INT32;
	else
		return INTSET_ENC_INT16;
}

/* 
 * 根据给定的编码方式 enc ，返回集合的底层数组在 pos 索引上的元素。
 *
 * T = O(1)
 */
static int64_t _intsetGetEncoded(intset *is, int pos, uint8_t enc) {
	int64_t v64;
	int32_t v32;
	int16_t v16;

	/* ((ENCODING*)is->contents) 首先将数组转换回被编码的类型
	 * 然后 ((ENCODING*)is->contents)+pos 计算出元素在数组中的正确位置
	 * 之后 member(&vEnc, ..., sizeof(vEnc)) 再从数组中拷贝出正确数量的字节
	 * 如果有需要的话， memrevEncifbe(&vEnc) 会对拷贝出的字节进行大小端转换
	 * 最后将值返回 */
	if (enc == INTSET_ENC_INT64) {
		memcpy(&v64, ((int64_t*)is->contents) + pos, sizeof(v64));
		memrev64ifbe(&v64);
		return v64;
	}
	else if (enc == INTSET_ENC_INT32) {
		memcpy(&v32, ((int32_t*)is->contents) + pos, sizeof(v32));
		memrev32ifbe(&v32);
		return v32;
	}
	else {
		memcpy(&v16, ((int16_t*)is->contents) + pos, sizeof(v16));
		memrev16ifbe(&v16);
		return v16;
	}
}

/* 
 * 根据集合的编码方式，返回底层数组在 pos 索引上的值
 *
 * T = O(1)
 */
static int64_t _intsetGet(intset *is, int pos) {
	return _intsetGetEncoded(is, pos, intrev32ifbe(is->encoding));
}

/* 
 * 根据集合的编码方式，将底层数组在 pos 位置上的值设为 value 。
 *
 * T = O(1)
 */
static void _intsetSet(intset *is, int pos, int64_t value) {

	/* 取出集合的编码方式 */
	uint32_t encoding = intrev32ifbe(is->encoding);

	/* 根据编码 ((Enc_t*)is->contents) 将数组转换回正确的类型
	 * 然后 ((Enc_t*)is->contents)[pos] 定位到数组索引上
	 * 接着 ((Enc_t*)is->contents)[pos] = value 将值赋给数组
	 * 最后， ((Enc_t*)is->contents)+pos 定位到刚刚设置的新值上 
	 * 如果有需要的话， memrevEncifbe 将对值进行大小端转换 */
	if (encoding == INTSET_ENC_INT64) {
		((int64_t*)is->contents)[pos] = value;
		memrev64ifbe(((int64_t*)is->contents) + pos);
	}
	else if (encoding == INTSET_ENC_INT32) {
		((int32_t*)is->contents)[pos] = value;
		memrev32ifbe(((int32_t*)is->contents) + pos);
	}
	else {
		((int16_t*)is->contents)[pos] = value;
		memrev16ifbe(((int16_t*)is->contents) + pos);
	}
}

/*
 * 创建并返回一个新的空整数集合
 *
 * T = O(1)
 */
intset *intsetNew(void) {
	/* 为整数集合结构分配空间 */
	intset *is = zmalloc(sizeof(intset));
	/* 设置初始编码 */
	is->encoding = intrev32ifbe(INTSET_ENC_INT16);
	/* 初始化元素数量 */
	is->length = 0;
	return is;
}

/*
 * 调整整数集合的内存空间大小
 *
 * 如果调整后的大小要比集合原来的大小要大，
 * 那么集合中原有元素的值不会被改变。 
 *
 * 返回值：调整大小后的整数集合
 *
 * T = O(N)
 */
static intset *intsetResize(intset *is, uint32_t len) {

	/* 计算数组的空间大小 */
	uint32_t size = len * intrev32ifbe(is->encoding);

	/* 根据空间大小，重新分配空间
	 * 注意这里使用的是 zrealloc,
	 * 所以如果新空间大小比原来的空间大小要大，
	 * 那么数组原有的数据会被保留 */
	is = zrealloc(is, sizeof(intset) + size);
	return is;
}

/* 
 * 在集合 is 的底层数组中查找值 value 所在的索引。
 *
 * 成功找到 value 时，函数返回 1 ，并将 *pos 的值设为 value 所在的索引。
 *
 * 当在数组中没找到 value 时，返回 0 。
 * 并将 *pos 的值设为 value 可以插入到数组中的位置。
 *
 * T = O(log N)
 */
static uint8_t intsetSearch(intset *is, int64_t value, uint32_t *pos) {
	int min = 0, max = intrev32ifbe(is->length) - 1, mid = -1;
	int64_t cur = -1;

	/* 处理 is 为空时的情况 */
	if (intrev32ifbe(is->length) == 0) {
		if (pos) *pos = 0;
		return 0;
	}
	else {
		/* 因为底层数组是有序的，如果 value 比数组中最后一个值都要大
		 * 那么 value 肯定不存在于集合中，
		 * 并且应该将 value 添加到底层数组的最末端 */
		if (value > _intsetGet(is, intrev32ifbe(is->length) - 1)) {
			if (pos) *pos = intrev32ifbe(is->length);
			return 0;
			/* 因为底层数组是有序的，如果 value 比数组中最前一个值都要小
			 * 那么 value 肯定不存在于集合中，
			 * 并且应该将它添加到底层数组的最前端 */
		}
		else if (value < _intsetGet(is, 0)) {
			if (pos) *pos = 0;
			return 0;
		}
	}

	/* 在有序数组中进行二分查找 */
	while (max >= min) {
		mid = (min + max) / 2;
		cur = _intsetGet(is, mid);
		if (value > cur) {
			min = mid + 1;
		}
		else if (value < cur) {
			max = mid - 1;
		}
		else {
			break;
		}
	}

	/* 检查是否已经找到了 value */
	if (value == cur) {
		if (pos) *pos = mid;
		return 1;
	}
	else {
		if (pos) *pos = min;
		return 0;
	}
}

/*
 * 根据值 value 所使用的编码方式，对整数集合的编码进行升级，
 * 并将值 value 添加到升级后的整数集合中。
 *
 * 返回值：添加新元素之后的整数集合
 *
 * T = O(N)
 */
static intset *intsetUpgradeAndAdd(intset *is, int64_t value) {

	/* 当前的编码方式 */
	uint8_t curenc = intrev32ifbe(is->encoding);

	/* 新值所需的编码方式 */
	uint8_t newenc = _intsetValueEncoding(value);

	/* 当前集合的元素数量 */
	int length = intrev32ifbe(is->length);

	/* 根据 value 的值，决定是将它添加到底层数组的最前端还是最后端
	 * 注意，因为 value 的编码比集合原有的其他元素的编码都要大
	 * 所以 value 要么大于集合中的所有元素，要么小于集合中的所有元素
	 * 因此，value 只能添加到底层数组的最前端或最后端 */
	int prepend = value < 0 ? 1 : 0;

	/* 更新集合的编码方式 */
	is->encoding = intrev32ifbe(newenc);
	/* 根据新编码对集合（的底层数组）进行空间调整 */
	is = intsetResize(is, intrev32ifbe(is->length) + 1);

	// 根据集合原来的编码方式，从底层数组中取出集合元素
	// 然后再将元素以新编码的方式添加到集合中
	// 当完成了这个步骤之后，集合中所有原有的元素就完成了从旧编码到新编码的转换
	// 因为新分配的空间都放在数组的后端，所以程序先从后端向前端移动元素
	// 举个例子，假设原来有 curenc 编码的三个元素，它们在数组中排列如下：
	// | x | y | z | 
	// 当程序对数组进行重分配之后，数组就被扩容了（符号 ？ 表示未使用的内存）：
	// | x | y | z | ? |   ?   |   ?   |
	// 这时程序从数组后端开始，重新插入元素：
	// | x | y | z | ? |   z   |   ?   |
	// | x | y |   y   |   z   |   ?   |
	// |   x   |   y   |   z   |   ?   |
	// 最后，程序可以将新元素添加到最后 ？ 号标示的位置中：
	// |   x   |   y   |   z   |  new  |
	// 上面演示的是新元素比原来的所有元素都大的情况，也即是 prepend == 0
	// 当新元素比原来的所有元素都小时（prepend == 1），调整的过程如下：
	// | x | y | z | ? |   ?   |   ?   |
	// | x | y | z | ? |   ?   |   z   |
	// | x | y | z | ? |   y   |   z   |
	// | x | y |   x   |   y   |   z   |
	// 当添加新值时，原本的 | x | y | 的数据将被新值代替
	// |  new  |   x   |   y   |   z   |
	// T = O(N)
	while (length--)
		_intsetSet(is, length + prepend, _intsetGetEncoded(is, length, curenc));

	/* 设置新值，根据 prepend 的值来决定是添加到数组头还是数组尾 */
	if (prepend)
		_intsetSet(is, 0, value);
	else
		_intsetSet(is, intrev32ifbe(is->length), value);

	/* 更新整数集合的元素数量 */
	is->length = intrev32ifbe(intrev32ifbe(is->length) + 1);

	return is;
}

/*
 * 向前或先后移动指定索引范围内的数组元素
 *
 * 函数名中的 MoveTail 其实是一个有误导性的名字，
 * 这个函数可以向前或向后移动元素，
 * 而不仅仅是向后
 *
 * 在添加新元素到数组时，就需要进行向后移动，
 * 如果数组表示如下（？表示一个未设置新值的空间）：
 * | x | y | z | ? |
 *     |<----->|
 * 而新元素 n 的 pos 为 1 ，那么数组将移动 y 和 z 两个元素
 * | x | y | y | z |
 *         |<----->|
 * 接着就可以将新元素 n 设置到 pos 上了：
 * | x | n | y | z |
 *
 * 当从数组中删除元素时，就需要进行向前移动，
 * 如果数组表示如下，并且 b 为要删除的目标：
 * | a | b | c | d |
 *         |<----->|
 * 那么程序就会移动 b 后的所有元素向前一个元素的位置，
 * 从而覆盖 b 的数据：
 * | a | c | d | d |
 *     |<----->|
 * 最后，程序再从数组末尾删除一个元素的空间：
 * | a | c | d |
 * 这样就完成了删除操作。
 *
 * T = O(N)
 */
static void intsetMoveTail(intset *is, uint32_t from, uint32_t to) {

	void *src, *dst;

	/* 要移动的元素个数 */
	uint32_t bytes = intrev32ifbe(is->length) - from;

	/* 集合的编码方式 */
	uint32_t encoding = intrev32ifbe(is->encoding);

	/* 根据不同的编码
	 * src = (Enc_t*)is->contents+from 记录移动开始的位置
	 * dst = (Enc_t*)is_.contents+to 记录移动结束的位置
	 * bytes *= sizeof(Enc_t) 计算一共要移动多少字节 */
	if (encoding == INTSET_ENC_INT64) {
		src = (int64_t*)is->contents + from;
		dst = (int64_t*)is->contents + to;
		bytes *= sizeof(int64_t);
	}
	else if (encoding == INTSET_ENC_INT32) {
		src = (int32_t*)is->contents + from;
		dst = (int32_t*)is->contents + to;
		bytes *= sizeof(int32_t);
	}
	else {
		src = (int16_t*)is->contents + from;
		dst = (int16_t*)is->contents + to;
		bytes *= sizeof(int16_t);
	}

	/* 进行移动 */
	memmove(dst, src, bytes);
}

/* 
 * 尝试将元素 value 添加到整数集合中。
 *
 * *success 的值指示添加是否成功：
 * - 如果添加成功，那么将 *success 的值设为 1 。
 * - 因为元素已存在而造成添加失败时，将 *success 的值设为 0 。
 *
 * T = O(N)
 */
intset *intsetAdd(intset *is, int64_t value, uint8_t *success) {

	/* 计算编码 value 所需的长度 */
	uint8_t valenc = _intsetValueEncoding(value);
	uint32_t pos;

	/* 默认设置插入为成功 */
	if (success) *success = 1;

	/* 如果 value 的编码比整数集合现在的编码要大
	 * 那么表示 value 必然可以添加到整数集合中
	 * 并且整数集合需要对自身进行升级，才能满足 value 所需的编码 */
	if (valenc > intrev32ifbe(is->encoding)) {
		/* This always succeeds, so we don't need to curry *success. */
		return intsetUpgradeAndAdd(is, value);
	}
	else {
		/* 运行到这里，表示整数集合现有的编码方式适用于 value */

		/* 在整数集合中查找 value ，看他是否存在：
		 * - 如果存在，那么将 *success 设置为 0 ，并返回未经改动的整数集合
		 * - 如果不存在，那么可以插入 value 的位置将被保存到 pos 指针中
		 *   等待后续程序使用 */
		if (intsetSearch(is, value, &pos)) {
			if (success) *success = 0;
			return is;
		}

		/* 运行到这里，表示 value 不存在于集合中, 程序需要将 value 添加到整数集合中 */

		/* 为 value 在集合中分配空间 */
		is = intsetResize(is, intrev32ifbe(is->length) + 1);
		// 如果新元素不是被添加到底层数组的末尾
		// 那么需要对现有元素的数据进行移动，空出 pos 上的位置，用于设置新值
		// 举个例子
		// 如果数组为：
		// | x | y | z | ? |
		//     |<----->|
		// 而新元素 n 的 pos 为 1 ，那么数组将移动 y 和 z 两个元素
		// | x | y | y | z |
		//         |<----->|
		// 这样就可以将新元素设置到 pos 上了：
		// | x | n | y | z |
		// T = O(N)
		if (pos < intrev32ifbe(is->length)) intsetMoveTail(is, pos, pos + 1);
	}

	/* 将新值设置到底层数组的指定位置中 */
	_intsetSet(is, pos, value);

	/* 增一集合元素数量的计数器 */
	is->length = intrev32ifbe(intrev32ifbe(is->length) + 1);

	/* 返回添加新元素后的整数集合 */
	return is;
}

/*
 * 从整数集合中删除值 value 。
 *
 * *success 的值指示删除是否成功：
 * - 因值不存在而造成删除失败时该值为 0 。
 * - 删除成功时该值为 1 。
 *
 * T = O(N)
 */
intset *intsetRemove(intset *is, int64_t value, int *success) {

	/* 计算 value 的编码方式 */
	uint8_t valenc = _intsetValueEncoding(value);
	uint32_t pos;

	/* 默认设置标识值为删除失败 */
	if (success) *success = 0;

	/* 当 value 的编码大小小于或等于集合的当前编码方式（说明 value 有可能存在于集合）
	 * 并且 intsetSearch 的结果为真，那么执行删除
	 * T = O(log N) */
	if (valenc <= intrev32ifbe(is->encoding) && intsetSearch(is, value, &pos)) {
		/* 取出集合当前的元素数量 */
		uint32_t len = intrev32ifbe(is->length);

		/* 设置标识值为删除成功 */
		if (success) *success = 1;

		// 如果 value 不是位于数组的末尾
		// 那么需要对原本位于 value 之后的元素进行移动
		//
		// 举个例子，如果数组表示如下，而 b 为删除的目标
		// | a | b | c | d |
		// 那么 intsetMoveTail 将 b 之后的所有数据向前移动一个元素的空间，
		// 覆盖 b 原来的数据
		// | a | c | d | d |
		// 之后 intsetResize 缩小内存大小时，
		// 数组末尾多出来的一个元素的空间将被移除
		// | a | c | d |
		if (pos < (len - 1)) intsetMoveTail(is, pos + 1, pos);
		/* 缩小数组的大小，移除被删除元素占用的空间 */
		is = intsetResize(is, len - 1);
		/* 更新集合的元素数量 */
		is->length = intrev32ifbe(len - 1);
	}
	return is;
}

/* 
 * 检查给定值 value 是否集合中的元素。
 *
 * 是返回 1 ，不是返回 0 。
 *
 * T = O(log N)
 */
uint8_t intsetFind(intset *is, int64_t value) {

	/* 计算 value 的编码 */
	uint8_t valenc = _intsetValueEncoding(value);

	/* 如果 value 的编码大于集合的当前编码，那么 value 一定不存在于集合
	 * 当 value 的编码小于等于集合的当前编码时，
	 * 才再使用 intsetSearch 进行查找 */
	return valenc <= intrev32ifbe(is->encoding) && intsetSearch(is, value, NULL);
}

/*
 * 从整数集合中随机返回一个元素
 *
 * 只能在集合非空时使用
 *
 * T = O(1)
 */
int64_t intsetRandom(intset *is) {
	/* intrev32ifbe(is->length) 取出集合的元素数量
	 * 而 rand() % intrev32ifbe(is->length) 根据元素数量计算一个随机索引
	 * 然后 _intsetGet 负责根据随机索引来查找值 */
	return _intsetGet(is, rand() % intrev32ifbe(is->length));
}

/*
 * 取出集合底层数组指定位置中的值，并将它保存到 value 指针中。
 *
 * 如果 pos 没超出数组的索引范围，那么返回 1 ，如果超出索引，那么返回 0 。
 *
 * p.s. 上面原文的文档说这个函数用于设置值，这是错误的。
 *
 * T = O(1)
 */
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value) {

	/* 检查 pos 是否符合数组的范围 */
	if (pos < intrev32ifbe(is->length)) {

		/* 保存值到指针 */
		*value = _intsetGet(is, pos);

		/* 返回成功指示值 */
		return 1;
	}

	/* 超出索引范围 */
	return 0;
}

/* 
 * 返回整数集合现有的元素个数
 *
 * T = O(1)
 */
uint32_t intsetLen(intset *is) {
	return intrev32ifbe(is->length);
}

/*
 * 返回整数集合现在占用的字节总数量
 * 这个数量包括整数集合的结构大小，以及整数集合所有元素的总大小
 *
 * T = O(1)
 */
size_t intsetBlobLen(intset *is) {
	return sizeof(intset) + intrev32ifbe(is->length)*intrev32ifbe(is->encoding);
}




