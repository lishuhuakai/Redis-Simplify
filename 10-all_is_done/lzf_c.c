#include "lzfP.h"
# include <errno.h>

#define HSIZE (1 << (HLOG))

# define SET_ERRNO(n) errno = (n)
/*
* don't play with this unless you benchmark!
* decompression is not dependent on the hash function
* the hashing function might seem strange, just believe me
* it works ;)
*/

# define FRST(p) (((p[0]) << 8) | p[1])
# define NEXT(v,p) (((v) << 8) | p[2])
#define IDX(h) ((( h >> (3*8 - HLOG)) - h*5) & (HSIZE - 1))
/*
* IDX works because it is very similar to a multiplicative hash, e.g.
* ((h * 57321 >> (3*8 - HLOG)) & (HSIZE - 1))
* the latter is also quite fast on newer CPUs, and compresses similarly.
*
* the next one is also quite good, albeit slow ;)
* (int)(cos(h & 0xffffff) * 1e6)
*/


#define        MAX_LIT        (1 <<  5)
#define        MAX_OFF        (1 << 13)
#define        MAX_REF        ((1 << 8) + (1 << 3))

# define expect(expr,value)         __builtin_expect ((expr),(value))
# define inline                     inline

#define expect_false(expr) expect ((expr) != 0, 0)
#define expect_true(expr)  expect ((expr) != 0, 1)

/*
* compressed format
*
* 000LLLLL <L+1>    ; literal
* LLLooooo oooooooo ; backref L
* 111ooooo LLLLLLLL oooooooo ; backref L+7
*
*/

unsigned int
lzf_compress(const void *const in_data, unsigned int in_len,
	void *out_data, unsigned int out_len)
{
	LZF_STATE htab;
	const u8 **hslot;
	const u8 *ip = (const u8 *)in_data;
	u8 *op = (u8 *)out_data;
	const u8 *in_end = ip + in_len;
	u8 *out_end = op + out_len;
	const u8 *ref;

	/* off requires a type wide enough to hold a general pointer difference.
	* ISO C doesn't have that (size_t might not be enough and ptrdiff_t only
	* works for differences within a single object). We also assume that no
	* no bit pattern traps. Since the only platform that is both non-POSIX
	* and fails to support both assumptions is windows 64 bit, we make a
	* special workaround for it.
	*/

	unsigned long off;
	unsigned int hval;
	int lit;

	if (!in_len || !out_len)
		return 0;

	lit = 0; op++; /* start run */

	hval = FRST(ip);
	while (ip < in_end - 2)
	{
		hval = NEXT(hval, ip);
		hslot = htab + IDX(hval);
		ref = *hslot; *hslot = ip;

		if (1
			&& (off = ip - ref - 1) < MAX_OFF
			&& ip + 4 < in_end
			&& ref > (u8 *)in_data
			&& ref[0] == ip[0]
			&& ref[1] == ip[1]
			&& ref[2] == ip[2])
		{
			/* match found at *ref++ */
			unsigned int len = 2;
			unsigned int maxlen = in_end - ip - len;
			maxlen = maxlen > MAX_REF ? MAX_REF : maxlen;

			op[-lit - 1] = lit - 1; /* stop run */
			op -= !lit; /* undo run if length is zero */

			if (expect_false(op + 3 + 1 >= out_end))
				return 0;

			for (;;)
			{
				if (expect_true(maxlen > 16))
				{
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;

					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;

					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;

					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
					len++; if (ref[len] != ip[len]) break;
				}

				do
					len++;
				while (len < maxlen && ref[len] == ip[len]);

				break;
			}

			len -= 2; /* len is now #octets - 1 */
			ip++;

			if (len < 7)
			{
				*op++ = (off >> 8) + (len << 5);
			}
			else
			{
				*op++ = (off >> 8) + (7 << 5);
				*op++ = len - 7;
			}

			*op++ = off;
			lit = 0; op++; /* start run */

			ip += len + 1;

			if (expect_false(ip >= in_end - 2))
				break;
			--ip;
			--ip;
			hval = FRST(ip);

			hval = NEXT(hval, ip);
			htab[IDX(hval)] = ip;
			ip++;
			hval = NEXT(hval, ip);
			htab[IDX(hval)] = ip;
			ip++;

			ip -= len + 1;
			do
			{
				hval = NEXT(hval, ip);
				htab[IDX(hval)] = ip;
				ip++;
			} while (len--);

		}
		else
		{
			/* one more literal byte we must copy */
			if (expect_false(op >= out_end))
				return 0;

			lit++; *op++ = *ip++;

			if (expect_false(lit == MAX_LIT))
			{
				op[-lit - 1] = lit - 1; /* stop run */
				lit = 0; op++; /* start run */
			}
		}
	}

	if (op + 3 > out_end) /* at most 3 bytes can be missing here */
		return 0;

	while (ip < in_end)
	{
		lit++; *op++ = *ip++;

		if (expect_false(lit == MAX_LIT))
		{
			op[-lit - 1] = lit - 1; /* stop run */
			lit = 0; op++; /* start run */
		}
	}

	op[-lit - 1] = lit - 1; /* end run */
	op -= !lit; /* undo run if length is zero */

	return op - (u8 *)out_data;
}


unsigned int
lzf_decompress(const void *const in_data, unsigned int in_len,
	void             *out_data, unsigned int out_len)
{
	u8 const *ip = (const u8 *)in_data;
	u8       *op = (u8 *)out_data;
	u8 const *const in_end = ip + in_len;
	u8       *const out_end = op + out_len;

	do
	{
		unsigned int ctrl = *ip++;

		if (ctrl < (1 << 5)) /* literal run */
		{
			ctrl++;

			if (op + ctrl > out_end)
			{
				SET_ERRNO(E2BIG);
				return 0;
			}


			if (ip + ctrl > in_end)
			{
				SET_ERRNO(EINVAL);
				return 0;
			}

			do {
				*op++ = *ip++;
			} while (--ctrl);
		}
		else /* back reference */
		{
			unsigned int len = ctrl >> 5;

			u8 *ref = op - ((ctrl & 0x1f) << 8) - 1;


			if (ip >= in_end)
			{
				SET_ERRNO(EINVAL);
				return 0;
			}

			if (len == 7)
			{
				len += *ip++;

				if (ip >= in_end)
				{
					SET_ERRNO(EINVAL);
					return 0;
				}

			}

			ref -= *ip++;

			if (op + len + 2 > out_end)
			{
				SET_ERRNO(E2BIG);
				return 0;
			}

			if (ref < (u8 *)out_data)
			{
				SET_ERRNO(EINVAL);
				return 0;
			}


			*op++ = *ref++;
			*op++ = *ref++;

			do {
				*op++ = *ref++;
			} while (--len);
		}
	} while (ip < in_end);

	return op - (u8 *)out_data;
}

