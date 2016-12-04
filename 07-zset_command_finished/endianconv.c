/* endinconv.c -- Endian conversions utilities. */


#include <stdint.h>

/* Toggle the 16 bit unsigned integer pointed by *p from little endian to
* big endian */
void memrev16(void *p) {
	unsigned char *x = p, t;

	t = x[0];
	x[0] = x[1];
	x[1] = t;
}

/* Toggle the 32 bit unsigned integer pointed by *p from little endian to
* big endian */
void memrev32(void *p) {
	unsigned char *x = p, t;

	t = x[0];
	x[0] = x[3];
	x[3] = t;
	t = x[1];
	x[1] = x[2];
	x[2] = t;
}

/* Toggle the 64 bit unsigned integer pointed by *p from little endian to
* big endian */
void memrev64(void *p) {
	unsigned char *x = p, t;

	t = x[0];
	x[0] = x[7];
	x[7] = t;
	t = x[1];
	x[1] = x[6];
	x[6] = t;
	t = x[2];
	x[2] = x[5];
	x[5] = t;
	t = x[3];
	x[3] = x[4];
	x[4] = t;
}

uint16_t intrev16(uint16_t v) {
	memrev16(&v);
	return v;
}

uint32_t intrev32(uint32_t v) {
	memrev32(&v);
	return v;
}

uint64_t intrev64(uint64_t v) {
	memrev64(&v);
	return v;
}
