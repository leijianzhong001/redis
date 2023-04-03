/* Listpack -- A lists of strings serialization format
 *
 * This file implements the specification you can find at:
 *
 *  https://github.com/antirez/listpack
 *
 * Copyright (c) 2017, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2020, Redis Labs, Inc
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdint.h>
#include <limits.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "listpack.h"
#include "listpack_malloc.h"
#include "redisassert.h"

#define LP_HDR_SIZE 6       /* 32 bit total len + 16 bit number of elements. */
#define LP_HDR_NUMELE_UNKNOWN UINT16_MAX
#define LP_MAX_INT_ENCODING_LEN 9
#define LP_MAX_BACKLEN_SIZE 5
#define LP_MAX_ENTRY_BACKLEN 34359738367ULL
#define LP_ENCODING_INT 0
#define LP_ENCODING_STRING 1

#define LP_ENCODING_7BIT_UINT 0
#define LP_ENCODING_7BIT_UINT_MASK 0x80
// 0xxxxxxx & 10000000 = 0
#define LP_ENCODING_IS_7BIT_UINT(byte) (((byte)&LP_ENCODING_7BIT_UINT_MASK)==LP_ENCODING_7BIT_UINT)

// 0x80 = 10000000
#define LP_ENCODING_6BIT_STR 0x80
#define LP_ENCODING_6BIT_STR_MASK 0xC0
// 10xxxxxx & 11000000 = 10000000
#define LP_ENCODING_IS_6BIT_STR(byte) (((byte)&LP_ENCODING_6BIT_STR_MASK)==LP_ENCODING_6BIT_STR)

#define LP_ENCODING_13BIT_INT 0xC0
#define LP_ENCODING_13BIT_INT_MASK 0xE0
#define LP_ENCODING_IS_13BIT_INT(byte) (((byte)&LP_ENCODING_13BIT_INT_MASK)==LP_ENCODING_13BIT_INT)

#define LP_ENCODING_12BIT_STR 0xE0
#define LP_ENCODING_12BIT_STR_MASK 0xF0
#define LP_ENCODING_IS_12BIT_STR(byte) (((byte)&LP_ENCODING_12BIT_STR_MASK)==LP_ENCODING_12BIT_STR)

#define LP_ENCODING_16BIT_INT 0xF1
#define LP_ENCODING_16BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_16BIT_INT(byte) (((byte)&LP_ENCODING_16BIT_INT_MASK)==LP_ENCODING_16BIT_INT)

#define LP_ENCODING_24BIT_INT 0xF2
#define LP_ENCODING_24BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_24BIT_INT(byte) (((byte)&LP_ENCODING_24BIT_INT_MASK)==LP_ENCODING_24BIT_INT)

#define LP_ENCODING_32BIT_INT 0xF3
#define LP_ENCODING_32BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_32BIT_INT(byte) (((byte)&LP_ENCODING_32BIT_INT_MASK)==LP_ENCODING_32BIT_INT)

#define LP_ENCODING_64BIT_INT 0xF4
#define LP_ENCODING_64BIT_INT_MASK 0xFF
// 11110100 & 11111111 = 11110100
#define LP_ENCODING_IS_64BIT_INT(byte) (((byte)&LP_ENCODING_64BIT_INT_MASK)==LP_ENCODING_64BIT_INT)

#define LP_ENCODING_32BIT_STR 0xF0
#define LP_ENCODING_32BIT_STR_MASK 0xFF
#define LP_ENCODING_IS_32BIT_STR(byte) (((byte)&LP_ENCODING_32BIT_STR_MASK)==LP_ENCODING_32BIT_STR)

#define LP_EOF 0xFF
// 10xxxxxx & 00111111 = 00xxxxxx
#define LP_ENCODING_6BIT_STR_LEN(p) ((p)[0] & 0x3F)
// 1110xxxx & 00001111 = 00001111 <<8 = 00001111 00000000 | 00000000 11111111 =  00001111 11111111 = 4095
#define LP_ENCODING_12BIT_STR_LEN(p) ((((p)[0] & 0xF) << 8) | (p)[1])
#define LP_ENCODING_32BIT_STR_LEN(p) (((uint32_t)(p)[1]<<0) | \
                                      ((uint32_t)(p)[2]<<8) | \
                                      ((uint32_t)(p)[3]<<16) | \
                                      ((uint32_t)(p)[4]<<24))

#define lpGetTotalBytes(p)           (((uint32_t)(p)[0]<<0) | \
                                      ((uint32_t)(p)[1]<<8) | \
                                      ((uint32_t)(p)[2]<<16) | \
                                      ((uint32_t)(p)[3]<<24))

#define lpGetNumElements(p)          (((uint32_t)(p)[4]<<0) | \
                                      ((uint32_t)(p)[5]<<8))
#define lpSetTotalBytes(p,v) do { \
    (p)[0] = (v)&0xff; \
    (p)[1] = ((v)>>8)&0xff; \
    (p)[2] = ((v)>>16)&0xff; \
    (p)[3] = ((v)>>24)&0xff; \
} while(0)

#define lpSetNumElements(p,v) do { \
    (p)[4] = (v)&0xff; \
    (p)[5] = ((v)>>8)&0xff; \
} while(0)

/* Validates that 'p' is not ouside the listpack.
 * All function that return a pointer to an element in the listpack will assert
 * that this element is valid, so it can be freely used.
 * Generally functions such lpNext and lpDelete assume the input pointer is
 * already validated (since it's the return value of another function). */
#define ASSERT_INTEGRITY(lp, p) do { \
    assert((p) >= (lp)+LP_HDR_SIZE && (p) < (lp)+lpGetTotalBytes((lp))); \
} while (0)

/* Similar to the above, but validates the entire element lenth rather than just
 * it's pointer. */
#define ASSERT_INTEGRITY_LEN(lp, p, len) do { \
    assert((p) >= (lp)+LP_HDR_SIZE && (p)+(len) < (lp)+lpGetTotalBytes((lp))); \
} while (0)

static inline void lpAssertValidEntry(unsigned char* lp, size_t lpbytes, unsigned char *p);

/* Convert a string into a signed 64 bit integer.
 * The function returns 1 if the string could be parsed into a (non-overflowing)
 * signed 64 bit int, 0 otherwise. The 'value' will be set to the parsed value
 * when the function returns success.
 *
 * Note that this function demands that the string strictly represents
 * a int64 value: no spaces or other characters before or after the string
 * representing the number are accepted, nor zeroes at the start if not
 * for the string "0" representing the zero number.
 *
 * Because of its strictness, it is safe to use this function to check if
 * you can convert a string into a long long, and obtain back the string
 * from the number without any loss in the string representation. *
 *
 * -----------------------------------------------------------------------------
 *
 * Credits: this function was adapted from the Redis source code, file
 * "utils.c", function string2ll(), and is copyright:
 *
 * Copyright(C) 2011, Pieter Noordhuis
 * Copyright(C) 2011, Salvatore Sanfilippo
 *
 * The function is released under the BSD 3-clause license.
 */
int lpStringToInt64(const char *s, unsigned long slen, int64_t *value) {
    const char *p = s;
    unsigned long plen = 0;
    int negative = 0;
    uint64_t v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return 1;
    }

    if (p[0] == '-') {
        negative = 1;
        p++; plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0]-'0';
        p++; plen++;
    } else if (p[0] == '0' && slen == 1) {
        *value = 0;
        return 1;
    } else {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (UINT64_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (UINT64_MAX - (p[0]-'0'))) /* Overflow. */
            return 0;
        v += p[0]-'0';

        p++; plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    if (negative) {
        if (v > ((uint64_t)(-(INT64_MIN+1))+1)) /* Overflow. */
            return 0;
        if (value != NULL) *value = -v;
    } else {
        if (v > INT64_MAX) /* Overflow. */
            return 0;
        if (value != NULL) *value = v;
    }
    return 1;
}

/* Create a new, empty listpack.
 * On success the new listpack is returned, otherwise an error is returned.
 * Pre-allocate at least `capacity` bytes of memory,
 * over-allocated memory can be shrinked by `lpShrinkToFit`.
 *
 * 创建一个空的listpack
 * */
unsigned char *lpNew(size_t capacity) {
    // lp_malloc 即 zmalloc
    // capacity 表示预分配的内存大小
    // LP_HDR_SIZE + end => LP_HDR_SIZE+1 => 32 bit total len + 16 bit number of elements + 1 byte = 7
    unsigned char *lp = lp_malloc(capacity > LP_HDR_SIZE+1 ? capacity : LP_HDR_SIZE+1);
    if (lp == NULL) return NULL;
    // 填充totalByte属性，设置占用的字节数
    lpSetTotalBytes(lp,LP_HDR_SIZE+1);
    // 填充元素数量属性，空的话就是0
    lpSetNumElements(lp,0);
    // 填充结束符
    lp[LP_HDR_SIZE] = LP_EOF;
    return lp;
}

/* Free the specified listpack. */
void lpFree(unsigned char *lp) {
    lp_free(lp);
}

/* Shrink the memory to fit. */
unsigned char* lpShrinkToFit(unsigned char *lp) {
    size_t size = lpGetTotalBytes(lp);
    if (size < lp_malloc_size(lp)) {
        return lp_realloc(lp, size);
    } else {
        return lp;
    }
}

/* Given an element 'ele' of size 'size', determine if the element can be
 * represented inside the listpack encoded as integer, and returns
 * LP_ENCODING_INT if so. Otherwise returns LP_ENCODING_STR if no integer
 * encoding is possible.
 *
 * If the LP_ENCODING_INT is returned, the function stores the integer encoded
 * representation of the element in the 'intenc' buffer.
 *
 * Regardless of the returned encoding, 'enclen' is populated by reference to
 * the number of bytes that the string or integer encoded element will require
 * in order to be represented. */
int lpEncodeGetType(unsigned char *ele, uint32_t size, unsigned char *intenc, uint64_t *enclen) {
    int64_t v;
    if (lpStringToInt64((const char*)ele, size, &v)) {
        //  这个分支全都是数值编码
        if (v >= 0 && v <= 127) {
            /* Single byte 0-127 integer. */
            intenc[0] = v;
            *enclen = 1;
        } else if (v >= -4096 && v <= 4095) {
            /* 13 bit integer. */
            if (v < 0) v = ((int64_t)1<<13)+v;
            intenc[0] = (v>>8)|LP_ENCODING_13BIT_INT;
            intenc[1] = v&0xff;
            *enclen = 2;
        } else if (v >= -32768 && v <= 32767) {
            /* 16 bit integer. */
            if (v < 0) v = ((int64_t)1<<16)+v;
            intenc[0] = LP_ENCODING_16BIT_INT;
            intenc[1] = v&0xff;
            intenc[2] = v>>8;
            *enclen = 3;
        } else if (v >= -8388608 && v <= 8388607) {
            /* 24 bit integer. */
            if (v < 0) v = ((int64_t)1<<24)+v;
            intenc[0] = LP_ENCODING_24BIT_INT;
            intenc[1] = v&0xff;
            intenc[2] = (v>>8)&0xff;
            intenc[3] = v>>16;
            *enclen = 4;
        } else if (v >= -2147483648 && v <= 2147483647) {
            /* 32 bit integer. */
            if (v < 0) v = ((int64_t)1<<32)+v;
            intenc[0] = LP_ENCODING_32BIT_INT;
            intenc[1] = v&0xff;
            intenc[2] = (v>>8)&0xff;
            intenc[3] = (v>>16)&0xff;
            intenc[4] = v>>24;
            *enclen = 5;
        } else {
            /* 64 bit integer. */
            uint64_t uv = v;
            intenc[0] = LP_ENCODING_64BIT_INT;
            intenc[1] = uv&0xff;
            intenc[2] = (uv>>8)&0xff;
            intenc[3] = (uv>>16)&0xff;
            intenc[4] = (uv>>24)&0xff;
            intenc[5] = (uv>>32)&0xff;
            intenc[6] = (uv>>40)&0xff;
            intenc[7] = (uv>>48)&0xff;
            intenc[8] = uv>>56;
            *enclen = 9;
        }
        return LP_ENCODING_INT;
    } else {
        // 字符串编码
        if (size < 64) *enclen = 1+size; // 小字符串，元素长度 enclen 为 1 + size， 其中1是encoding属性占用的长度
        else if (size < 4096) *enclen = 2+size; // 中等字符串，元素长度 enclen 为 2 + size， 其中2是encoding属性占用的长度
        else *enclen = 5+(uint64_t)size;// 大字符串，元素长度 enclen 为 5 + size， 其中5是encoding属性占用的长度
        return LP_ENCODING_STRING;
    }
}

/* Store a reverse-encoded variable length field, representing the length
 * of the previous element of size 'l', in the target buffer 'buf'.
 * The function returns the number of bytes used to encode it, from
 * 1 to 5. If 'buf' is NULL the function just returns the number of bytes
 * needed in order to encode the backlen.
 * 翻译：将长度为l的前一个元素的反向编码变量长度字段存储在目标缓冲区“buf”中。
 * 该函数返回用于编码它的字节数，从1到5。如果“buf”为NULL，则函数仅返回用于编码backlen所需的字节数。
 *
 * lpEncodeBacklen 函数负责将 enclen（编码长度+内容长度, 不包含backlen的元素长度)转换为backlen，转换规则为：
 * 将 enclen 的每7个bit划分为一组（有1位是符号位），保存到backlen的一个字节中，并且将字节序倒转。
 * 例如，元素内容为字符串且长度为 11912261672， 则enclen为11912261677， 二进制值为：
 * 11110000 00000010 11000110 00000110 10110000 00101000
 * backlen 的每个字节最高位为1时，代表前面1字节也是backlen的一部分
 *
 * 这个函数具体的的转换方式有点看不懂，看起来象是将enclen值转换为小端序存储
 * */
unsigned long lpEncodeBacklen(unsigned char *buf, uint64_t l) {
    if (l <= 127) {
        // 127 ==> 01111111
        if (buf) buf[0] = l;
        // 元素编码长度 + 字符串内容的大小小于127， 说明是整型值或者小字符串。以小字符串为例，其元素 编码长度 + 字符串内容的大小最大为64个字节，64使用1个字节即可表示，所以backlen返回1
        // 127 刚好是一个字节可以表达的最大值
        return 1;
    } else if (l < 16383) {
        // 16383 ==> 00111111 11111111
        if (buf) {
            buf[0] = l>>7;
            buf[1] = (l&127)|128;
        }
        // 元素编码长度 + 字符串内容的大小大于127且小于16383， 则整个元素的长度可使用两个字节表示，所以backlen的长度返回2
        return 2;
    } else if (l < 2097151) {
        if (buf) {
            buf[0] = l>>14;
            buf[1] = ((l>>7)&127)|128;
            buf[2] = (l&127)|128;
        }
        return 3;
    } else if (l < 268435455) {
        if (buf) {
            buf[0] = l>>21;
            buf[1] = ((l>>14)&127)|128;
            buf[2] = ((l>>7)&127)|128;
            buf[3] = (l&127)|128;
        }
        return 4;
    } else {
        if (buf) {
            buf[0] = l>>28;
            buf[1] = ((l>>21)&127)|128;
            buf[2] = ((l>>14)&127)|128;
            buf[3] = ((l>>7)&127)|128;
            buf[4] = (l&127)|128;
        }
        return 5;
    }
}

/* Decode the backlen and returns it. If the encoding looks invalid (more than
 * 5 bytes are used), UINT64_MAX is returned to report the problem. */
uint64_t lpDecodeBacklen(unsigned char *p) {
    uint64_t val = 0;
    uint64_t shift = 0;
    do {
        val |= (uint64_t)(p[0] & 127) << shift;
        if (!(p[0] & 128)) break;
        shift += 7;
        p--;
        if (shift > 28) return UINT64_MAX;
    } while(1);
    return val;
}

/* Encode the string element pointed by 's' of size 'len' in the target
 * buffer 's'. The function should be called with 'buf' having always enough
 * space for encoding the string. This is done by calling lpEncodeGetType()
 * before calling this function. */
void lpEncodeString(unsigned char *buf, unsigned char *s, uint32_t len) {
    if (len < 64) {
        // 63 | 10000000 => 00111111 | 10000000 => 10111111
        buf[0] = len | LP_ENCODING_6BIT_STR; // 写入字符串编码类型，小字符串，长度范围是`0~63`字节，编码类型就是 10xxxxxx，其中xxxxxx部分为字符串长度
        memcpy(buf+1,s,len); // 写入字符串内容
    } else if (len < 4096) {
        buf[0] = (len >> 8) | LP_ENCODING_12BIT_STR;
        buf[1] = len & 0xff;
        memcpy(buf+2,s,len);
    } else {
        buf[0] = LP_ENCODING_32BIT_STR;
        buf[1] = len & 0xff;
        buf[2] = (len >> 8) & 0xff;
        buf[3] = (len >> 16) & 0xff;
        buf[4] = (len >> 24) & 0xff;
        memcpy(buf+5,s,len);
    }
}

/* Return the encoded length of the listpack element pointed by 'p'.
 * This includes the encoding byte, length bytes, and the element data itself.
 * If the element encoding is wrong then 0 is returned.
 * Note that this method may access additional bytes (in case of 12 and 32 bit
 * str), so should only be called when we know 'p' was already validated by
 * lpCurrentEncodedSizeBytes or ASSERT_INTEGRITY_LEN (possibly since 'p' is
 * a return value of another function that validated its return.
- 其中整数编码如下：

  | 编码                                           | 编码长度   | 整数类型             | 描述                                                         |
  | ---------------------------------------------- | -------- | -------------------- | ------------------------------------------------------------ |
  | `0xxxxxxx`                                     | 1        | int8_t（1 bytes）    | `|0_______|` － 读取后面7位为无符号整数，范围0~127           |
  | `110xxxxx yyyyyyyy`                            | 2        | int16_t（2 bytes）   | `|110_____|________|` － 读取当前字节的后5位和后面 `1` 个字节作为有符号整数 |
  | `11110001 aaaaaaaa bbbbbbbb`                   | 3        | int16_t（2 bytes）   | `|11110001|________|________|` － 读取编码类型后面 `2` 个字节作为 `16 bit` 有符号整数 |
  | `11110010 aaaaaaaa bbbbbbbb cccccccc`          | 4        | int24_t（3 bytes）   | `|11110010|________|________|________|` － 读取后面 `3` 个字节作为 `24 bit` 有符号整数 |
  | `11110011 aaaaaaaa bbbbbbbb cccccccc dddddddd` | 5        | int32_t（4 bytes）   | `|11110011|________|________|________|________|` － 读取后面 `4` 个字节作为 `32 bit` 有符号整数 |
  | `11110100 aaaaaaaa ... hhhhhhhh`               | 6        | 8位有符整数(1 bytes)  | `|11110100|________|________|________|________|________|` － 读取后面 `5` 个字节作为 `40 bit` 有符号整数 |

- 字符串编码如下：

  | **编码**                                       | **编码长度**   | **字符串大小**                                               |
  | ---------------------------------------------- | ------------ | ------------------------------------------------------------ |
  | `10xxxxxx`                                     | **1 bytes**  | `|10______|` - 读取当前字节后面的6位作为字符串长度，用于表示小字符串，长度范围是`0~63`字节 |
  | `1110xxxx yyyyyyyy`                            | **2 bytes**  | `|1110____|________|` - 读取当前字节的后6位和后面1个字节表示作为字符串长度，用于标识中等长度的字符串，长度范围是`0~4095` |
  | `11110000 aaaaaaaa bbbbbbbb cccccccc dddddddd` | **5 bytes**  | `|11110000|________|________|________|________|` - 读取后面4个字节表示作为字符串长度, 表示大字符串. |

- `11111111` 表示 listpack 的结束符号，也就是`0xFF`。
 *
 * 获取当前元素编码长度 + 字符串内容的大小
 * */
uint32_t lpCurrentEncodedSizeUnsafe(unsigned char *p) {
    // p[0] 即第一个字节
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) return 1; // p[0] = 0xxxxxxx
    // 最大为 1 + 63 = 64
    if (LP_ENCODING_IS_6BIT_STR(p[0])) return 1+LP_ENCODING_6BIT_STR_LEN(p); // p[0] = 10xxxxxx 读取当前字节后面的6位作为字符串长度，用于表示小字符串，长度范围是`0~63`字节
    if (LP_ENCODING_IS_13BIT_INT(p[0])) return 2; // p[0]... = 110xxxxx yyyyyyyy
    if (LP_ENCODING_IS_16BIT_INT(p[0])) return 3; // p[0]... = 11110001 aaaaaaaa bbbbbbbb
    if (LP_ENCODING_IS_24BIT_INT(p[0])) return 4; // p[0]... = 11110010 aaaaaaaa bbbbbbbb cccccccc
    if (LP_ENCODING_IS_32BIT_INT(p[0])) return 5; // p[0]... = 11110011 aaaaaaaa bbbbbbbb cccccccc dddddddd
    if (LP_ENCODING_IS_64BIT_INT(p[0])) return 9; // p[0]... = 11110100 aaaaaaaa ... hhhhhhhh
    // 最大为 2 + 4095 = 4097
    if (LP_ENCODING_IS_12BIT_STR(p[0])) return 2+LP_ENCODING_12BIT_STR_LEN(p); // p[0]... = 1110xxxx yyyyyyyy
    // 最大为 5 + 4294967295 = 4294967300
    if (LP_ENCODING_IS_32BIT_STR(p[0])) return 5+LP_ENCODING_32BIT_STR_LEN(p); // p[0]... = 11110000 aaaaaaaa bbbbbbbb cccccccc dddddddd
    if (p[0] == LP_EOF) return 1; // p[0] = `11111111` 表示 listpack 的结束符号，也就是`0xFF`
    return 0;
}

/* Return bytes needed to encode the length of the listpack element pointed by 'p'.
 * This includes just the encodign byte, and the bytes needed to encode the length
 * of the element (excluding the element data itself)
 * If the element encoding is wrong then 0 is returned. */
uint32_t lpCurrentEncodedSizeBytes(unsigned char *p) {
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) return 1;
    if (LP_ENCODING_IS_6BIT_STR(p[0])) return 1;
    if (LP_ENCODING_IS_13BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_16BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_24BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_32BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_64BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_12BIT_STR(p[0])) return 2;
    if (LP_ENCODING_IS_32BIT_STR(p[0])) return 5;
    if (p[0] == LP_EOF) return 1;
    return 0;
}

/* Skip the current entry returning the next. It is invalid to call this
 * function if the current element is the EOF element at the end of the
 * listpack, however, while this function is used to implement lpNext(),
 * it does not return NULL when the EOF element is encountered.
 * listpack 结构如下：
 *      <total_bytes><size><entry><entry>...<entry><end>
 * entry的结构如下：
 *      <encoding><content><backlen>
 *   其中encoding为当前元素编码，content为当前元素的实际内容，backlen为当前元素长度
 * */
unsigned char *lpSkip(unsigned char *p) {
    // 获取当前元素编码长度 + 字符串内容的大小
    unsigned long entrylen = lpCurrentEncodedSizeUnsafe(p);
    // 加上backlen的长度  lpEncodeBacklen 函数根据编码长度 entrylen 计算backlen属性占用的字节数
    entrylen += lpEncodeBacklen(NULL,entrylen);
    // 加上当前元素的长度，则指针指向下一个元素的位置了
    p += entrylen;
    return p;
}

/* If 'p' points to an element of the listpack, calling lpNext() will return
 * the pointer to the next element (the one on the right), or NULL if 'p'
 * already pointed to the last element of the listpack. */
unsigned char *lpNext(unsigned char *lp, unsigned char *p) {
    assert(p);
    p = lpSkip(p);
    if (p[0] == LP_EOF) return NULL;
    lpAssertValidEntry(lp, lpBytes(lp), p);
    return p;
}

/* If 'p' points to an element of the listpack, calling lpPrev() will return
 * the pointer to the previous element (the one on the left), or NULL if 'p'
 * already pointed to the first element of the listpack. */
unsigned char *lpPrev(unsigned char *lp, unsigned char *p) {
    assert(p);
    if (p-lp == LP_HDR_SIZE) return NULL;
    p--; /* Seek the first backlen byte of the last element. */
    uint64_t prevlen = lpDecodeBacklen(p);
    prevlen += lpEncodeBacklen(NULL,prevlen);
    p -= prevlen-1; /* Seek the first byte of the previous entry. */
    lpAssertValidEntry(lp, lpBytes(lp), p);
    return p;
}

/* Return a pointer to the first element of the listpack, or NULL if the
 * listpack has no elements. */
unsigned char *lpFirst(unsigned char *lp) {
    unsigned char *p = lp + LP_HDR_SIZE; /* Skip the header. */
    if (p[0] == LP_EOF) return NULL;
    lpAssertValidEntry(lp, lpBytes(lp), p);
    return p;
}

/* Return a pointer to the last element of the listpack, or NULL if the
 * listpack has no elements. */
unsigned char *lpLast(unsigned char *lp) {
    unsigned char *p = lp+lpGetTotalBytes(lp)-1; /* Seek EOF element. */
    return lpPrev(lp,p); /* Will return NULL if EOF is the only element. */
}

/* Return the number of elements inside the listpack. This function attempts
 * to use the cached value when within range, otherwise a full scan is
 * needed. As a side effect of calling this function, the listpack header
 * could be modified, because if the count is found to be already within
 * the 'numele' header field range, the new value is set. */
uint32_t lpLength(unsigned char *lp) {
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN) return numele;

    /* Too many elements inside the listpack. We need to scan in order
     * to get the total number. */
    uint32_t count = 0;
    unsigned char *p = lpFirst(lp);
    while(p) {
        count++;
        p = lpNext(lp,p);
    }

    /* If the count is again within range of the header numele field,
     * set it. */
    if (count < LP_HDR_NUMELE_UNKNOWN) lpSetNumElements(lp,count);
    return count;
}

/* Return the listpack element pointed by 'p'.
 *
 * The function changes behavior depending on the passed 'intbuf' value.
 * Specifically, if 'intbuf' is NULL:
 *
 * If the element is internally encoded as an integer, the function returns
 * NULL and populates the integer value by reference in 'count'. Otherwise if
 * the element is encoded as a string a pointer to the string (pointing inside
 * the listpack itself) is returned, and 'count' is set to the length of the
 * string.
 *
 * If instead 'intbuf' points to a buffer passed by the caller, that must be
 * at least LP_INTBUF_SIZE bytes, the function always returns the element as
 * it was a string (returning the pointer to the string and setting the
 * 'count' argument to the string length by reference). However if the element
 * is encoded as an integer, the 'intbuf' buffer is used in order to store
 * the string representation.
 *
 * The user should use one or the other form depending on what the value will
 * be used for. If there is immediate usage for an integer value returned
 * by the function, than to pass a buffer (and convert it back to a number)
 * is of course useless.
 *
 * If the function is called against a badly encoded ziplist, so that there
 * is no valid way to parse it, the function returns like if there was an
 * integer encoded with value 12345678900000000 + <unrecognized byte>, this may
 * be an hint to understand that something is wrong. To crash in this case is
 * not sensible because of the different requirements of the application using
 * this lib.
 *
 * Similarly, there is no error returned since the listpack normally can be
 * assumed to be valid, so that would be a very high API cost. However a function
 * in order to check the integrity of the listpack at load time is provided,
 * check lpIsValid(). */
unsigned char *lpGet(unsigned char *p, int64_t *count, unsigned char *intbuf) {
    int64_t val;
    uint64_t uval, negstart, negmax;

    assert(p); /* assertion for valgrind (avoid NPD) */
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) {
        negstart = UINT64_MAX; /* 7 bit ints are always positive. */
        negmax = 0;
        uval = p[0] & 0x7f;
    } else if (LP_ENCODING_IS_6BIT_STR(p[0])) {
        *count = LP_ENCODING_6BIT_STR_LEN(p);
        return p+1;
    } else if (LP_ENCODING_IS_13BIT_INT(p[0])) {
        uval = ((p[0]&0x1f)<<8) | p[1];
        negstart = (uint64_t)1<<12;
        negmax = 8191;
    } else if (LP_ENCODING_IS_16BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8;
        negstart = (uint64_t)1<<15;
        negmax = UINT16_MAX;
    } else if (LP_ENCODING_IS_24BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8 |
               (uint64_t)p[3]<<16;
        negstart = (uint64_t)1<<23;
        negmax = UINT32_MAX>>8;
    } else if (LP_ENCODING_IS_32BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8 |
               (uint64_t)p[3]<<16 |
               (uint64_t)p[4]<<24;
        negstart = (uint64_t)1<<31;
        negmax = UINT32_MAX;
    } else if (LP_ENCODING_IS_64BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8 |
               (uint64_t)p[3]<<16 |
               (uint64_t)p[4]<<24 |
               (uint64_t)p[5]<<32 |
               (uint64_t)p[6]<<40 |
               (uint64_t)p[7]<<48 |
               (uint64_t)p[8]<<56;
        negstart = (uint64_t)1<<63;
        negmax = UINT64_MAX;
    } else if (LP_ENCODING_IS_12BIT_STR(p[0])) {
        *count = LP_ENCODING_12BIT_STR_LEN(p);
        return p+2;
    } else if (LP_ENCODING_IS_32BIT_STR(p[0])) {
        *count = LP_ENCODING_32BIT_STR_LEN(p);
        return p+5;
    } else {
        uval = 12345678900000000ULL + p[0];
        negstart = UINT64_MAX;
        negmax = 0;
    }

    /* We reach this code path only for integer encodings.
     * Convert the unsigned value to the signed one using two's complement
     * rule. */
    if (uval >= negstart) {
        /* This three steps conversion should avoid undefined behaviors
         * in the unsigned -> signed conversion. */
        uval = negmax-uval;
        val = uval;
        val = -val-1;
    } else {
        val = uval;
    }

    /* Return the string representation of the integer or the value itself
     * depending on intbuf being NULL or not. */
    if (intbuf) {
        *count = snprintf((char*)intbuf,LP_INTBUF_SIZE,"%lld",(long long)val);
        return intbuf;
    } else {
        *count = val;
        return NULL;
    }
}

/* Insert, delete or replace the specified element 'ele' of length 'len' at
 * the specified position 'p', with 'p' being a listpack element pointer
 * obtained with lpFirst(), lpLast(), lpNext(), lpPrev() or lpSeek().
 *
 * The element is inserted before, after, or replaces the element pointed
 * by 'p' depending on the 'where' argument, that can be LP_BEFORE, LP_AFTER
 * or LP_REPLACE.
 *
 * If 'ele' is set to NULL, the function removes the element pointed by 'p'
 * instead of inserting one.
 *
 * Returns NULL on out of memory or when the listpack total length would exceed
 * the max allowed size of 2^32-1, otherwise the new pointer to the listpack
 * holding the new element is returned (and the old pointer passed is no longer
 * considered valid)
 *
 * If 'newp' is not NULL, at the end of a successful call '*newp' will be set
 * to the address of the element just added, so that it will be possible to
 * continue an interation with lpNext() and lpPrev().
 *
 * For deletion operations ('ele' set to NULL) 'newp' is set to the next
 * element, on the right of the deleted one, or to NULL if the deleted element
 * was the last one.
 * 一个listpack的整体结构如下：
 *      <lpbytes><lpnumbers><entry><entry>...<entry><end>
 *  其中：
 *      <lpbytes>记录了 当前`listpack`占用的字节数，长度为4个字节；
 *      `<size>`: 表示当前`listpack`中元素的数量，长度为2个字节；
 *      `<end>`: 结束标识符，其值恒为`0xff`, 1个字节；
 *  listpack中的entry组成为:
 *      <encoding><content><backlen>
 * */
unsigned char *lpInsert(unsigned char *lp, unsigned char *ele, uint32_t size, unsigned char *p, int where, unsigned char **newp) {
    // *ele 插入元素的内容
    // size 插入元素的大小
    // p 元素插入的位置
    // where 插入方式，存在 LP_BEFORE、 LP_AFTER、LP_REPLACE三个值，即在插入位置前、插入位置后或者直接替换插入位置的元素
    // newp 记录新元素
    unsigned char intenc[LP_MAX_INT_ENCODING_LEN];
    unsigned char backlen[LP_MAX_BACKLEN_SIZE];

    uint64_t enclen; /* The length of the encoded element. */

    /* An element pointer set to NULL means deletion, which is conceptually
     * replacing the element with a zero-length element. So whatever we
     * get passed as 'where', set it to LP_REPLACE. */
    // 【1】 如果插入元素为空，则实际上意味着删除元素，所以将where赋值为LP_REPLACE，以便后续将指定的元素设置为NULL。
    if (ele == NULL) where = LP_REPLACE;

    /* If we need to insert after the current element, we just jump to the
     * next element (that could be the EOF one) and handle the case of
     * inserting before. So the function will actually deal with just two
     * cases: LP_BEFORE and LP_REPLACE. */
    // 如果where参数为LP_AFTER，即在插入位置的后面插入元素，则找到后驱节点，并将插入操作调整为 LP_BEFORE，（在后驱节点之前插入）。这样的话就只需要处理两个逻辑LP_BEFORE和LP_REPLACE
    if (where == LP_AFTER) {
        // 找到后驱节点
        p = lpSkip(p);
        // 将插入操作调整为 LP_BEFORE
        where = LP_BEFORE;
        ASSERT_INTEGRITY(lp, p);
    }

    /* Store the offset of the element 'p', so that we can obtain its
     * address again after a reallocation.
     * 存储元素 'p' 的偏移量poff，以便我们可以在重新分配后再次获取其地址
     * */
    unsigned long poff = p-lp;

    /* Calling lpEncodeGetType() results into the encoded version of the
     * element to be stored into 'intenc' in case it is representable as
     * an integer: in that case, the function returns LP_ENCODING_INT.
     * Otherwise if LP_ENCODING_STR is returned, we'll have to call
     * lpEncodeString() to actually write the encoded string on place later.
     *
     * Whatever the returned encoding is, 'enclen' is populated with the
     * length of the encoded element. */
    // 【2】 lpEncodeGetType函数对插入元素内容进行编码，返回编码类型，编码类型有 LP_ENCODING_INT（数值编码）和 LP_ENCODING_STRING（字符串编码） 两种。
    // 该函数处理成功后，会将元素编码（encode属性） 存储在 intenc 变量中（数值时为具体的编码，字符串则该值为空），元素长度存储在enclen变量中（这个长度为编码长度+字符串内容长度，注意是值，不是字段宽度）
    int enctype;
    if (ele) {
        // enctype为编码类型，整数编码或字符串
        enctype = lpEncodeGetType(ele,size,intenc,&enclen);
    } else {
        enctype = -1;
        enclen = 0;
    }

    /* We need to also encode the backward-parsable length of the element
     * and append it to the end: this allows to traverse the listpack from
     * the end to the start. */
    // 【3】 lpEncodeBacklen 函数将插入的元素长度 enclen（这个长度为编码长度+字符串内容长度）转化为backlen属性，并将其记录在backlen参数中，返回backlen属性占用的字节数（也即是元素length部分的宽度）
    unsigned long backlen_size = ele ? lpEncodeBacklen(backlen,enclen) : 0;
    // 【4】 计算新的listpack占用的空间 new_listpack_bytes
    uint64_t old_listpack_bytes = lpGetTotalBytes(lp);
    uint32_t replaced_len  = 0;
    if (where == LP_REPLACE) {
        // listpack中的entry组成为: <encoding><content><backlen>
        // 获取当前元素编码长度 + 字符串内容的大小
        replaced_len = lpCurrentEncodedSizeUnsafe(p);
        // 加上backlen属性占用的字节数
        replaced_len += lpEncodeBacklen(NULL,replaced_len);
        ASSERT_INTEGRITY_LEN(lp, p, replaced_len);
    }

    // new_listpack_bytes的组成为：原来的listpack占用的空间 + 插入元素的编码长度(即编码类型和元素内容被编码后占用的长度，其实就是enclen) + backlen属性占用的字节数 - 替换的元素的长度
    uint64_t new_listpack_bytes = old_listpack_bytes + enclen + backlen_size
                                  - replaced_len;
    if (new_listpack_bytes > UINT32_MAX) return NULL;

    /* We now need to reallocate in order to make space or shrink the
     * allocation (in case 'when' value is LP_REPLACE and the new element is
     * smaller). However we do that before memmoving the memory to
     * make room for the new element if the final allocation will get
     * larger, or we do it after if the final allocation will get smaller. */

    // dst表示的是插入位置或者替换元素的位置起始地址
    unsigned char *dst = lp + poff; /* May be updated after reallocation. */

    /* Realloc before: we need more room. */
    // 【5】 如果新的空间大于原来的空间（插入元素或替换为更大的元素），则调用 lp_realloc 函数申请新的内存空间
    if (new_listpack_bytes > old_listpack_bytes &&
        new_listpack_bytes > lp_malloc_size(lp)) {
        if ((lp = lp_realloc(lp,new_listpack_bytes)) == NULL) return NULL;
        dst = lp + poff;
    }

    /* Setup the listpack relocating the elements to make the exact room
     * we need to store the new one. */
    // 【6】 如果是插入元素，则将插入位置后面的元素后移，为插入元素腾出空间。如果是替换元素，则调整替换元素的大小
    if (where == LP_BEFORE) {
        // void * memmove ( void * destination, const void * source, size_t num );
        // dst+enclen+backlen_size 移动的目标地址
        // dst 移动的源地址
        // old_listpack_bytes-poff 移动的内容长度
        memmove(dst+enclen+backlen_size,dst,old_listpack_bytes-poff);
    } else { /* LP_REPLACE. */
        // 新旧元素长度的差值，enclen+backlen_size为新元素的长度，replaced_len为旧元素的长度
        long lendiff = (enclen+backlen_size)-replaced_len;
        // void * memmove ( void * destination, const void * source, size_t num );
        // dst+replaced_len+lendiff 移动的目标地址
        // dst+replaced_len 移动的源地址
        // old_listpack_bytes-poff-replaced_len 移动的内容长度
        // 下面这个操作会将被替换的元素后面的内容向前或者向后移动，以使得内存空隙符合新元素的大小
        memmove(dst+replaced_len+lendiff,
                dst+replaced_len,
                old_listpack_bytes-poff-replaced_len);
    }

    /* Realloc after: we need to free space. */
    // 【7】 如果新的空间小于原来的空间（删除或替换为更小的元素），则调用 lp_realloc 函数调整listpack空间
    if (new_listpack_bytes < old_listpack_bytes) {
        // 如果占用内存变小了，重新找个合适的内存片段存放listpack以节省空间，
        // 比如本来listpack占用9kb， 则内存分配器会给其分配12kb的内存空间，但是如果替换之后大小变为7kb，则直接为其分配一个8kb的内存空间就足够了
        if ((lp = lp_realloc(lp,new_listpack_bytes)) == NULL) return NULL;
        dst = lp + poff;
    }

    /* Store the entry. */
    // 【8】 执行到这里，说明listpack空间已经调整好了，直接将新元素插入到dst位置即可。这里将newp指针指向插入位置dst，插入元素后newp指针将指向新元素
    if (newp) {
        *newp = dst;
        /* In case of deletion, set 'newp' to NULL if the next element is
         * the EOF element. */
        if (!ele && dst[0] == LP_EOF) *newp = NULL;
    }
    if (ele) {
        // 【9】 将插入元素内容保存到listpack中
        if (enctype == LP_ENCODING_INT) {
            // 如果插入的内容是数值编码，则表示插入元素已经保存到intenc变量中（在lpEncodeBacklen函数中操作），则直接将intenc变量写入listpack即可。
            memcpy(dst,intenc,enclen);
        } else {
            // 否则调用 lpEncodeString 函数，依次写入元素编码和元素内容
            lpEncodeString(dst,ele,size);
        }
        // 【10】 写入backlen属性，即写入最后的length部分
        dst += enclen; // dst本来指向插入位置或者替换元素的位置起始地址，现在指向插入元素或替换元素的末尾，也是length部分的起始地址
        memcpy(dst,backlen,backlen_size);
        dst += backlen_size;
    }

    /* Update header. */
    // 【11】 更新listpack中lpbytes、 lpnumbers属性
    if (where != LP_REPLACE || ele == NULL) {
        uint32_t num_elements = lpGetNumElements(lp);
        if (num_elements != LP_HDR_NUMELE_UNKNOWN) {
            if (ele)
                lpSetNumElements(lp,num_elements+1);
            else
                lpSetNumElements(lp,num_elements-1);
        }
    }
    lpSetTotalBytes(lp,new_listpack_bytes);

#if 0
    /* This code path is normally disabled: what it does is to force listpack
     * to return *always* a new pointer after performing some modification to
     * the listpack, even if the previous allocation was enough. This is useful
     * in order to spot bugs in code using listpacks: by doing so we can find
     * if the caller forgets to set the new pointer where the listpack reference
     * is stored, after an update. */
    unsigned char *oldlp = lp;
    lp = lp_malloc(new_listpack_bytes);
    memcpy(lp,oldlp,new_listpack_bytes);
    if (newp) {
        unsigned long offset = (*newp)-oldlp;
        *newp = lp + offset;
    }
    /* Make sure the old allocation contains garbage. */
    memset(oldlp,'A',new_listpack_bytes);
    lp_free(oldlp);
#endif

    return lp;
}

/* Append the specified element 'ele' of length 'len' at the end of the
 * listpack. It is implemented in terms of lpInsert(), so the return value is
 * the same as lpInsert(). */
unsigned char *lpAppend(unsigned char *lp, unsigned char *ele, uint32_t size) {
    uint64_t listpack_bytes = lpGetTotalBytes(lp);
    unsigned char *eofptr = lp + listpack_bytes - 1;
    return lpInsert(lp,ele,size,eofptr,LP_BEFORE,NULL);
}

/* Remove the element pointed by 'p', and return the resulting listpack.
 * If 'newp' is not NULL, the next element pointer (to the right of the
 * deleted one) is returned by reference. If the deleted element was the
 * last one, '*newp' is set to NULL. */
unsigned char *lpDelete(unsigned char *lp, unsigned char *p, unsigned char **newp) {
    return lpInsert(lp,NULL,0,p,LP_REPLACE,newp);
}

/* Return the total number of bytes the listpack is composed of. */
uint32_t lpBytes(unsigned char *lp) {
    return lpGetTotalBytes(lp);
}

/* Seek the specified element and returns the pointer to the seeked element.
 * Positive indexes specify the zero-based element to seek from the head to
 * the tail, negative indexes specify elements starting from the tail, where
 * -1 means the last element, -2 the penultimate and so forth. If the index
 * is out of range, NULL is returned. */
unsigned char *lpSeek(unsigned char *lp, long index) {
    int forward = 1; /* Seek forward by default. */

    /* We want to seek from left to right or the other way around
     * depending on the listpack length and the element position.
     * However if the listpack length cannot be obtained in constant time,
     * we always seek from left to right. */
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN) {
        if (index < 0) index = (long)numele+index;
        if (index < 0) return NULL; /* Index still < 0 means out of range. */
        if (index >= (long)numele) return NULL; /* Out of range the other side. */
        /* We want to scan right-to-left if the element we are looking for
         * is past the half of the listpack. */
        if (index > (long)numele/2) {
            forward = 0;
            /* Right to left scanning always expects a negative index. Convert
             * our index to negative form. */
            index -= numele;
        }
    } else {
        /* If the listpack length is unspecified, for negative indexes we
         * want to always scan right-to-left. */
        if (index < 0) forward = 0;
    }

    /* Forward and backward scanning is trivially based on lpNext()/lpPrev(). */
    if (forward) {
        unsigned char *ele = lpFirst(lp);
        while (index > 0 && ele) {
            ele = lpNext(lp,ele);
            index--;
        }
        return ele;
    } else {
        unsigned char *ele = lpLast(lp);
        while (index < -1 && ele) {
            ele = lpPrev(lp,ele);
            index++;
        }
        return ele;
    }
}

/* Same as lpFirst but without validation assert, to be used right before lpValidateNext. */
unsigned char *lpValidateFirst(unsigned char *lp) {
    unsigned char *p = lp + LP_HDR_SIZE; /* Skip the header. */
    if (p[0] == LP_EOF) return NULL;
    return p;
}

/* Validate the integrity of a single listpack entry and move to the next one.
 * The input argument 'pp' is a reference to the current record and is advanced on exit.
 * Returns 1 if valid, 0 if invalid. */
int lpValidateNext(unsigned char *lp, unsigned char **pp, size_t lpbytes) {
#define OUT_OF_RANGE(p) ( \
        (p) < lp + LP_HDR_SIZE || \
        (p) > lp + lpbytes - 1)
    unsigned char *p = *pp;
    if (!p)
        return 0;

    /* Before accessing p, make sure it's valid. */
    if (OUT_OF_RANGE(p))
        return 0;

    if (*p == LP_EOF) {
        *pp = NULL;
        return 1;
    }

    /* check that we can read the encoded size */
    uint32_t lenbytes = lpCurrentEncodedSizeBytes(p);
    if (!lenbytes)
        return 0;

    /* make sure the encoded entry length doesn't rech outside the edge of the listpack */
    if (OUT_OF_RANGE(p + lenbytes))
        return 0;

    /* get the entry length and encoded backlen. */
    unsigned long entrylen = lpCurrentEncodedSizeUnsafe(p);
    unsigned long encodedBacklen = lpEncodeBacklen(NULL,entrylen);
    entrylen += encodedBacklen;

    /* make sure the entry doesn't rech outside the edge of the listpack */
    if (OUT_OF_RANGE(p + entrylen))
        return 0;

    /* move to the next entry */
    p += entrylen;

    /* make sure the encoded length at the end patches the one at the beginning. */
    uint64_t prevlen = lpDecodeBacklen(p-1);
    if (prevlen + encodedBacklen != entrylen)
        return 0;

    *pp = p;
    return 1;
#undef OUT_OF_RANGE
}

/* Validate that the entry doesn't reach outside the listpack allocation. */
static inline void lpAssertValidEntry(unsigned char* lp, size_t lpbytes, unsigned char *p) {
    assert(lpValidateNext(lp, &p, lpbytes));
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
int lpValidateIntegrity(unsigned char *lp, size_t size, int deep){
    /* Check that we can actually read the header. (and EOF) */
    if (size < LP_HDR_SIZE + 1)
        return 0;

    /* Check that the encoded size in the header must match the allocated size. */
    size_t bytes = lpGetTotalBytes(lp);
    if (bytes != size)
        return 0;

    /* The last byte must be the terminator. */
    if (lp[size-1] != LP_EOF)
        return 0;

    if (!deep)
        return 1;

    /* Validate the invividual entries. */
    uint32_t count = 0;
    unsigned char *p = lp + LP_HDR_SIZE;
    while(p && p[0] != LP_EOF) {
        if (!lpValidateNext(lp, &p, bytes))
            return 0;
        count++;
    }

    /* Make sure 'p' really does point to the end of the listpack. */
    if (p != lp + size - 1)
        return 0;

    /* Check that the count in the header is correct */
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN && numele != count)
        return 0;

    return 1;
}
