#ifndef __MONOTONIC_H
#define __MONOTONIC_H
/* The monotonic clock is an always increasing clock source.  It is unrelated to
 * the actual time of day and should only be used for relative timings.  The
 * monotonic clock is also not guaranteed to be chronologically precise; there
 * may be slight skew/shift from a precise clock.
 *
 * Depending on system architecture, the monotonic time may be able to be
 * retrieved much faster than a normal clock source by using an instruction
 * counter on the CPU.  On x86 architectures (for example), the RDTSC
 * instruction is a very fast clock source for this purpose.
 */

#include "fmacros.h"
#include <stdint.h>
#include <unistd.h>

/* A counter in micro-seconds.  The 'monotime' type is provided for variables
 * holding a monotonic time.  This will help distinguish & document that the
 * variable is associated with the monotonic clock and should not be confused
 * with other types of time.
 * Redis中的`monotime`是一个以微秒为单位的时间戳，表示Redis服务器启动时的时间戳。它是一个相对于某个未定义的时间点的时间差值，可以用于计算Redis服务器启动后经过的时间。
 * 在Redis源代码中，`monotime`是通过调用`redisUnixtime()`函数获取的，该函数返回的是服务器当前的UNIX时间戳（即1970年1月1日至今所经过的秒数），然后用微秒精度的时间戳减去这个UNIX时间戳得到的。
 * 需要注意的是，`monotime`并不是一个绝对的时间戳，因为它是服务器启动后的时间差值，如果Redis服务器重启后，`monotime`的值会重新计算，因此不能用于跨服务器的时间比较。
 * */
typedef uint64_t monotime;

/* Retrieve counter of micro-seconds relative to an arbitrary point in time.
 * 返回的其实是微妙为单位的redis启动时间
 * */
extern monotime (*getMonotonicUs)(void);

typedef enum monotonic_clock_type {
    MONOTONIC_CLOCK_POSIX,
    MONOTONIC_CLOCK_HW,
} monotonic_clock_type;

/* Call once at startup to initialize the monotonic clock.  Though this only
 * needs to be called once, it may be called additional times without impact.
 * Returns a printable string indicating the type of clock initialized.
 * (The returned string is static and doesn't need to be freed.)  */
const char *monotonicInit();

/* Return a string indicating the type of monotonic clock being used. */
const char *monotonicInfoString();

/* Return the type of monotonic clock being used. */
monotonic_clock_type monotonicGetType();

/* Functions to measure elapsed time.  Example:
 *     monotime myTimer;
 *     elapsedStart(&myTimer);
 *     while (elapsedMs(myTimer) < 10) {} // loops for 10ms
 */
static inline void elapsedStart(monotime *start_time) {
    *start_time = getMonotonicUs();
}

static inline uint64_t elapsedUs(monotime start_time) {
    return getMonotonicUs() - start_time;
}

static inline uint64_t elapsedMs(monotime start_time) {
    return elapsedUs(start_time) / 1000;
}

#endif
