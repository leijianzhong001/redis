/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __AE_H__
#define __AE_H__

#include "monotonic.h"

#define AE_OK 0
#define AE_ERR -1

#define AE_NONE 0       /* No events registered. */
#define AE_READABLE 1   /* Fire when descriptor is readable. */
#define AE_WRITABLE 2   /* Fire when descriptor is writable. */
#define AE_BARRIER 4    /* With WRITABLE, never fire the event if the
                           READABLE event already fired in the same event
                           loop iteration. Useful when you want to persist
                           things to disk before sending replies, and want
                           to do that in a group fashion. */

#define AE_FILE_EVENTS (1<<0)
#define AE_TIME_EVENTS (1<<1)
#define AE_ALL_EVENTS (AE_FILE_EVENTS|AE_TIME_EVENTS)
#define AE_DONT_WAIT (1<<2)
#define AE_CALL_BEFORE_SLEEP (1<<3)
#define AE_CALL_AFTER_SLEEP (1<<4)

#define AE_NOMORE -1
#define AE_DELETED_EVENT_ID -1

/* Macros */
#define AE_NOTUSED(V) ((void) V)

struct aeEventLoop;

/* Types and data structures */
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

/* File event structure
 * aeFileEvent 存储了一个文件描述符上已注册的文件事件
 * aeFileEvent 中并没有记录文件描述符的fd属性。POSIX标准对文件描述符fd有如下约束：
 *      1、值为0、1、2的文件描述符分别表述标准输入、标准输出、和错误输出
 *      2、每次打开新的文件描述符，必须使用当前进程中最小可用的文件描述符
 * redis充分利用文件描述符的这些特点， ，定义了一个数组 aeEventLoop.events来存储已注册的文件事件。数组索引即文件描述符，数组元素即该文件描述符
 * 上注册的文件事件，如aeEventLoop.events[99]存放了置为99的文件描述符的文件事件
 *
 * */
typedef struct aeFileEvent {
    int mask; /* one of AE_(READABLE|WRITABLE|BARRIER)                         已注册的文件事件类型 */
    aeFileProc *rfileProc; /*                                                  RE_ADABLE 事件处理函数 */
    aeFileProc *wfileProc; /*                                                  WR_ITABLE 事件处理函数 */
    void *clientData; /*                                                       附加数据 */
} aeFileEvent;

/* Time event structure
 * 存储已注册的事件事件信息
 * */
typedef struct aeTimeEvent {
    long long id; /* time event identifier.                                     时间事件id */
    monotime when; /*                                                           时间事件的下一次执行的微妙数(UNIX时间戳) */
    aeTimeProc *timeProc; /*                                                    时间事件处理函数 */
    aeEventFinalizerProc *finalizerProc; /*                                     时间事件终结函数 */
    void *clientData; /*                                                        客户端传入的附加数据 */
    struct aeTimeEvent *prev; /*                                                指向前一个时间事件 */
    struct aeTimeEvent *next; /*                                                指向后一个时间事件 */
    int refcount; /* refcount to prevent timer events from being
  		   * freed in recursive time event calls. */
} aeTimeEvent;

/* A fired event
 * IO复用层(ae_epoll.c、ae_evport.c等)会将已就绪的事件转化为 aeFiredEvent ， 存放在 aeEventLoop.fired数组中，等待事件循环器处理
 * */
typedef struct aeFiredEvent {
    int fd; /*                                                                 产生事件的文件描述符 */
    int mask; /*                                                               产生的事件类型 */
} aeFiredEvent;

/* State of an event based program
 *
 * redis中的事件循环器， 负责管理事件
 * */
typedef struct aeEventLoop {
    int maxfd;   /* highest file descriptor currently registered       当前已注册的最大文件描述符 */
    int setsize; /* max number of file descriptors tracked             该事件循环器允许监听的最大的文件描述符 */
    long long timeEventNextId; /*                                      下一个事件时间id */
    aeFileEvent *events; /* Registered events                          已注册的文件事件表 */
    aeFiredEvent *fired; /* Fired events                               已就绪的事件表 */
    aeTimeEvent *timeEventHead; /*                                     时间事件表的头节点指针 */
    int stop; /*                                                       事件循环器是否停止 */
    void *apidata; /* This is used for polling API specific data       存放用于IO复用层的附加数据 */
    aeBeforeSleepProc *beforesleep; /*                                 进程阻塞前后调用的钩子函数 */
    aeBeforeSleepProc *aftersleep;
    int flags;
} aeEventLoop;

/* Prototypes */
aeEventLoop *aeCreateEventLoop(int setsize);
void aeDeleteEventLoop(aeEventLoop *eventLoop);
void aeStop(aeEventLoop *eventLoop);
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData);
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);
int aeGetFileEvents(aeEventLoop *eventLoop, int fd);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
int aeWait(int fd, int mask, long long milliseconds);
void aeMain(aeEventLoop *eventLoop);
char *aeGetApiName(void);
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep);
int aeGetSetSize(aeEventLoop *eventLoop);
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize);
void aeSetDontWait(aeEventLoop *eventLoop, int noWait);

#endif
