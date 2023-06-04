/* Linux epoll(2) based ae.c module
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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


#include <sys/epoll.h>

typedef struct aeApiState {
    int epfd; // epoll专用文件描述符, 一个I/O复用监听程序实例，可以看作是Java NIO中的Selector
    struct epoll_event *events; // 一个epoll事件结构体数组，用于接收已就绪的事件
} aeApiState;

// 创建IO复用机制的上下文环境
static int aeApiCreate(aeEventLoop *eventLoop) {
    // 【1】 创建 aeApiState 结构体，并为 aeApiState.events申请内存空间，用于存放后面已就绪的事件
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;
    state->events = zmalloc(sizeof(struct epoll_event)*eventLoop->setsize);
    if (!state->events) {
        zfree(state);
        return -1;
    }
    // 【2】 创建一个 epoll 实例，即创建一个I/O复用监听程序
    state->epfd = epoll_create(1024); /* 1024 is just a hint for the kernel */
    if (state->epfd == -1) {
        zfree(state->events);
        zfree(state);
        return -1;
    }
    anetCloexec(state->epfd);
    // 【3】 将 aeApiState 保存到事件循环器的 apidata 中
    eventLoop->apidata = state;
    return 0;
}

static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    aeApiState *state = eventLoop->apidata;

    state->events = zrealloc(state->events, sizeof(struct epoll_event)*setsize);
    return 0;
}

static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;

    close(state->epfd);
    zfree(state->events);
    zfree(state);
}

// 添加一个监听对象
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    struct epoll_event ee = {0}; /* avoid valgrind warning */
    /* If the fd was already monitored for some event, we need a MOD
     * operation. Otherwise we need an ADD operation. */
    // 【1】 如果该文件描述符已经存在监听事件了，则使用 EPOLL_CTL_MOD 标志，修改监听对象的监听事件
    // 否则使用 EPOLL_CTL_ADD 添加一个监听对象
    int op = eventLoop->events[fd].mask == AE_NONE ?
            EPOLL_CTL_ADD : EPOLL_CTL_MOD;

    // 【2】 将 AE抽象层事件类型转化为epoll事件类型。AE抽象层定义的 AE_READABLE 对应epoll的 EPOLLIN 事件。 从这里可以看出，Redis使用了Epoll的条件触发模式
    ee.events = 0;
    mask |= eventLoop->events[fd].mask; /* Merge old events */
    if (mask & AE_READABLE) ee.events |= EPOLLIN;
    if (mask & AE_WRITABLE) ee.events |= EPOLLOUT;
    ee.data.fd = fd;
    // epoll_ctl 向内核添加、修改或删除要监控的文件描述符
    /**
     * epoll_ctl函数原型为: int epoll_ctl(int epfd, int op, int fd, struct epoll_event *event)
     *      epfd: epoll专用文件描述符， 用于指定epoll实例
     *      op: 指定监听对象额添加、删除、修改等操作， op部分取值如下：
     *           EPOLL_CTL_ADD: 将监听对象注册到epoll实例；
     *           EPOLL_CTL_DEL: 从epoll实例中删除监听对象；
     *           EPOLL_CTL_MOD: 更改监听对象的监听事件；
     *      fd: 监听对象文件描述符
     *      event: 指定监听的事件类型， event的部分取值如下：
     *           EPOLLIN: 缓冲区当前可读
     *           EPOLLOUT: 缓冲区当前可写入
     *           EPOLLERR: 发送错误
     *           EPOLLHUP: 连接被断开
     *           EPOLLET: 以边缘触发的方式得到事件通知
     * 在IO多路复用模型中，当新的连接进来之后，会触发监听套接字的可读事件，所以这里只需要监听服务器套接字的EPOLLIN事件类型即可。
     *
     */
    if (epoll_ctl(state->epfd,op,fd,&ee) == -1) return -1;
    return 0;
}

// 删除一个监听对象
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int delmask) {
    aeApiState *state = eventLoop->apidata;
    struct epoll_event ee = {0}; /* avoid valgrind warning */
    int mask = eventLoop->events[fd].mask & (~delmask);

    ee.events = 0;
    if (mask & AE_READABLE) ee.events |= EPOLLIN;
    if (mask & AE_WRITABLE) ee.events |= EPOLLOUT;
    ee.data.fd = fd;
    if (mask != AE_NONE) {
        epoll_ctl(state->epfd,EPOLL_CTL_MOD,fd,&ee);
    } else {
        /* Note, Kernel < 2.6.9 requires a non null event pointer even for
         * EPOLL_CTL_DEL. */
        epoll_ctl(state->epfd,EPOLL_CTL_DEL,fd,&ee);
    }
}

/**
 * 阻塞进程，等待事件就绪或给定事件到期
 * aeApiPoll 函数在事件循环器的每次循环中被调用， 负责阻塞进程等待事件发生或者给定时间到期
 * @param eventLoop
 * @param tvp
 * @return
 *
 */
static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;

    // epoll_wait 类似发起了 java NIO中的 selector.select() 调用， 开始阻塞等待事件
    // epoll_wait 的函数原型是：int epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout);
    // 参数说明：
    //      epfd：epoll实例的文件描述符。
    //      events：用于存放epoll事件的数组，epoll_wait函数会将已就绪的事件放到这个数组中。
    //      maxevents：events数组的大小，即最多能存放多少个事件。
    //      timeout：等待事件的超时时间，单位为毫秒，如果为-1，则表示阻塞直到有事件发生，如果为0，则表示立即返回不阻塞，如果为正数，则表示等待指定毫秒数后返回。
    // epoll_wait函数的返回值为就绪的fd的个数
    retval = epoll_wait(state->epfd,state->events,eventLoop->setsize,
            tvp ? (tvp->tv_sec*1000 + (tvp->tv_usec + 999)/1000) : -1);
    if (retval > 0) {
        int j;

        // 【1】 将 Epoll的EPOLLIN、EPOLLERR、EPOLLHUP事件转化为 AE 抽象层的 AE_READABLE 事件
        //      将 Epoll的EPOLLOUT、EPOLLERR、EPOLLHUP事件转化为 AE 抽象层的 AE_WRITABLE 事件
        numevents = retval;
        for (j = 0; j < numevents; j++) {
            int mask = 0;
            struct epoll_event *e = state->events+j;

            // 如果有入站数据，则将就绪的事件类型置为读就绪
            if (e->events & EPOLLIN) mask |= AE_READABLE;
            // 缓冲区当前可写入
            if (e->events & EPOLLOUT) mask |= AE_WRITABLE;
            // 发送错误
            if (e->events & EPOLLERR) mask |= AE_WRITABLE|AE_READABLE;
            // 连接被断开
            if (e->events & EPOLLHUP) mask |= AE_WRITABLE|AE_READABLE;
            // 将发生就绪事件的fd放入事件循环器的就绪事件表中
            eventLoop->fired[j].fd = e->data.fd;
            eventLoop->fired[j].mask = mask;
        }
    }
    return numevents;
}

static char *aeApiName(void) {
    return "epoll";
}
