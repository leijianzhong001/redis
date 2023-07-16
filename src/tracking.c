/* tracking.c - Client side caching: keys tracking and invalidation
 *
 * Copyright (c) 2019, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "server.h"

/* The tracking table is constituted by a radix tree of keys, each pointing
 * to a radix tree of client IDs, used to track the clients that may have
 * certain keys in their local, client side, cache.
 *
 * When a client enables tracking with "CLIENT TRACKING on", each key served to
 * the client is remembered in the table mapping the keys to the client IDs.
 * Later, when a key is modified, all the clients that may have local copy
 * of such key will receive an invalidation message.
 *
 * Clients will normally take frequently requested objects in memory, removing
 * them when invalidation messages are received.
 * tracking table 由键的基数树组成，每个键都是指向客户端id的基数树，用于跟踪可能在其本地、客户端缓存中具有某些键的客户端。
 * 当客户端启用“client tracking on”跟踪时，每个提供给客户端的key都会被记录在表中（表中将key映射到client id）。稍后，当一个密钥被修改时，所有可能拥有该key本地副本的客户端都将收到一条失效消息。
 * 客户端通常会在内存中获取频繁请求的对象，在收到无效消息时将其删除。
 *
 * TrackingTable 变量用于非广播模式，TrackingTable 的键记录了客户端查询过的redis key，TrackingTable的值也是Rax变量，这个Rax变量的key为client id, 值为NULL（这里使用rax结构的作用类似于列表，只不过因为rax更加节省内存，所以使用了rax树）
 * */
rax *TrackingTable = NULL;
/**
 * PrefixTable 变量用于广播模式。其中key为客户端关注的key前缀，value指向bcastState变量
 */
rax *PrefixTable = NULL;
uint64_t TrackingTableTotalItems = 0; /* Total number of IDs stored across
                                         the whole tracking table. This gives
                                         an hint about the total memory we
                                         are using server side for CSC. */
robj *TrackingChannelName;                                                  // 在tracking被打开后，固定初始化为 __redis__:invalidate

/* This is the structure that we have as value of the PrefixTable, and
 * represents the list of keys modified, and the list of clients that need
 * to be notified, for a given prefix. */
typedef struct bcastState {
    rax *keys;      /* Keys modified in the current event loop cycle.          rax类型，键记录当前已变更的Redis键，值指向变更Redis键的客户端 */
    rax *clients;   /* Clients subscribed to the notification events for this
                       prefix.                                                 rax类型，键记录所有关注该前缀的客户端（存的是指针，而非client id），值为NULL */
} bcastState;

/* Remove the tracking state from the client 'c'. Note that there is not much
 * to do for us here, if not to decrement the counter of the clients in
 * tracking mode, because we just store the ID of the client in the tracking
 * table, so we'll remove the ID reference in a lazy way. Otherwise when a
 * client with many entries in the table is removed, it would cost a lot of
 * time to do the cleanup. */
void disableTracking(client *c) {
    /* If this client is in broadcasting mode, we need to unsubscribe it
     * from all the prefixes it is registered to. */
    if (c->flags & CLIENT_TRACKING_BCAST) {
        raxIterator ri;
        raxStart(&ri,c->client_tracking_prefixes);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            bcastState *bs = raxFind(PrefixTable,ri.key,ri.key_len);
            serverAssert(bs != raxNotFound);
            raxRemove(bs->clients,(unsigned char*)&c,sizeof(c),NULL);
            /* Was it the last client? Remove the prefix from the
             * table. */
            if (raxSize(bs->clients) == 0) {
                raxFree(bs->clients);
                raxFree(bs->keys);
                zfree(bs);
                raxRemove(PrefixTable,ri.key,ri.key_len,NULL);
            }
        }
        raxStop(&ri);
        raxFree(c->client_tracking_prefixes);
        c->client_tracking_prefixes = NULL;
    }

    /* Clear flags and adjust the count. */
    if (c->flags & CLIENT_TRACKING) {
        server.tracking_clients--;
        c->flags &= ~(CLIENT_TRACKING|CLIENT_TRACKING_BROKEN_REDIR|
                      CLIENT_TRACKING_BCAST|CLIENT_TRACKING_OPTIN|
                      CLIENT_TRACKING_OPTOUT|CLIENT_TRACKING_CACHING|
                      CLIENT_TRACKING_NOLOOP);
    }
}

static int stringCheckPrefix(unsigned char *s1, size_t s1_len, unsigned char *s2, size_t s2_len) {
    size_t min_length = s1_len < s2_len ? s1_len : s2_len;
    return memcmp(s1,s2,min_length) == 0;   
}

/* Check if any of the provided prefixes collide with one another or
 * with an existing prefix for the client. A collision is defined as two 
 * prefixes that will emit an invalidation for the same key. If no prefix 
 * collision is found, 1 is return, otherwise 0 is returned and the client 
 * has an error emitted describing the error. */
int checkPrefixCollisionsOrReply(client *c, robj **prefixes, size_t numprefix) {
    for (size_t i = 0; i < numprefix; i++) {
        /* Check input list has no overlap with existing prefixes. */
        if (c->client_tracking_prefixes) {
            raxIterator ri;
            raxStart(&ri,c->client_tracking_prefixes);
            raxSeek(&ri,"^",NULL,0);
            while(raxNext(&ri)) {
                if (stringCheckPrefix(ri.key,ri.key_len,
                    prefixes[i]->ptr,sdslen(prefixes[i]->ptr))) 
                {
                    sds collision = sdsnewlen(ri.key,ri.key_len);
                    addReplyErrorFormat(c,
                        "Prefix '%s' overlaps with an existing prefix '%s'. "
                        "Prefixes for a single client must not overlap.",
                        (unsigned char *)prefixes[i]->ptr,
                        (unsigned char *)collision);
                    sdsfree(collision);
                    raxStop(&ri);
                    return 0;
                }
            }
            raxStop(&ri);
        }
        /* Check input has no overlap with itself. */
        for (size_t j = i + 1; j < numprefix; j++) {
            if (stringCheckPrefix(prefixes[i]->ptr,sdslen(prefixes[i]->ptr),
                prefixes[j]->ptr,sdslen(prefixes[j]->ptr)))
            {
                addReplyErrorFormat(c,
                    "Prefix '%s' overlaps with another provided prefix '%s'. "
                    "Prefixes for a single client must not overlap.",
                    (unsigned char *)prefixes[i]->ptr,
                    (unsigned char *)prefixes[j]->ptr);
                return i;
            }
        }
    }
    return 1;
}

/* Set the client 'c' to track the prefix 'prefix'. If the client 'c' is
 * already registered for the specified prefix, no operation is performed.
 * 设置客户端'c'来跟踪前缀'prefix'。如果客户端'c'已经注册了指定的前缀，则不执行任何操作。
 * */
void enableBcastTrackingForPrefix(client *c, char *prefix, size_t plen) {
    // 【1】 从prefix中查找该前缀对应的bcastState，如果不存在，则创建bcastState，并添加到 PrefixTable 中
    bcastState *bs = raxFind(PrefixTable,(unsigned char*)prefix,plen);
    /* If this is the first client subscribing to such prefix, create
     * the prefix in the table. */
    if (bs == raxNotFound) {
        bs = zmalloc(sizeof(*bs));
        bs->keys = raxNew();
        bs->clients = raxNew();
        raxInsert(PrefixTable,(unsigned char*)prefix,plen,bs,NULL);
    }
    // 【2】 将当前客户端添加到 bcastState.clients 中
    if (raxTryInsert(bs->clients,(unsigned char*)&c,sizeof(c),NULL,NULL)) {
        if (c->client_tracking_prefixes == NULL)
            c->client_tracking_prefixes = raxNew();
        raxInsert(c->client_tracking_prefixes,
                  (unsigned char*)prefix,plen,NULL,NULL);
    }
}

/* Enable the tracking state for the client 'c', and as a side effect allocates
 * the tracking table if needed. If the 'redirect_to' argument is non zero, the
 * invalidation messages for this client will be sent to the client ID
 * specified by the 'redirect_to' argument. Note that if such client will
 * eventually get freed, we'll send a message to the original client to
 * inform it of the condition. Multiple clients can redirect the invalidation
 * messages to the same client ID.
 * 为客户端'c'启用tracking状态，并在需要时分配tracking table。如果'redirect_to'参数不为零，该客户端的无效消息将被发送到'redirect_to'参数指定的客户端ID。
 * 请注意，如果这样的客户端最终被释放，我们将向原始客户端发送消息，通知它该情况。多个客户端可以将无效消息重定向到相同的客户端ID。
 * */
void enableTracking(client *c, uint64_t redirect_to, uint64_t options, robj **prefix, size_t numprefix) {
    if (!(c->flags & CLIENT_TRACKING)) server.tracking_clients++;
    // 【1】 客户端添加 CLIENT_TRACKING 标记，并清除其他标记
    c->flags |= CLIENT_TRACKING;
    c->flags &= ~(CLIENT_TRACKING_BROKEN_REDIR|CLIENT_TRACKING_BCAST|
                  CLIENT_TRACKING_OPTIN|CLIENT_TRACKING_OPTOUT|
                  CLIENT_TRACKING_NOLOOP);
    c->client_tracking_redirection = redirect_to;

    /* This may be the first client we ever enable. Create the tracking
     * table if it does not exist.
     * 这可能是我们启用的第一个客户端。如果跟踪表不存在，则创建跟踪表
     * */
    if (TrackingTable == NULL) {
        TrackingTable = raxNew();
        PrefixTable = raxNew();
        TrackingChannelName = createStringObject("__redis__:invalidate",20);
    }

    // 【2】如果开启了广播模式，则将客户端关注的前缀添加到 PrefixTable 中
    /* For broadcasting, set the list of prefixes in the client. */
    if (options & CLIENT_TRACKING_BCAST) {
        c->flags |= CLIENT_TRACKING_BCAST;
        // 如果没有前缀，则会添加一个空字符串作为前缀，这样在任务键变更之后，都会发送失效消息给当前客户端，可能会导致性能问题，所以在广播模式下通常使用 prefix 前缀标识客户端只关注指定前缀开头的键
        if (numprefix == 0) enableBcastTrackingForPrefix(c,"",0);
        for (size_t j = 0; j < numprefix; j++) {
            sds sdsprefix = prefix[j]->ptr;
            enableBcastTrackingForPrefix(c,sdsprefix,sdslen(sdsprefix));
        }
    }

    // 【3】
    /* Set the remaining flags that don't need any special handling. */
    c->flags |= options & (CLIENT_TRACKING_OPTIN|CLIENT_TRACKING_OPTOUT|
                           CLIENT_TRACKING_NOLOOP);
}

/* This function is called after the execution of a readonly command in the
 * case the client 'c' has keys tracking enabled and the tracking is not
 * in BCAST mode. It will populate the tracking invalidation table according
 * to the keys the user fetched, so that Redis will know what are the clients
 * that should receive an invalidation message with certain groups of keys
 * are modified.
 * 在客户端'c'启用了keys tracking且 不是BCAST模式的情况下，在执行只读命令后调用此函数。
 * 它将根据用户发送的key填充tracking失效表，这样Redis就会知道哪些客户端应该接收到某些key组被修改后的失效消息。
 * */
void trackingRememberKeys(client *c) {
    // 【1】 在optin模式下，客户端发送 client caching yes 后，客户端将打开 CLIENT_TRACKING_CACHING 标志。在optout模式下客户端发送client caching no命令后，客户端将打开 CLIENT_TRACKING_CACHING
    // 如果客户端开启optin模式或者optout模式，那么这里需要检查 CLIENT_TRACKING_CACHING 标志是否被打开。如果没有打开，则不需要记录命令键
    /* Return if we are in optin/out mode and the right CACHING command
     * was/wasn't given in order to modify the default behavior. */
    uint64_t optin = c->flags & CLIENT_TRACKING_OPTIN;
    uint64_t optout = c->flags & CLIENT_TRACKING_OPTOUT;
    uint64_t caching_given = c->flags & CLIENT_TRACKING_CACHING;
    if ((optin && !caching_given) || (optout && caching_given)) return;

    getKeysResult result = GETKEYS_RESULT_INIT;
    int numkeys = getKeysFromCommand(c->cmd,c->argv,c->argc,&result);
    if (!numkeys) {
        getKeysFreeResult(&result);
        return;
    }

    int *keys = result.keys;

    // 【2】 遍历命令中的所有key
    for(int j = 0; j < numkeys; j++) {
        int idx = keys[j];
        sds sdskey = c->argv[idx]->ptr;
        // 【3】 从 TrackingTable 中查找该 Redis key 对应的而客户端id（实际上是一个rax树，key为 client id，值为 NULL），如果没有则创建客户端 rax 再插入 TrackingTable 中。最后将当前客户端id插入客户端Rax中
        rax *ids = raxFind(TrackingTable,(unsigned char*)sdskey,sdslen(sdskey));
        if (ids == raxNotFound) {
            ids = raxNew();
            int inserted = raxTryInsert(TrackingTable,(unsigned char*)sdskey,
                                        sdslen(sdskey),ids, NULL);
            serverAssert(inserted == 1);
        }
        if (raxTryInsert(ids,(unsigned char*)&c->id,sizeof(c->id),NULL,NULL))
            TrackingTableTotalItems++;
    }
    getKeysFreeResult(&result);
}

/* Given a key name, this function sends an invalidation message in the
 * proper channel (depending on RESP version: PubSub or Push message) and
 * to the proper client (in case fo redirection), in the context of the
 * client 'c' with tracking enabled.
 *
 * In case the 'proto' argument is non zero, the function will assume that
 * 'keyname' points to a buffer of 'keylen' bytes already expressed in the
 * form of Redis RESP protocol. This is used for:
 * - In BCAST mode, to send an array of invalidated keys to all
 *   applicable clients
 * - Following a flush command, to send a single RESP NULL to indicate
 *   that all keys are now invalid.
 *
 * 给定一个key名，该函数将在正确的通道(取决于RESP版本:PubSub或Push消息)中发送失效消息，并在启用了tracking的客户端'c'上下文中发送到正确的客户端(如果是重定向)。
 * 如果'proto'参数不为零，该函数将假设'keyname'指向已经以Redis RESP协议形式表示的'keylen'字节的缓冲区。这用于：
 *  - 在BCAST模式下，向所有适用的客户端发送一组失效的key
 *  - 在flush命令之后，发送单个RESP NULL表示所有key现在都无效。
 *   */
void sendTrackingMessage(client *c, char *keyname, size_t keylen, int proto) {
    int using_redirection = 0;
    // 【1】 如果客户端开启了转发模式，则查找转发客户端，将c指针指向转发客户端。如果转发客户端已不存在（如连接断开），则打开客户端 CLIENT_TRACKING_BROKEN_REDIR 标志，并返回错误信息，退出函数
    if (c->client_tracking_redirection) {
        client *redir = lookupClientByID(c->client_tracking_redirection);
        if (!redir) {
            c->flags |= CLIENT_TRACKING_BROKEN_REDIR;
            /* We need to signal to the original connection that we
             * are unable to send invalidation messages to the redirected
             * connection, because the client no longer exist.
             * 我们需要向原始连接发出信号，表明我们无法向重定向的连接发送key失效消息，因为客户端不再存在。
             * */
            if (c->resp > 2) {
                addReplyPushLen(c,2);
                addReplyBulkCBuffer(c,"tracking-redir-broken",21);
                addReplyLongLong(c,c->client_tracking_redirection);
            }
            return;
        }
        c = redir;
        using_redirection = 1;
    }

    // 【2】 如果使用的是RESP 3协议，则直接推送失效消息
    /* Only send such info for clients in RESP version 3 or more. However
     * if redirection is active, and the connection we redirect to is
     * in Pub/Sub mode, we can support the feature with RESP 2 as well,
     * by sending Pub/Sub messages in the __redis__:invalidate channel.
     * 为RESP版本3或更高版本的客户端发送此类信息。但是，如果重定向是激活的，并且我们重定向到的连接处于PubSub模式，我们也可以通过在__redis__:invalidate通道中发送PubSub消息来支持RESP 2的功能。
     * */
    if (c->resp > 2) {
        addReplyPushLen(c,2);
        addReplyBulkCBuffer(c,"invalidate",10);
    } else if (using_redirection && c->flags & CLIENT_PUBSUB) {
        // 【3】 如果使用的是resp 2协议，并且重定向是激活的，并且我们重定向到的连接处于PubSub模式（转发的客户端正在使用SUBSCRIBE等命令等待消息），则发送消息到抓发客户端
        /* We use a static object to speedup things, however we assume
         * that addReplyPubsubMessage() will not take a reference. */
        addReplyPubsubMessage(c,TrackingChannelName,NULL);
    } else {
        /* If are here, the client is not using RESP3, nor is
         * redirecting to another client. We can't send anything to
         * it since RESP2 does not support push messages in the same
         * connection. */
        return;
    }

    // 【4】 前面2步只是写入对应的resp协议的前缀内容（如resp3的前缀内容为<2\r\n$10\r\ninvalidate）,这里写入key的当前值
    /* Send the "value" part, which is the array of keys. */
    if (proto) {
        addReplyProto(c,keyname,keylen);
    } else {
        addReplyArrayLen(c,1);
        addReplyBulkCBuffer(c,keyname,keylen);
    }
}

/* This function is called when a key is modified in Redis and in the case
 * we have at least one client with the BCAST mode enabled.
 * Its goal is to set the key in the right broadcast state if the key
 * matches one or more prefixes in the prefix table. Later when we
 * return to the event loop, we'll send invalidation messages to the
 * clients subscribed to each prefix.
 * 当一个键在Redis中被修改时，这个函数被调用，在这种情况下，我们至少有一个客户端启用了BCAST模式。
 * 它的目标是，如果键与PrefixTable表中的一个或多个前缀匹配，则将键设置为正确的广播状态。稍后返回事件循环时（beforeSleep），我们将向订阅每个前缀的客户端发送失效消息。
 * */
void trackingRememberKeyToBroadcast(client *c, char *keyname, size_t keylen) {
    raxIterator ri;
    raxStart(&ri,PrefixTable);
    raxSeek(&ri,"^",NULL,0);
    // 【1】 遍历 PrefixTable 表
    while(raxNext(&ri)) {
        // 【2】 检查变更的redis key是否以某个客户端关注的前缀开始，入股欧斯和，则添加该key到对应的 bcastState 中
        if (ri.key_len > keylen) continue;
        if (ri.key_len != 0 && memcmp(ri.key,keyname,ri.key_len) != 0)
            continue;
        bcastState *bs = ri.data;
        /* We insert the client pointer as associated value in the radix
         * tree. This way we know who was the client that did the last
         * change to the key, and can avoid sending the notification in the
         * case the client is in NOLOOP mode.
         * 我们将客户端指针作为关联值插入到基数树中。这样我们就知道谁是最后更改密钥的客户端，并且可以避免在客户端处于NOLOOP模式的情况下发送通知。
         * */
        raxInsert(bs->keys,(unsigned char*)keyname,keylen,c,NULL);
    }
    raxStop(&ri);
}

/* This function is called from signalModifiedKey() or other places in Redis
 * when a key changes value. In the context of keys tracking, our task here is
 * to send a notification to every client that may have keys about such caching
 * slot.
 *
 * Note that 'c' may be NULL in case the operation was performed outside the
 * context of a client modifying the database (for instance when we delete a
 * key because of expire).
 *
 * The last argument 'bcast' tells the function if it should also schedule
 * the key for broadcasting to clients in BCAST mode. This is the case when
 * the function is called from the Redis core once a key is modified, however
 * we also call the function in order to evict keys in the key table in case
 * of memory pressure: in that case the key didn't really change, so we want
 * just to notify the clients that are in the table for this key, that would
 * otherwise miss the fact we are no longer tracking the key for them.
 *
 * 当一个键改变值时，这个函数从signalModifiedKey()或Redis中的其他地方调用。在keys tracking的上下文中，我们这里的任务是向每个可能拥有关于此key缓存的客户端发送通知。
 *
 * 注意，如果操作是在客户端修改数据库的上下文中执行的(例如，当我们因为过期而删除键时)，'c'可能是NULL。
 *
 * 最后一个参数'bcast'告诉函数是否也应该安排键以bcast模式广播到客户端。这种情况下，当一个键被修改时，函数从Redis核心被调用，但是我们也调用这个函数，以便在内存压力的情况下驱逐键表中的键:在这种情况下，键并没有真正改变，
 * 所以我们只想通知表中的客户端这个键，否则我们就会错过我们不再跟踪键的事实。
 *
 * 这个函数会检查修改的key是否被缓存，如果是，则发送失效消息通知给客户端
 * */
void trackingInvalidateKey(client *c, robj *keyobj, int bcast) {
    // bcast 是否需要发送失效消息给广播模式下的客户端。这是因为客户端缓存功能是基于客户端（server.c/client）级别的，也就是说对于同一个key，有的客户端可能使用 client tracking on 开启了默认模式，并查询了这个key。而另个一客户端则开启了广播模式，并关注了符合这个key的前缀
    // 此时对于该key的变化，需要基于多种模式发送失效消息
    if (TrackingTable == NULL) return;

    unsigned char *key = (unsigned char*)keyobj->ptr;
    size_t keylen = sdslen(keyobj->ptr);

    // 【1】 调用 trackingRememberKeyToBroadcast 函数记录广播模式下待发送失效消息的键。Redis key变更时调用 trackingInvalidateKey 函数，bcast 固定为1
    if (bcast && raxSize(PrefixTable) > 0)
        // 广播模式下发送失效消息
        trackingRememberKeyToBroadcast(c,(char *)key,keylen);

    // 【2】 从 TrackingTable 中找到Redis键对应的客户端Rax。该Rax中存放了所有查询过该key的客户端id
    rax *ids = raxFind(TrackingTable,key,keylen);
    if (ids == raxNotFound) return;

    // 【3】 遍历上一步得到的客户端Rax
    raxIterator ri;
    raxStart(&ri,ids);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        uint64_t id;
        memcpy(&id,ri.key,sizeof(id));
        client *target = lookupClientByID(id);
        // 【4】 如果某个客户端已经断开连接、关闭了Tracking机制，或者开启了广播模式，则不发送失效消息
        /* Note that if the client is in BCAST mode, we don't want to
         * send invalidation messages that were pending in the case
         * previously the client was not in BCAST mode. This can happen if
         * TRACKING is enabled normally, and then the client switches to
         * BCAST mode.
         * 注意，如果客户端处于BCAST模式，我们不希望发送在之前客户端未处于BCAST模式的情况下挂起的无效消息。如果正常启用TRACKING，然后客户端切换到BCAST模式，就会发生这种情况。
         * */
        if (target == NULL ||
            !(target->flags & CLIENT_TRACKING)||
            target->flags & CLIENT_TRACKING_BCAST)
        {
            continue;
        }

        // 【5】 客户端开启了 NOLOOP 选项，并且该客户端就是变更了Redis键的客户端，则不发送失效消息
        /* If the client enabled the NOLOOP mode, don't send notifications
         * about keys changed by the client itself. */
        if (target->flags & CLIENT_TRACKING_NOLOOP &&
            target == c)
        {
            continue;
        }

        /* If target is current client, we need schedule key invalidation.
         * As the invalidation messages may be interleaved with command
         * response and should after command response
         * 如果目标是当前客户端，我们需要调度键失效。由于无效消息可能与命令响应交织在一起，并且应该在命令响应之后
         * */
        if (target == server.current_client){
            incrRefCount(keyobj);
            listAddNodeTail(server.tracking_pending_keys, keyobj);
        } else {
            // 【6】 发送失效消息
            sendTrackingMessage(target,(char *)keyobj->ptr,sdslen(keyobj->ptr),0);
        }
    }
    raxStop(&ri);

    /* Free the tracking table: we'll create the radix tree and populate it
     * again if more keys will be modified in this caching slot. */
    TrackingTableTotalItems -= raxSize(ids);
    raxFree(ids);
    // 【7】 从TrackingTable中删除该变更key内容。默认模式下，删除之后，如果该客户端不在查询这个key，则不会再收到该key的失效消息。
    // 但是如果该客户端再次查询这个key，则除了会在返回结果中 -> invalidate: 'key' 这样的失效消息之外，还会再次将这个key添加到 TrackingTable 中。所以其实执行一次client tracking on就够了
    raxRemove(TrackingTable,(unsigned char*)key,keylen,NULL);
}

void trackingHandlePendingKeyInvalidations() {
    if (!listLength(server.tracking_pending_keys)) return;

    listNode *ln;
    listIter li;

    listRewind(server.tracking_pending_keys,&li);
    while ((ln = listNext(&li)) != NULL) {
        robj *key = listNodeValue(ln);
        /* current_client maybe freed, so we need to send invalidation
         * message only when current_client is still alive */
        if (server.current_client != NULL)
            sendTrackingMessage(server.current_client,(char *)key->ptr,sdslen(key->ptr),0);
        decrRefCount(key);
    }
    listEmpty(server.tracking_pending_keys);
}

/* This function is called when one or all the Redis databases are
 * flushed. Caching keys are not specific for each DB but are global: 
 * currently what we do is send a special notification to clients with 
 * tracking enabled, sending a RESP NULL, which means, "all the keys", 
 * in order to avoid flooding clients with many invalidation messages 
 * for all the keys they may hold.
 */
void freeTrackingRadixTreeCallback(void *rt) {
    raxFree(rt);
}

void freeTrackingRadixTree(rax *rt) {
    raxFreeWithCallback(rt,freeTrackingRadixTreeCallback);
}

/* A RESP NULL is sent to indicate that all keys are invalid */
void trackingInvalidateKeysOnFlush(int async) {
    if (server.tracking_clients) {
        listNode *ln;
        listIter li;
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = listNodeValue(ln);
            if (c->flags & CLIENT_TRACKING) {
                sendTrackingMessage(c,shared.null[c->resp]->ptr,sdslen(shared.null[c->resp]->ptr),1);
            }
        }
    }

    /* In case of FLUSHALL, reclaim all the memory used by tracking. */
    if (TrackingTable) {
        if (async) {
            freeTrackingRadixTreeAsync(TrackingTable);
        } else {
            freeTrackingRadixTree(TrackingTable);
        }
        TrackingTable = raxNew();
        TrackingTableTotalItems = 0;
    }
}

/* Tracking forces Redis to remember information about which client may have
 * certain keys. In workloads where there are a lot of reads, but keys are
 * hardly modified, the amount of information we have to remember server side
 * could be a lot, with the number of keys being totally not bound.
 *
 * So Redis allows the user to configure a maximum number of keys for the
 * invalidation table. This function makes sure that we don't go over the
 * specified fill rate: if we are over, we can just evict informations about
 * a random key, and send invalidation messages to clients like if the key was
 * modified.
 * Tracking迫使Redis记住哪些客户端可能拥有某些key的信息。在有大量读操作，但键几乎不被修改的工作负载中，我们必须记住的服务器端信息量可能很大，而键的数量完全没有绑定。
 * 因此，Redis允许用户配置无效表的最大键数。这个函数确保我们不会超过指定的填充率:如果我们超过了，我们可以只是驱逐关于随机key的信息，并向客户端发送无效消息，就像密钥被修改一样。
 * */
void trackingLimitUsedSlots(void) {
    // 【1】 如果 server.tracking_table_max_keys == 0 ，则不主动清除记录键，该配置默认为 1000000.
    static unsigned int timeout_counter = 0;
    if (TrackingTable == NULL) return;
    if (server.tracking_table_max_keys == 0) return; /* No limits set. */
    // 【2】 如果 TrackingTable 键的数量小于或等于 server.tracking_table_max_keys 配置，则不需要执行清除操作，只需要重置 timeout_counter 计数。
    // timeout_counter 是一个静态全局变量，用于计算每轮清除键的数量
    size_t max_keys = server.tracking_table_max_keys;
    if (raxSize(TrackingTable) <= max_keys) {
        timeout_counter = 0;
        return; /* Limit not reached. */
    }

    // 【3】使用 timeout_counter 计算清除键的数量 effort ，作为本轮清除键的数量
    /* We have to invalidate a few keys to reach the limit again. The effort
     * we do here is proportional to the number of times we entered this
     * function and found that we are still over the limit.
     * 我们必须使几个键失效才能再次达到极限。我们在这里所做的努力与我们进入这个函数的次数成正比，并且发现我们仍然超过了极限。
     * */
    int effort = 100 * (timeout_counter+1);

    // 【4】 开始执行本轮清除操作，调用 raxRandomWalk 函数获取水机的记录键，调用 trackingInvalidateKey 函数删除记录键并发送失效消息。
    // 如果本路弄清楚操作未能执行完成，TrackingTable 键的数量已经小于或等于 server.tracking_table_max_keys 配置，则重置 timeout_counter 并退出函数。
    // 另外，这里调用 trackingInvalidateKey 函数时的bcast参数为0，代表不需要发送失效消息给广播模式下的客户但，因为这些被主动清除的key并没有变更。
    /* We just remove one key after another by using a random walk. */
    raxIterator ri;
    raxStart(&ri,TrackingTable);
    while(effort > 0) {
        effort--;
        raxSeek(&ri,"^",NULL,0);
        raxRandomWalk(&ri,0);
        if (raxEOF(&ri)) break;
        robj *keyobj = createStringObject((char*)ri.key,ri.key_len);
        trackingInvalidateKey(NULL,keyobj,0);
        decrRefCount(keyobj);
        if (raxSize(TrackingTable) <= max_keys) {
            timeout_counter = 0;
            raxStop(&ri);
            return; /* Return ASAP: we are again under the limit. */
        }
    }

    /* If we reach this point, we were not able to go under the configured
     * limit using the maximum effort we had for this run. */
    raxStop(&ri);
    // 【5】 执行本轮清除操作完成，TrackingTable中键的数量依然大于 server.tracking_table_max_keys 配置 ，则timeout_counter计数+1.
    // 可以看到，如果缪论清除操作执行完成，TrackingTable中键的数量仍未达到要求，则下一轮清除键的数量会增加。这样每论清除的键的数量会不断增加，直到 TrackingTable中键的数量达到要求才重置计数器
    timeout_counter++;
}

/* Generate Redis protocol for an array containing all the key names
 * in the 'keys' radix tree. If the client is not NULL, the list will not
 * include keys that were modified the last time by this client, in order
 * to implement the NOLOOP option.
 *
 * If the resultin array would be empty, NULL is returned instead.
 *
 * 生成一个包含“keys”基数树中所有键名的数组的Redis协议。如果客户端不是NULL，列表将不包括该客户端上次修改的键，以实现NOLOOP选项。
 *
 * 如果结果数组为空，则返回NULL。
 * */
sds trackingBuildBroadcastReply(client *c, rax *keys) {
    raxIterator ri;
    uint64_t count;

    if (c == NULL) {
        count = raxSize(keys);
    } else {
        // 如果客户端不是NULL，列表将不包括该客户端上次修改的键，以实现NOLOOP选项。
        count = 0;
        raxStart(&ri,keys);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            if (ri.data != c) count++;
        }
        raxStop(&ri);

        if (count == 0) return NULL;
    }

    /* Create the array reply with the list of keys once, then send
    * it to all the clients subscribed to this prefix. */
    char buf[32];
    size_t len = ll2string(buf,sizeof(buf),count);
    sds proto = sdsempty();
    proto = sdsMakeRoomFor(proto,count*15);
    proto = sdscatlen(proto,"*",1);
    proto = sdscatlen(proto,buf,len);
    proto = sdscatlen(proto,"\r\n",2);
    raxStart(&ri,keys);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        if (c && ri.data == c) continue;
        len = ll2string(buf,sizeof(buf),ri.key_len);
        proto = sdscatlen(proto,"$",1);
        proto = sdscatlen(proto,buf,len);
        proto = sdscatlen(proto,"\r\n",2);
        proto = sdscatlen(proto,ri.key,ri.key_len);
        proto = sdscatlen(proto,"\r\n",2);
    }
    raxStop(&ri);
    return proto;
}

/* This function will run the prefixes of clients in BCAST mode and
 * keys that were modified about each prefix, and will send the
 * notifications to each client in each prefix.
 * 该函数将遍历PrefixTable表， 将通知发送到每个前缀中的每个客户端。
 * */
void trackingBroadcastInvalidationMessages(void) {
    raxIterator ri, ri2;

    /* Return ASAP if there is nothing to do here. */
    if (TrackingTable == NULL || !server.tracking_clients) return;

    raxStart(&ri,PrefixTable);
    raxSeek(&ri,"^",NULL,0);

    // 【1】 遍历前缀表
    /* For each prefix... */
    while(raxNext(&ri)) {
        bcastState *bs = ri.data;

        // 【2】 如果某个 bcastState->keys 中存在已变更的键，则需要发送失效消息
        if (raxSize(bs->keys)) {
            /* Generate the common protocol for all the clients that are
             * not using the NOLOOP option.
             * 为没有使用NOLOOP选项的所有客户机生成公共协议
             * trackingBuildBroadcastReply 函数使用 bs->keys 中的key生成RESP协议的回复内容，第一个参数为客户端，如果变更Redis key的客户端是第一个参数指定的客户端，，则这些键不会添加到回复缓冲区内。
             * */
            sds proto = trackingBuildBroadcastReply(NULL,bs->keys);

            // 【3】 遍历 bs->clients 中所有的客户端，发送广播失效消息
            /* Send this array of keys to every client in the list. */
            raxStart(&ri2,bs->clients);
            raxSeek(&ri2,"^",NULL,0);
            while(raxNext(&ri2)) {
                client *c;
                memcpy(&c,ri2.key,sizeof(c));
                if (c->flags & CLIENT_TRACKING_NOLOOP) {
                    /* This client may have certain keys excluded. */
                    // 如果接收失效消息的客户端开启了noloop选项，则 trackingBuildBroadcastReply 函数的第一个参数需要设置为目标客户端，以过滤该客户端变更的redis key
                    sds adhoc = trackingBuildBroadcastReply(c,bs->keys);
                    if (adhoc) {
                        sendTrackingMessage(c,adhoc,sdslen(adhoc),1);
                        sdsfree(adhoc);
                    }
                } else {
                    sendTrackingMessage(c,proto,sdslen(proto),1);
                }
            }
            raxStop(&ri2);

            /* Clean up: we can remove everything from this state, because we
             * want to only track the new keys that will be accumulated starting
             * from now. */
            sdsfree(proto);
        }
        // 【4】 重置 bs->keys
        raxFree(bs->keys);
        bs->keys = raxNew();
    }
    raxStop(&ri);
}

/* This is just used in order to access the amount of used slots in the
 * tracking table. */
uint64_t trackingGetTotalItems(void) {
    return TrackingTableTotalItems;
}

uint64_t trackingGetTotalKeys(void) {
    if (TrackingTable == NULL) return 0;
    return raxSize(TrackingTable);
}

uint64_t trackingGetTotalPrefixes(void) {
    if (PrefixTable == NULL) return 0;
    return raxSize(PrefixTable);
}
