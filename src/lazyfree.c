#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "cluster.h"

static redisAtomic size_t lazyfree_objects = 0;
static redisAtomic size_t lazyfreed_objects = 0;

/* Release objects from the lazyfree thread. It's just decrRefCount()
 * updating the count of objects to release. */
void lazyfreeFreeObject(void *args[]) {
    robj *o = (robj *) args[0];
    decrRefCount(o);
    atomicDecr(lazyfree_objects,1);
    atomicIncr(lazyfreed_objects,1);
}

/* Release a database from the lazyfree thread. The 'db' pointer is the
 * database which was substituted with a fresh one in the main thread
 * when the database was logically deleted. */
void lazyfreeFreeDatabase(void *args[]) {
    dict *ht1 = (dict *) args[0];
    dict *ht2 = (dict *) args[1];

    size_t numkeys = dictSize(ht1);
    dictRelease(ht1);
    dictRelease(ht2);
    atomicDecr(lazyfree_objects,numkeys);
    atomicIncr(lazyfreed_objects,numkeys);
}

/* Release the skiplist mapping Redis Cluster keys to slots in the
 * lazyfree thread. */
void lazyfreeFreeSlotsMap(void *args[]) {
    rax *rt = args[0];
    size_t len = rt->numele;
    raxFree(rt);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* Release the key tracking table. */
void lazyFreeTrackingTable(void *args[]) {
    rax *rt = args[0];
    size_t len = rt->numele;
    freeTrackingRadixTree(rt);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

void lazyFreeLuaScripts(void *args[]) {
    dict *lua_scripts = args[0];
    long long len = dictSize(lua_scripts);
    dictRelease(lua_scripts);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* Return the number of currently pending objects to free. */
size_t lazyfreeGetPendingObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfree_objects,aux);
    return aux;
}

/* Return the number of objects that have been freed. */
size_t lazyfreeGetFreedObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfreed_objects,aux);
    return aux;
}

/* Return the amount of work needed in order to free an object.
 * The return value is not always the actual number of allocations the
 * object is composed of, but a number proportional to it.
 *
 * For strings the function always returns 1.
 *
 * For aggregated objects represented by hash tables or other data structures
 * the function just returns the number of elements the object is composed of.
 *
 * Objects composed of single allocations are always reported as having a
 * single item even if they are actually logical composed of multiple
 * elements.
 *
 * For lists the function returns the number of elements in the quicklist
 * representing the list.
 * 返回释放一个对象所需的工作量。返回值并不总是组成对象的实际分配数，而是与之成比例的数字。
 * 对于字符串，该函数总是返回1。
 * 对于由哈希表或其他数据结构表示的聚合对象，该函数仅返回组成该对象的元素的数量。
 * 由单个分配组成的对象总是报告为具有单个项，即使它们实际上是由多个元素逻辑组成的。
 *
 * 对于列表，该函数返回快速列表中表示列表的元素个数。
 * */
size_t lazyfreeGetFreeEffort(robj *key, robj *obj) {
    if (obj->type == OBJ_LIST) {
        quicklist *ql = obj->ptr;
        return ql->len;
    } else if (obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT) {
        dict *ht = obj->ptr;
        return dictSize(ht);
    } else if (obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST){
        zset *zs = obj->ptr;
        return zs->zsl->length;
    } else if (obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT) {
        dict *ht = obj->ptr;
        return dictSize(ht);
    } else if (obj->type == OBJ_STREAM) {
        size_t effort = 0;
        stream *s = obj->ptr;

        /* Make a best effort estimate to maintain constant runtime. Every macro
         * node in the Stream is one allocation. */
        effort += s->rax->numnodes;

        /* Every consumer group is an allocation and so are the entries in its
         * PEL. We use size of the first group's PEL as an estimate for all
         * others. */
        if (s->cgroups && raxSize(s->cgroups)) {
            raxIterator ri;
            streamCG *cg;
            raxStart(&ri,s->cgroups);
            raxSeek(&ri,"^",NULL,0);
            /* There must be at least one group so the following should always
             * work. */
            serverAssert(raxNext(&ri));
            cg = ri.data;
            effort += raxSize(s->cgroups)*(1+raxSize(cg->pel));
            raxStop(&ri);
        }
        return effort;
    } else if (obj->type == OBJ_MODULE) {
        moduleValue *mv = obj->ptr;
        moduleType *mt = mv->type;
        if (mt->free_effort != NULL) {
            size_t effort  = mt->free_effort(key,mv->value);
            /* If the module's free_effort returns 0, it will use asynchronous free
             memory by default */
            return effort == 0 ? ULONG_MAX : effort;
        } else {
            return 1;
        }
    } else {
        return 1; /* Everything else is a single allocation. */
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB.
 * If there are enough allocations to free the value object may be put into
 * a lazy free list instead of being freed synchronously. The lazy free list
 * will be reclaimed in a different bio.c thread.
 * 从数据库中删除键、值和关联的过期条目(如果有的话)。如果有足够的分配来释放值对象，则可以将其放入惰性释放列表中(延迟删除)，而不是同步释放。惰性空闲列表将在另一个bio.c线程中回收。
 * */
#define LAZYFREE_THRESHOLD 64
int dbAsyncDelete(redisDb *db, robj *key) {
    //【1】 如果过期字典中存在该key(说明设置了过期时间)，则先从过期字典中删除该key
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    if (dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);

    //【2】将该key从数据库字典中移除，返回键值对，这是还没有删除键值对对象
    /* If the value is composed of a few allocations, to free in a lazy way
     * is actually just slower... So under a certain limit we just free
     * the object synchronously. */
    dictEntry *de = dictUnlink(db->dict,key->ptr);
    if (de) {
        //【3】 计算该键值对的值对象的大小
        robj *val = dictGetVal(de);

        /* Tells the module that the key has been unlinked from the database. */
        moduleNotifyKeyUnlink(key,val);

        // 这里返回的实际上是元素个数，比如字符串总是返回1，集合元素则返回元素数量
        size_t free_effort = lazyfreeGetFreeEffort(key,val);

        //【4】 如果值对象的大小大于 LAZYFREE_THRESHOLD （64），并且该值对象只被当前一处引用，则执行如下操作，实现非阻塞删除：
        //  【4.1】 创建一个后台任务删除值对象，这些后台任务由后台线程处理
        //  【4.2】 将该键值对的值对象置为null, 保证主线程无法再访问该值对象
        // 另外大概解释一下这里为什么简单的按照元素个数来判定是否可以惰性删除。对于如字符串这样的单体对象来说，其最大为500mb, 并且一定是在一块连续的内存上，释放这样的内存对于redis来说并不会阻塞主线程。
        // 但是对于集合元素来说就是另一回事了。集合元素中的元素数量和大小理论上是没有限制的，另外最重要的是，这些元素可能存储在不同的内存块中，即在内存上是不连续的，在删除时要遍历所有的元素，释放其对应的内存空间。
        /* If releasing the object is too much work, do it in the background
         * by adding the object to the lazy free list.
         * Note that if the object is shared, to reclaim it now it is not
         * possible. This rarely happens, however sometimes the implementation
         * of parts of the Redis core may call incrRefCount() to protect
         * objects, and then call dbDelete(). In this case we'll fall
         * through and reach the dictFreeUnlinkedEntry() call, that will be
         * equivalent to just calling decrRefCount(). */
        if (free_effort > LAZYFREE_THRESHOLD && val->refcount == 1) {
            atomicIncr(lazyfree_objects,1);
            // 添加一个后台释放任务，执行函数为 lazyfreeFreeObject, 要释放的对象为 val
            bioCreateLazyFreeJob(lazyfreeFreeObject,1, val);
            // 置为NULL之后，主线程中所有该对象的引用都已经被清除，主线程将不再能够访问该对象，此时只有后台线程可以访问该对象，所以后台线程后续删除对象时并不需要进行线程同步操作。
            dictSetVal(db->dict,de,NULL);
        }
    }

    /* Release the key-val pair, or just the key if we set the val
     * field to NULL in order to lazy free it later.
     * 释放key-val对，或者如果我们将val字段设置为NULL，则只释放键，以便稍后惰性释放它。
     * */
    if (de) {
        //【5】 删除该键值对对象，并释放其内存空间。如果是非阻塞删除，那么这里的值对象引用已经被设置为NULL了，并不会阻塞当前线程
        dictFreeUnlinkedEntry(db->dict,de);
        if (server.cluster_enabled) slotToKeyDel(key->ptr);
        return 1;
    } else {
        return 0;
    }
}

/* Free an object, if the object is huge enough, free it in async way. */
void freeObjAsync(robj *key, robj *obj) {
    size_t free_effort = lazyfreeGetFreeEffort(key,obj);
    if (free_effort > LAZYFREE_THRESHOLD && obj->refcount == 1) {
        atomicIncr(lazyfree_objects,1);
        bioCreateLazyFreeJob(lazyfreeFreeObject,1,obj);
    } else {
        decrRefCount(obj);
    }
}

/* Empty a Redis DB asynchronously. What the function does actually is to
 * create a new empty set of hash tables and scheduling the old ones for
 * lazy freeing. */
void emptyDbAsync(redisDb *db) {
    dict *oldht1 = db->dict, *oldht2 = db->expires;
    db->dict = dictCreate(&dbDictType,NULL);
    db->expires = dictCreate(&dbExpiresDictType,NULL);
    atomicIncr(lazyfree_objects,dictSize(oldht1));
    bioCreateLazyFreeJob(lazyfreeFreeDatabase,2,oldht1,oldht2);
}

/* Release the radix tree mapping Redis Cluster keys to slots asynchronously. */
void freeSlotsToKeysMapAsync(rax *rt) {
    atomicIncr(lazyfree_objects,rt->numele);
    bioCreateLazyFreeJob(lazyfreeFreeSlotsMap,1,rt);
}

/* Free an object, if the object is huge enough, free it in async way. */
void freeTrackingRadixTreeAsync(rax *tracking) {
    atomicIncr(lazyfree_objects,tracking->numele);
    bioCreateLazyFreeJob(lazyFreeTrackingTable,1,tracking);
}

/* Free lua_scripts dict, if the dict is huge enough, free it in async way. */
void freeLuaScriptsAsync(dict *lua_scripts) {
    if (dictSize(lua_scripts) > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects,dictSize(lua_scripts));
        bioCreateLazyFreeJob(lazyFreeLuaScripts,1,lua_scripts);
    } else {
        dictRelease(lua_scripts);
    }
}
