#ifndef STREAM_H
#define STREAM_H

#include "rax.h"
#include "listpack.h"

/* Stream item ID: a 128 bit number composed of a milliseconds time and
 * a sequence counter. IDs generated in the same millisecond (or in a past
 * millisecond if the clock jumped backward) will use the millisecond time
 * of the latest generated ID and an incremented sequence. */
typedef struct streamID {
    uint64_t ms;        /* Unix time in milliseconds. 毫秒级别的时间戳*/
    uint64_t seq;       /* Sequence number. 序号，如果在1ms之内添加了多条消息，则递增序号*/
} streamID;

typedef struct stream {
    rax *rax;               /* The radix tree holding the stream.  存放消息内容，该rax中key为消息id, 值指向listpack */
    uint64_t length;        /* Number of elements inside this stream. 该消息流中消息的数量 */
    streamID last_id;       /* Zero if there are yet no items.  消息流中最新的消息id*/
    rax *cgroups;           /* Consumer groups dictionary: name -> streamCG  存放消息组，该rax中key为消费者名，值指向StreamCG变量*/
} stream;

/* We define an iterator to iterate stream items in an abstract way, without
 * caring about the radix tree + listpack representation. Technically speaking
 * the iterator is only used inside streamReplyWithRange(), so could just
 * be implemented inside the function, but practically there is the AOF
 * rewriting code that also needs to iterate the stream to emit the XADD
 * commands. */
typedef struct streamIterator {
    stream *stream;         /* The stream we are iterating. */
    streamID master_id;     /* ID of the master entry at listpack head. */
    uint64_t master_fields_count;       /* Master entries # of fields. */
    unsigned char *master_fields_start; /* Master entries start in listpack. */
    unsigned char *master_fields_ptr;   /* Master field to emit next. */
    int entry_flags;                    /* Flags of entry we are emitting. */
    int rev;                /* True if iterating end to start (reverse). */
    uint64_t start_key[2];  /* Start key as 128 bit big endian. */
    uint64_t end_key[2];    /* End key as 128 bit big endian. */
    raxIterator ri;         /* Rax iterator. */
    unsigned char *lp;      /* Current listpack. */
    unsigned char *lp_ele;  /* Current listpack cursor. */
    unsigned char *lp_flags; /* Current entry flags pointer. */
    /* Buffers used to hold the string of lpGet() when the element is
     * integer encoded, so that there is no string representation of the
     * element inside the listpack itself. */
    unsigned char field_buf[LP_INTBUF_SIZE];
    unsigned char value_buf[LP_INTBUF_SIZE];
} streamIterator;

/* Consumer group. */
typedef struct streamCG {
    streamID last_id;       /* Last delivered (not acknowledged) ID for this
                               group. Consumers that will just ask for more
                               messages will served with IDs > than this. 该消费组最新读取的消息id(已读取未确认)---lost_delivered_id */
    rax *pel;               /* Pending entries list. This is a radix tree that
                               has every message delivered to consumers (without
                               the NOACK option) that was yet not acknowledged
                               as processed. The key of the radix tree is the
                               ID as a 64 bit big endian number, while the
                               associated value is a streamNACK structure. 该消费组中所有待确认的消息， rax树的键是消息ID，而相关联的值是一个streamNACK结构 */
    rax *consumers;         /* A radix tree representing the consumers by name
                               and their associated representation in the form
                               of streamConsumer structures. 该消费组中所有的消费者，Rax类型，Rax键为消费者的名称，Rax值指向streamConsumer */
} streamCG;

/* A specific consumer in a consumer group.  */
typedef struct streamConsumer {
    mstime_t seen_time;         /* Last time this consumer was active. 该消费者上一次活跃时间 */
    sds name;                   /* Consumer name. This is how the consumer
                                   will be identified in the consumer group
                                   protocol. Case sensitive. 消费者名称，消费组中通过该名称区分消费者，name区分大小写 */
    rax *pel;                   /* Consumer specific pending entries list: all
                                   the pending messages delivered to this
                                   consumer not yet acknowledged. Keys are
                                   big endian message IDs, while values are
                                   the same streamNACK structure referenced
                                   in the "pel" of the consumer group structure
                                   itself, so the value is shared. 归属该消费者的所有待确认的消息. 键是大端序表示的消息id，而值是 streamCG结构中的“pel”中引用的streamNACK结构，因此值是共享的 */
} streamConsumer;

/* Pending (yet not acknowledged) message in a consumer group. */
typedef struct streamNACK {
    mstime_t delivery_time;     /* Last time this message was delivered. 消费发送给消费者的最新时间 */
    uint64_t delivery_count;    /* Number of times this message was delivered. 消息发送给消费者的次数 */
    streamConsumer *consumer;   /* The consumer this message was delivered to
                                   in the last delivery. 消息属于哪个消费者 */
} streamNACK;

/* Stream propagation informations, passed to functions in order to propagate
 * XCLAIM commands to AOF and slaves. */
typedef struct streamPropInfo {
    robj *keyname;
    robj *groupname;
} streamPropInfo;

/* Prototypes of exported APIs. */
struct client;

/* Flags for streamLookupConsumer */
#define SLC_NONE      0
#define SLC_NOCREAT   (1<<0) /* Do not create the consumer if it doesn't exist */
#define SLC_NOREFRESH (1<<1) /* Do not update consumer's seen-time */

stream *streamNew(void);
void freeStream(stream *s);
unsigned long streamLength(const robj *subject);
size_t streamReplyWithRange(client *c, stream *s, streamID *start, streamID *end, size_t count, int rev, streamCG *group, streamConsumer *consumer, int flags, streamPropInfo *spi);
void streamIteratorStart(streamIterator *si, stream *s, streamID *start, streamID *end, int rev);
int streamIteratorGetID(streamIterator *si, streamID *id, int64_t *numfields);
void streamIteratorGetField(streamIterator *si, unsigned char **fieldptr, unsigned char **valueptr, int64_t *fieldlen, int64_t *valuelen);
void streamIteratorRemoveEntry(streamIterator *si, streamID *current);
void streamIteratorStop(streamIterator *si);
streamCG *streamLookupCG(stream *s, sds groupname);
streamConsumer *streamLookupConsumer(streamCG *cg, sds name, int flags, int *created);
streamCG *streamCreateCG(stream *s, char *name, size_t namelen, streamID *id);
streamNACK *streamCreateNACK(streamConsumer *consumer);
void streamDecodeID(void *buf, streamID *id);
int streamCompareID(streamID *a, streamID *b);
void streamFreeNACK(streamNACK *na);
int streamIncrID(streamID *id);
int streamDecrID(streamID *id);
void streamPropagateConsumerCreation(client *c, robj *key, robj *groupname, sds consumername);
robj *streamDup(robj *o);
int streamValidateListpackIntegrity(unsigned char *lp, size_t size, int deep);
int streamParseID(const robj *o, streamID *id);
robj *createObjectFromStreamID(streamID *id);
int streamAppendItem(stream *s, robj **argv, int64_t numfields, streamID *added_id, streamID *use_id);
int streamDeleteItem(stream *s, streamID *id);
int64_t streamTrimByLength(stream *s, long long maxlen, int approx);
int64_t streamTrimByID(stream *s, streamID minid, int approx);

#endif
