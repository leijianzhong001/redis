#ifndef __CLUSTER_H
#define __CLUSTER_H

/*-----------------------------------------------------------------------------
 * Redis cluster data structures, defines, exported API.
 *----------------------------------------------------------------------------*/

#define CLUSTER_SLOTS 16384
#define CLUSTER_OK 0          /* Everything looks ok */
#define CLUSTER_FAIL 1        /* The cluster can't work */
#define CLUSTER_NAMELEN 40    /* sha1 hex length */
#define CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */

/* The following defines are amount of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). */
#define CLUSTER_FAIL_REPORT_VALIDITY_MULT 2 /* Fail report validity. */
#define CLUSTER_FAIL_UNDO_TIME_MULT 2 /* Undo fail if master is back. */
#define CLUSTER_FAIL_UNDO_TIME_ADD 10 /* Some additional time. */
#define CLUSTER_FAILOVER_DELAY 5 /* Seconds */
#define CLUSTER_MF_TIMEOUT 5000 /* Milliseconds to do a manual failover. */
#define CLUSTER_MF_PAUSE_MULT 2 /* Master pause manual failover mult. */
#define CLUSTER_SLAVE_MIGRATION_DELAY 5000 /* Delay for slave migration. */

/* Redirection errors returned by getNodeByQuery(). */
#define CLUSTER_REDIR_NONE 0          /* Node can serve the request. */
#define CLUSTER_REDIR_CROSS_SLOT 1    /* -CROSSSLOT request. */
#define CLUSTER_REDIR_UNSTABLE 2      /* -TRYAGAIN redirection required */
#define CLUSTER_REDIR_ASK 3           /* -ASK redirection required. */
#define CLUSTER_REDIR_MOVED 4         /* -MOVED redirection required. */
#define CLUSTER_REDIR_DOWN_STATE 5    /* -CLUSTERDOWN, global state. */
#define CLUSTER_REDIR_DOWN_UNBOUND 6  /* -CLUSTERDOWN, unbound slot. */
#define CLUSTER_REDIR_DOWN_RO_STATE 7 /* -CLUSTERDOWN, allow reads. */

struct clusterNode;

/* clusterLink encapsulates everything needed to talk with a remote node. */
typedef struct clusterLink {
    mstime_t ctime;             /* Link creation time */
    connection *conn;           /* Connection to remote node */
    sds sndbuf;                 /* Packet send buffer                          数据包发送缓冲区 */
    char *rcvbuf;               /* Packet reception buffer                     包接收缓冲器 */
    size_t rcvbuf_len;          /* Used size of rcvbuf                         已使用的rvbuf大小 */
    size_t rcvbuf_alloc;        /* Allocated size of rcvbuf                    分配的rvbuf大小 */
    struct clusterNode *node;   /* Node related to this link if any, or NULL   与此链接相关的节点(如果有)，或NULL */
} clusterLink;

/* Cluster node flags and macros. */
#define CLUSTER_NODE_MASTER 1     /* The node is a master                      该节点是主节点 */
#define CLUSTER_NODE_SLAVE 2      /* The node is a slave                       该节点是从节点 */
#define CLUSTER_NODE_PFAIL 4      /* Failure? Need acknowledge                 该节点已主观下线 */
#define CLUSTER_NODE_FAIL 8       /* The node is believed to be malfunctioning 该节点已可客观下线 */
#define CLUSTER_NODE_MYSELF 16    /* This node is myself */
#define CLUSTER_NODE_HANDSHAKE 32 /* We have still to exchange the first ping */
#define CLUSTER_NODE_NOADDR   64  /* We don't know the address of this node */
#define CLUSTER_NODE_MEET 128     /* Send a MEET message to this node          要求进行握手的消息 */
#define CLUSTER_NODE_MIGRATE_TO 256 /* Master eligible for replica migration. */
#define CLUSTER_NODE_NOFAILOVER 512 /* Slave will not try to failover.         从服务器不会尝试故障转移 */
#define CLUSTER_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

#define nodeIsMaster(n) ((n)->flags & CLUSTER_NODE_MASTER)
#define nodeIsSlave(n) ((n)->flags & CLUSTER_NODE_SLAVE)
#define nodeInHandshake(n) ((n)->flags & CLUSTER_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->flags & CLUSTER_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->flags & CLUSTER_NODE_NOADDR)
#define nodeTimedOut(n) ((n)->flags & CLUSTER_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & CLUSTER_NODE_FAIL)
#define nodeCantFailover(n) ((n)->flags & CLUSTER_NODE_NOFAILOVER)

/* Reasons why a slave is not able to failover. */
#define CLUSTER_CANT_FAILOVER_NONE 0
#define CLUSTER_CANT_FAILOVER_DATA_AGE 1
#define CLUSTER_CANT_FAILOVER_WAITING_DELAY 2
#define CLUSTER_CANT_FAILOVER_EXPIRED 3
#define CLUSTER_CANT_FAILOVER_WAITING_VOTES 4
#define CLUSTER_CANT_FAILOVER_RELOG_PERIOD (60*5) /* seconds. */

/* clusterState todo_before_sleep flags. */
#define CLUSTER_TODO_HANDLE_FAILOVER (1<<0)
#define CLUSTER_TODO_UPDATE_STATE (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<3)
#define CLUSTER_TODO_HANDLE_MANUALFAILOVER (1<<4)

/* Message types.
 *
 * Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
#define CLUSTERMSG_TYPE_PING 0          /* Ping                                心跳消息 */
#define CLUSTERMSG_TYPE_PONG 1          /* Pong (reply to Ping)                心跳响应消息或广播消息 */
#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message           握手消息 */
#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing            节点客观下线广播消息 */
#define CLUSTERMSG_TYPE_PUBLISH 4       /* Pub/Sub Publish propagation */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover?              故障转移选举时的投票请求消息 */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote        故障转移选举时的同意投票响应消息 */
#define CLUSTERMSG_TYPE_UPDATE 7        /* Another node slots configuration */
#define CLUSTERMSG_TYPE_MFSTART 8       /* Pause clients for manual failover */
#define CLUSTERMSG_TYPE_MODULE 9        /* Module cluster API message. */
#define CLUSTERMSG_TYPE_COUNT 10        /* Total number of message types. */

/* Flags that a module can set in order to prevent certain Redis Cluster
 * features to be enabled. Useful when implementing a different distributed
 * system on top of Redis Cluster message bus, using modules. */
#define CLUSTER_MODULE_FLAG_NONE 0
#define CLUSTER_MODULE_FLAG_NO_FAILOVER (1<<1)
#define CLUSTER_MODULE_FLAG_NO_REDIRECTION (1<<2)

/* This structure represent elements of node->fail_reports. */
typedef struct clusterNodeFailReport {
    struct clusterNode *node;  /* Node reporting the failure condition.        报告该节点为主观下线的节点 */
    mstime_t time;             /* Time of the last report from this node.      最近收到下线报告的时间 */
} clusterNodeFailReport;

/* clusterNode 结构体负责存放CLuster节点实例的相关信息 */
typedef struct clusterNode {
    mstime_t ctime; /* Node object creation time. */
    char name[CLUSTER_NAMELEN]; /* Node name, hex string, sha1-size            节点名称，即节点id， 每个节点启动时都将该属性初始化为40字节的随机字符串， 作为节点的唯一标识 */
    int flags;      /* CLUSTER_NODE_...                                        flags: 节点标志，存储节点的状态、属性等。主要关注以下标志： CLUSTER_NODE_MASTER，CLUSTER_NODE_SLAVE,CLUSTER_NODE_PFAIL,CLUSTER_NODE_FAIL  */
    uint64_t configEpoch; /* Last configEpoch observed for this node           每个主节点自身维护一个配置纪元（clusterNode.configEpoch）标示当前主节点的版本， 所有主节点的配置纪元都不相等 ，从节点会复制主节点的配置纪元 */
    unsigned char slots[CLUSTER_SLOTS/8]; /* slots handled by this node        槽位位图， 记录该节点负责的槽位 */
    sds slots_info; /* Slots info represented by string. */
    int numslots;   /* Number of slots handled by this node */
    int numslaves;  /* Number of slave nodes, if this is a master */
    struct clusterNode **slaves; /* pointers to slave nodes                     该节点的从节点列表 */
    struct clusterNode *slaveof; /* pointer to the master node. Note that it
                                    may be NULL even if the node is a slave
                                    if we don't have the master node in our
                                    tables.                                    该节点的主节点实例。指向主节点的指针。注意，如果我们的表中没有主节点，即使节点是从节点，它也可能是NULL。*/
    mstime_t ping_sent;      /* Unix time we sent latest ping                  上次给该节点发送ping请求的时间 如果我们已经收到了PONG，那么node->ping_sent为0 */
    mstime_t pong_received;  /* Unix time we received the pong                 上次从该节点收到pong响应的时间 */
    mstime_t data_received;  /* Unix time we received any data                 上次从该节点收到任何响应数据的时间 */
    mstime_t fail_time;      /* Unix time when FAIL flag was set               节点下线时间 */
    mstime_t voted_time;     /* Last time we voted for a slave of this master  上次给该节点的投票时间 */
    mstime_t repl_offset_time;  /* Unix time we received offset for this node */
    mstime_t orphaned_time;     /* Starting time of orphaned master condition */
    long long repl_offset;      /* Last known repl offset for this node. */
    char ip[NET_IP_STR_LEN];  /* Latest known IP address of this node           节点ip地址 */
    int port;                   /* Latest known clients port (TLS or plain).    节点端口 */
    int pport;                  /* Latest known clients plaintext port. Only used
                                   if the main clients port is for TLS. */
    int cport;                  /* Latest known cluster port of this node.      节点总线端口 */
    clusterLink *link;          /* TCP/IP link with this node                   当前节点与该节点的连接 */
    list *fail_reports;         /* List of nodes signaling this as failing      当前节点的下线报告链表，记录所有判定该节点主观下线的主节点，用于进行客观下线统计。clusterNodeFailReport结构体， */
} clusterNode;

// Redis Cluster 中的每个节点都维护一份自己视角下的整个集群的状态，该状态的信息存储在clusterState结构体中
typedef struct clusterState {
    clusterNode *myself;  /* This node                                         节点自身实例 */
    uint64_t currentEpoch; /*                                                  整个集群维护一个全局的配置纪元（clusterState.currentEpoch），用于记录集群内所有主节点配置纪元的最大版本 */
    int state;            /* CLUSTER_OK, CLUSTER_FAIL, ...                     集群状态码，Cluster集群存在 CLUSTER_OK, CLUSTER_FAIL 等状态码 */
    int size;             /* Num of master nodes with at least one slot        至少有一个槽位的主节点个数 */
    dict *nodes;          /* Hash table of name -> clusterNode structures      集群节点实例字典，字典键为节点id, 字典值指向 clusterNode 结构体 */
    dict *nodes_black_list; /* Nodes we don't re-add for a few seconds. */
    clusterNode *migrating_slots_to[CLUSTER_SLOTS]; /*                         迁出槽位，数组元素不为空，代表该槽位数据正在从当前节点迁移到数组元素的指定节点. 数组下标代表槽位, 数组值代表要迁移到哪个节点 */
    clusterNode *importing_slots_from[CLUSTER_SLOTS]; /*                       迁入槽位，数组元素不为空，代表该槽位数组正从数组元素指定节点迁入当前节点, 数组下标代表槽位, 数组值代表该槽位从哪个节点迁入 */
    clusterNode *slots[CLUSTER_SLOTS]; /*                                      槽和节点映射数组。数组下标代表槽位，数组下标对应的值就是节点 */
    uint64_t slots_keys_count[CLUSTER_SLOTS]; /*                               每个槽位对应的key数量 */
    rax *slots_to_keys;
    /* The following fields are used to take the slave state on elections. */
    mstime_t failover_auth_time; /* Time of previous or next election.         本次选举的开始时间或下次选举的开始时间（本次选举已失败） ，集群最开始启动的时候，这个值为0 */
    int failover_auth_count;    /* Number of votes received so far.            到目前为止收到的票数 */
    int failover_auth_sent;     /* True if we already asked for votes.         本次选举是否已发送投票请求 */
    int failover_auth_rank;     /* This slave rank for current auth request.   节点优先级，该数值越大，节点优先级越低，故障转移时节点重新发起选举前等待时间越长 */
    uint64_t failover_auth_epoch; /* Epoch of the current election.            当前正在执行故障转移的纪元 */
    int cant_failover_reason;   /* Why a slave is currently not able to
                                   failover. See the CANT_FAILOVER_* macros. */
    /* Manual failover state in common. */
    mstime_t mf_end;            /* Manual failover time limit (ms unixtime).
                                   It is zero if there is no MF in progress.   手动故障转移时间限制(毫秒unix时间)。如果没有正在进行的手动故障转移，它是零。 */
    /* Manual failover state of master. */
    clusterNode *mf_slave;      /* Slave performing the manual failover.       正在执行手动故障转移的从节点。 */
    /* Manual failover state of slave. */
    long long mf_master_offset; /* Master offset the slave needs to start MF
                                   or -1 if still not received. */
    int mf_can_start;           /* If non-zero signal that the manual failover
                                   can start requesting masters vote. */
    /* The following fields are used by masters to take state on elections. */
    uint64_t lastVoteEpoch;     /* Epoch of the last vote granted.             当前节点最近一次投票同意的纪元 */
    int todo_before_sleep; /* Things to do in clusterBeforeSleep(). */
    /* Messages received and sent by type. */
    long long stats_bus_messages_sent[CLUSTERMSG_TYPE_COUNT];
    long long stats_bus_messages_received[CLUSTERMSG_TYPE_COUNT];
    long long stats_pfail_nodes;    /* Number of nodes in PFAIL status,
                                       excluding nodes without address.        处于PFAIL状态的节点数，不包括无地址的节点 */
} clusterState;

/* Redis cluster messages header */

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages.
 * clusterMsgDataGossip用于存放随机实例和主观下线节点的实例信息，主要用于 CLUSTERMSG_TYPE_MEET，CLUSTERMSG_TYPE_PING, CLUSTERMSG_TYPE_PONG 消息
 * */
typedef struct {
    char nodename[CLUSTER_NAMELEN]; /*                                         节点的nodeId */
    uint32_t ping_sent; /*                                                     最后一次向该节点发送ping消息时间 */
    uint32_t pong_received; /*                                                 最后一次接收该节点pong消息时间 */
    char ip[NET_IP_STR_LEN];  /* IP address last time it was seen              节点上次出现的IP地址 */
    uint16_t port;              /* base port last time it was seen             port */
    uint16_t cport;             /* cluster port last time it was seen          总线端口 */
    uint16_t flags;             /* node->flags copy                            该节点标识 */
    uint16_t pport;             /* plaintext-port, when base port is TLS */
    uint16_t notused1;
} clusterMsgDataGossip;

typedef struct {
    char nodename[CLUSTER_NAMELEN];
} clusterMsgDataFail;

typedef struct {
    uint32_t channel_len;
    uint32_t message_len;
    unsigned char bulk_data[8]; /* 8 bytes just as placeholder. */
} clusterMsgDataPublish;

typedef struct {
    uint64_t configEpoch; /* Config epoch of the specified instance. */
    char nodename[CLUSTER_NAMELEN]; /* Name of the slots owner. */
    unsigned char slots[CLUSTER_SLOTS/8]; /* Slots bitmap. */
} clusterMsgDataUpdate;

typedef struct {
    uint64_t module_id;     /* ID of the sender module. */
    uint32_t len;           /* ID of the sender module. */
    uint8_t type;           /* Type from 0 to 255. */
    unsigned char bulk_data[3]; /* 3 bytes just as placeholder. */
} clusterMsgModule;

union clusterMsgData {
    /* PING, MEET and PONG */
    struct {
        /* Array of N clusterMsgDataGossip structures
         * ping消息。消息体大部分时候是一个clusterMsgDataGossip类型的数组。clusterMsgDataGossip用于存放随机实例和主观下线节点的实例信息，主要用于 CLUSTERMSG_TYPE_MEET，CLUSTERMSG_TYPE_PING, CLUSTERMSG_TYPE_PONG 消息  */
        clusterMsgDataGossip gossip[1];
    } ping;

    /* FAIL  FAIL 消息体，节点客观下线通知消息，主要存放了客观下线的节点id， 用于 CLUSTERMSG_TYPE_FAIL 消息 */
    struct {
        clusterMsgDataFail about;
    } fail;

    /* PUBLISH */
    struct {
        clusterMsgDataPublish msg;
    } publish;

    /* UPDATE */
    struct {
        clusterMsgDataUpdate nodecfg;
    } update;

    /* MODULE */
    struct {
        clusterMsgModule msg;
    } module;
};

#define CLUSTER_PROTO_VER 1 /* Cluster bus protocol version. */
// 所有Gossip消息使用的消息头，内部包含了发送节点的系列信息
typedef struct {
    char sig[4];        /* Signature "RCmb" (Redis Cluster message bus).       信号标示。固定为Rcmb, 意为这是Cluster总线消息*/
    uint32_t totlen;    /* Total length of this message                        消息总长度 */
    uint16_t ver;       /* Protocol version, currently set to 1.               cluster消息协议版本，当前是1 */
    uint16_t port;      /* TCP base port number. */
    uint16_t type;      /* Message type                                        消息类型。详情见 CLUSTERMSG_TYPE_* */
    uint16_t count;     /* Only used for some kind of messages.                消息体包含的节点数量，仅用于meet,ping,ping消息类型 */
    uint64_t currentEpoch;  /* The epoch accordingly to the sending node.      发送节点最新配置纪元 */
    uint64_t configEpoch;   /* The config epoch if it's a master, or the last
                               epoch advertised by its master if it is a
                               slave.                                          发送节点最新写入文件的配置纪元 */
    uint64_t offset;    /* Master replication offset if node is a master or
                           processed replication offset if node is a slave.    如果节点是主节点，则主复制偏移量;如果节点是从节点，则已处理的复制偏移量 */
    char sender[CLUSTER_NAMELEN]; /* Name of the sender node                   发送节点名称， 即发送节点的nodeId */
    unsigned char myslots[CLUSTER_SLOTS/8]; /*                                 发送节点自己负责的槽位位图 */
    char slaveof[CLUSTER_NAMELEN]; /*                                          如果发送节点是从节点，记录对应主节点的nodeId */
    char myip[NET_IP_STR_LEN];    /* Sender IP, if not all zeroed.             发送节点ip地址 */
    char notused1[32];  /* 32 bytes reserved for future usage. */
    uint16_t pport;      /* Sender TCP plaintext port, if base port is TLS */
    uint16_t cport;      /* Sender TCP cluster bus port                        发送节点总线端口 */
    uint16_t flags;      /* Sender node flags                                  发送节点标识，区分主从角色，是否下线等 */
    unsigned char state; /* Cluster state from the POV of the sender           发送节点所处的集群状态 */
    unsigned char mflags[3]; /* Message flags: CLUSTERMSG_FLAG[012]_...        消息标识 */
    union clusterMsgData data; /*                                              发送节点消息体 */
} clusterMsg;

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg)-sizeof(union clusterMsgData))

/* Message flags better specify the packet content or are used to
 * provide some information about the node state. */
#define CLUSTERMSG_FLAG0_PAUSED (1<<0) /* Master paused for manual failover. */
#define CLUSTERMSG_FLAG0_FORCEACK (1<<1) /* Give ACK to AUTH_REQUEST even if
                                            master is up. */

/* ---------------------- API exported outside cluster.c -------------------- */
clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *ask);
int clusterRedirectBlockedClientIfNeeded(client *c);
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code);
unsigned long getClusterConnectionsCount(void);

#endif /* __CLUSTER_H */
