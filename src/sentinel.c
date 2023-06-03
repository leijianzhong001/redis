/* Redis Sentinel implementation
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

#include "server.h"
#include "hiredis.h"
#ifdef USE_OPENSSL
#include "openssl/ssl.h"
#include "hiredis_ssl.h"
#endif
#include "async.h"

#include <ctype.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>

extern char **environ;

#ifdef USE_OPENSSL
extern SSL_CTX *redis_tls_ctx;
extern SSL_CTX *redis_tls_client_ctx;
#endif

#define REDIS_SENTINEL_PORT 26379

/* ======================== Sentinel global state =========================== */

/* Address object, used to describe an ip:port pair. */
typedef struct sentinelAddr {
    char *hostname;         /* Hostname OR address, as specified */
    char *ip;               /* Always a resolved address */
    int port;
} sentinelAddr;

/* A Sentinel Redis Instance object is monitoring. */
#define SRI_MASTER  (1<<0)  /*                                         当flag的值包含 SRI_MASTER 标志位时， 说明该节点是主节点*/
#define SRI_SLAVE   (1<<1)  /*                                         当flag的值包含 SRI_SLAVE 标志位时， 说明该节点是从节点*/
#define SRI_SENTINEL (1<<2) /*                                         当flag的值包含 SRI_SENTINEL 标志位时， 说明该节点是 Sentinel 节点*/
#define SRI_S_DOWN (1<<3)   /* Subjectively down (no quorum).          该节点已主观下线*/
#define SRI_O_DOWN (1<<4)   /* Objectively down (confirmed by others). 该节点已客观下线 */
#define SRI_MASTER_DOWN (1<<5) /* A Sentinel with this flag set thinks that
                                   its master is down.                 当flag的值包含 SRI_MASTER_DOWN 标志位时， sentinel认为该主节点已经宕机了 */
#define SRI_FAILOVER_IN_PROGRESS (1<<6) /* Failover is in progress for
                                           this master.                该主节点的故障转移正在进行中 */
#define SRI_PROMOTED (1<<7)            /* Slave selected for promotion.该节点被选为故障转移中的晋升节点 */
#define SRI_RECONF_SENT (1<<8)     /* SLAVEOF <newmaster> sent.        已发送 slaveof 命令给节点*/
#define SRI_RECONF_INPROG (1<<9)   /* Slave synchronization in progress. 正在建立主从关系*/
#define SRI_RECONF_DONE (1<<10)     /* Slave synchronized with new master. 主从关系建立完成*/
#define SRI_FORCE_FAILOVER (1<<11)  /* Force failover with master up. */
#define SRI_SCRIPT_KILL_SENT (1<<12) /* SCRIPT KILL already sent on -BUSY */

/* Note: times are in milliseconds. */
#define SENTINEL_INFO_PERIOD 10000
#define SENTINEL_PING_PERIOD 1000
#define SENTINEL_ASK_PERIOD 1000
#define SENTINEL_PUBLISH_PERIOD 2000
#define SENTINEL_DEFAULT_DOWN_AFTER 30000
#define SENTINEL_HELLO_CHANNEL "__sentinel__:hello"
#define SENTINEL_TILT_TRIGGER 2000
#define SENTINEL_TILT_PERIOD (SENTINEL_PING_PERIOD*30)
#define SENTINEL_DEFAULT_SLAVE_PRIORITY 100
#define SENTINEL_SLAVE_RECONF_TIMEOUT 10000
#define SENTINEL_DEFAULT_PARALLEL_SYNCS 1
#define SENTINEL_MIN_LINK_RECONNECT_PERIOD 15000
#define SENTINEL_DEFAULT_FAILOVER_TIMEOUT (60*3*1000)
#define SENTINEL_MAX_PENDING_COMMANDS 100
#define SENTINEL_ELECTION_TIMEOUT 10000
#define SENTINEL_MAX_DESYNC 1000
#define SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG 1
#define SENTINEL_DEFAULT_RESOLVE_HOSTNAMES 0
#define SENTINEL_DEFAULT_ANNOUNCE_HOSTNAMES 0

/* Failover machine different states. */
#define SENTINEL_FAILOVER_STATE_NONE 0  /* No failover in progress. */
#define SENTINEL_FAILOVER_STATE_WAIT_START 1  /* Wait for failover_start_time*/
#define SENTINEL_FAILOVER_STATE_SELECT_SLAVE 2 /* Select slave to promote */
#define SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE 3 /* Slave -> Master */
#define SENTINEL_FAILOVER_STATE_WAIT_PROMOTION 4 /* Wait slave to change role */
#define SENTINEL_FAILOVER_STATE_RECONF_SLAVES 5 /* SLAVEOF newmaster */
#define SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 6 /* Monitor promoted slave. */

#define SENTINEL_MASTER_LINK_STATUS_UP 0
#define SENTINEL_MASTER_LINK_STATUS_DOWN 1

/* Generic flags that can be used with different functions.
 * They use higher bits to avoid colliding with the function specific
 * flags. */
#define SENTINEL_NO_FLAGS 0
#define SENTINEL_GENERATE_EVENT (1<<16)
#define SENTINEL_LEADER (1<<17)
#define SENTINEL_OBSERVER (1<<18)

/* Script execution flags and limits. */
#define SENTINEL_SCRIPT_NONE 0
#define SENTINEL_SCRIPT_RUNNING 1
#define SENTINEL_SCRIPT_MAX_QUEUE 256
#define SENTINEL_SCRIPT_MAX_RUNNING 16
#define SENTINEL_SCRIPT_MAX_RUNTIME 60000 /* 60 seconds max exec time. */
#define SENTINEL_SCRIPT_MAX_RETRY 10
#define SENTINEL_SCRIPT_RETRY_DELAY 30000 /* 30 seconds between retries. */

/* SENTINEL SIMULATE-FAILURE command flags. */
#define SENTINEL_SIMFAILURE_NONE 0
#define SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION (1<<0)
#define SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION (1<<1)

/* The link to a sentinelRedisInstance. When we have the same set of Sentinels
 * monitoring many masters, we have different instances representing the
 * same Sentinels, one per master, and we need to share the hiredis connections
 * among them. Otherwise if 5 Sentinels are monitoring 100 masters we create
 * 500 outgoing connections instead of 5.
 *
 * So this structure represents a reference counted link in terms of the two
 * hiredis connections for commands and Pub/Sub, and the fields needed for
 * failure detection, since the ping/pong time are now local to the link: if
 * the link is available, the instance is available. This way we don't just
 * have 5 connections instead of 500, we also send 5 pings instead of 500.
 *
 * Links are shared only for Sentinels: master and slave instances have
 * a link with refcount = 1, always.
 *
 * 当前master到一个 sentinelRedisInstance 的链接。
 * 当我们有一组相同的哨兵监视许多master服务器时，我们有不同的实例代表相同的哨兵，每个主服务器一个，我们需要在它们之间共享租用连接。否则，如果5个哨兵监视100个主机，我们将创建500个传出连接，而不是5个。
 *
 * 因此，这个结构表示一个引用计数的链接.
 * 根据两个分别用于commands和Pub/Sub的租用连接（即下面结构体的cc和pc），以及故障检测所需的字段(即下面ping/pong相关的字段)， 因为ping/pong使用的时间现在是连接的本地化时间:如果链接可用，则实例可用。这样我们不仅有5个连接而不是500个，我们还发送了5个ping而不是500个。
 *
 * 链接只对哨兵的sentinelRedisInstance共享: 主实例和从实例总是有一个 refcount = 1的链接。
 *
 *
 * 解释一下上面这段话：
 * 全局对象sentinel中包含了一个masters字典，该字典中存储了当前sentinel监控的所有master的信息，其中每个master都使用一个 sentinelRedisInstance 结构体表示。
 * 而在每个master的信息中，会有一个link字段，存储该master节点与当前sentinel节点的连接信息。这个连接信息并不共享，而是每个master都有一个独立的到当前sentinel的instanceLink连接信息。
 * 另外在每个master的信息中，也都会保存在当前sentinel视角下，该master的所有slave信息，以及sentinel信息，分别对应当前master信息中(sentinelRedisInstance)的 slaves 和 sentinels 字典。
 *      其中slave字典的key为slave的ip:port, 值是一个sentinelRedisInstance结构体。这个从节点结构体也有link字段，表示该从节点到当前sentinel的连接，这个连接信息并不共享，而是每个slave都有独立的一个到当前sentinel的instanceLink连接信息。
 *      而sentinel字典的key为sentinel的myid属性，值也是一个sentinelRedisInstance结构体。这个sentinel节点的结构体也有link字段，表示该sentinel到当前sentinel的连接，这个连接信息并不共享，而是每个sentinel都有独立的一个到当前sentinel的instanceLink连接信息。
 * 那前面说的共享到底是什么意思呢？
 *      其实说的是，sentinel字典中的每个sentinel到当前sentinel的连接，这个连接对于每个master来说是共享的。比如当前sentinel集群节点数量为5，并且监控了100
 *      个master节点, 那么全局sentinel对象的master字典中就会有100个master信息，而每个master信息中的sentinels字典中就会有4个sentinel信息，每个sentinel信息中
 *      都会有一个表示该sentinel到当前sentinel的连接信息link.
 *      如果每个master的sentinel字典中sentinel都有一个独立的到当前sentinel的连接对象，那么总共就会建立400个连接对象, 这显然是很浪费的，因为这些对象都是一样的,都是sentinel->当前sentinel的连接.
 *
 * 所以从这些数据的组织结构中也可以看出,sentinel和sentinel, 以及sentinel和从节点之间是没有直接的联系的(不是说没有连接,而是没有直接的数据层面的联系), 所有的其他的sentinel信息和从节点信息都是从master信息中获取的(通过info命令以及hello频道)。
 * */
typedef struct instanceLink {
    int refcount;          /* Number of sentinelRedisInstance owners. */
    int disconnected;      /* Non-zero if we need to reconnect cc or pc. */
    int pending_commands;  /* Number of commands sent waiting for a reply. */
    redisAsyncContext *cc; /* Hiredis context for commands.                    命令连接。Sentinel节点需要与监控的主从节点，集群其他sentinel节点建立命令连接，以便给这些节点发送命令 */
    redisAsyncContext *pc; /* Hiredis context for Pub / Sub.                   订阅连接。Sentinel节点只与主从节点建立订阅连接，并从特定频道中接收其他Sentinel节点发送的数据 */
    mstime_t cc_conn_time; /* cc connection time. */
    mstime_t pc_conn_time; /* pc connection time. */
    mstime_t pc_last_activity; /* Last time we received any message.           上次收到该节点频道数据的时间 */
    mstime_t last_avail_time; /* Last time the instance replied to ping with
                                 a reply we consider valid.                    上次收到该节点ping命令响应的时间 */
    mstime_t act_ping_time;   /* Time at which the last pending ping (no pong
                                 received after it) was sent. This field is
                                 set to 0 when a pong is received, and set again
                                 to the current time if the value is 0 and a new
                                 ping is sent.                                 上次给该节点发送（但还未收到响应）ping命令的时间，当收到响应时会重置为0*/
    mstime_t last_ping_time;  /* Time at which we sent the last ping. This is
                                 only used to avoid sending too many pings
                                 during failure. Idle time is computed using
                                 the act_ping_time field.                      上次发送该节点ping命令的时间 */
    mstime_t last_pong_time;  /* Last time the instance replied to ping,
                                 whatever the reply was. That's used to check
                                 if the link is idle and must be reconnected. */
    mstime_t last_reconn_time;  /* Last reconnection attempt performed when
                                   the link was down. */
} instanceLink;

/**
 * sentinelRedisInstance 结构体负责存储sentinel集群监控的redis实例的主从节点信息，以及其他Sentinel节点的实例数据。
 * 也就是无论是sentinel信息，还是主节点或者从节点信息，都是使用这个结构体来存储
 */
typedef struct sentinelRedisInstance {
    int flags;      /* See SRI_... defines                                     节点标志位，存储改节点的状态、属性等信息，具体可见SRI_开头的常量定义. 具体记录方式是使用低1位的值来表示是否为主节点, 使用低2位的值来表示是否位从节点*/
    char *name;     /* Master name from the point of view of this sentinel.    节点名称，也是sentinel字典或slaves字典的键。在主节点中他是实例名称，在从节点中他是ip:port, 在sentinel节点中它是myid属性*/
    char *runid;    /* Run ID of this instance, or unique ID if is a Sentinel. 作为节点唯一标识的随机字符串，sentinel节点使用myid属性， 主从节点从INFO响应中获取runId属性作为该值，每个节点都会在启动时初始化redisServer.runid属性作为节点标识（initServerConfig函数），并在INFO响应中返回该属性 */
    uint64_t config_epoch;  /* Configuration epoch.                            要写入配置文件中的配置纪元,也即是当前sentinel的配置纪元。 在进行故障转移向其他节点请求投票时, 只有配置纪元大于故障转移master的配置纪元,其他节点才会投票给当前节点 */
    sentinelAddr *addr; /* Master host.                                        节点的网络地址， 包括ip地址和端口信息 */
    instanceLink *link; /* Link to the instance, may be shared for Sentinels.  instanceLink结构体， 存储该节点（master或slave）与当前sentinel节点的连接信息 */
    mstime_t last_pub_time;   /* Last time we sent hello via Pub/Sub. */
    mstime_t last_hello_time; /* Only used if SRI_SENTINEL is set. Last time
                                 we received a hello from this Sentinel
                                 via Pub/Sub. */
    mstime_t last_master_down_reply_time; /* Time of last reply to
                                             SENTINEL is-master-down command. */
    mstime_t s_down_since_time; /* Subjectively down since time. */
    mstime_t o_down_since_time; /* Objectively down since time. */
    mstime_t down_after_period; /* Consider it down after that period.         存储sentinel down-after-milliseconds 配置，用于判定节点是否已经主观下线*/
    mstime_t info_refresh;  /* Time at which we received INFO output from it.  我们从它接收INFO输出的时间 */
    dict *renamed_commands;     /* Commands renamed in this instance:
                                   Sentinel will use the alternative commands
                                   mapped on this table to send things like
                                   SLAVEOF, CONFING, INFO, ... */

    /* Role and the first time we observed it.
     * This is useful in order to delay replacing what the instance reports
     * with our own configuration. We need to always wait some time in order
     * to give a chance to the leader to report the new configuration before
     * we do silly things.
     * 我们第一次观察到的角色。
     * 这有助于用我们自己的配置延迟替换实例报告的内容。我们总是需要等待一段时间，以便在我们做蠢事之前给leader一个报告新配置的机会。
     * */
    int role_reported;
    mstime_t role_reported_time;
    mstime_t slave_conf_change_time; /* Last time slave master addr changed. */

    /* Master specific. */
    dict *sentinels;    /* Other sentinels monitoring the same master.         主节点专有字段。存放集群中其他sentinel节点信息， 注意该字典并不包含当前sentinel节点实例. sentinels字典的信息来源是__sentinel__:hello频道*/
    dict *slaves;       /* Slaves for this master instance.                    主节点专有字段。slaves字典，存放该主节点下的所有从节点 slaves字典的信息来源是发到主节点上的info命令*/
    unsigned int quorum;/* Number of sentinels that need to agree on failure.  存储达成客观宕机所需票数 */
    int parallel_syncs; /* How many slaves to reconfigure at same time.        存储sentinel parallel-syncs配置的值，用于指定故障转移过程中最多有多少个从节点同时与晋升的主节点建立主从关系 */
    char *auth_pass;    /* Password to use for AUTH against master & replica. */
    char *auth_user;    /* Username for ACLs AUTH against master & replica. */

    /* Slave specific. */
    mstime_t master_link_down_time; /* Slave replication link down time. */
    int slave_priority; /* Slave priority according to its INFO output.        从节点专有字段。从节点优先级，用于在故障转移时选择要晋升的从节点*/
    int replica_announced; /* Replica announcing according to its INFO output. */
    mstime_t slave_reconf_sent_time; /* Time at which we sent SLAVE OF <new> */
    struct sentinelRedisInstance *master; /* Master instance if it's slave. */
    char *slave_master_host;    /* Master host as reported by INFO */
    int slave_master_port;      /* Master port as reported by INFO */
    int slave_master_link_status; /* Master link status as reported by INFO    从节点专有字段。主从连接状态，记录info响应中的master_link_status属性*/
    unsigned long long slave_repl_offset; /* Slave replication offset.         从节点专有字段。主从复制偏移量， 记录info响应中的 slave_repl_offset 属性*/
    /* Failover */
    char *leader;       /* If this is a master instance, this is the runid of
                           the Sentinel that should perform the failover. If
                           this is a Sentinel, this is the runid of the Sentinel
                           that this Sentinel voted as leader.                 故障转移专用字段。如果当前节点为master, 则这个字段存储最近一次执行故障转移的sentinel节点的runid. 如果当前节点是sentinel, 则该字段存储获得当前sentinel节点最新投票的sentinel节点的myid属性 */
    uint64_t leader_epoch; /* Epoch of the 'leader' field.                     获得当前sentinel节点最新投票的sentinel节点的纪元，即上面的leader的配置纪元. 这个字段用于保证目标主节点不会被优先级较低(配置纪元较低)的节点所影响. 对于所有配置纪元低于目标主节点的leader_epoch的投票请求,当前sentinel节点将拒绝投票  */
    uint64_t failover_epoch; /* Epoch of the currently started failover.       本次执行故障转移的配置纪元 */
    int failover_state; /* See SENTINEL_FAILOVER_STATE_* defines.              故障转移状态。 故障转移过程中需要根据不同的状态执行不同的逻辑。*/
    mstime_t failover_state_change_time;
    mstime_t failover_start_time;   /* Last failover attempt start time.       failover_start_time属性记录了当前sentinel节点上次故障转移的开始时间或者上次给其他sentinel节点投票的时间 */

    /**
     * failover-timeout通常被解释成故障转移超时时间，但实际上它作用于故障转移的各个阶段：
     *    a）选出合适从节点。
     *    b）晋升选出的从节点为主节点。
     *    c）命令其余从节点复制新的主节点。
     *    d）等待原主节点恢复后命令它去复制新的主节点。
     * failover-timeout的作用具体体现在四个方面：
     *    1）如果Redis Sentinel对一个主节点故障转移失败，那么下次再对该主节点做故障转移的起始时间是failover-timeout的2倍。
     *    2）在b）阶段时，如果Sentinel节点向a）阶段选出来的从节点执行slaveof no one一直失败（例如该从节点此时出现故障），当此过程超过 failover-timeout时，则故障转移失败
     *    3）在b）阶段如果执行成功，Sentinel节点还会执行info命令来确认a） 阶段选出来的节点确实晋升为主节点，如果此过程执行时间超过 failover-timeout 时，则故障转移失败。
     *    4）如果c）阶段执行时间超过了 failover-timeout（不包含复制时间）， 则故障转移失败。注意即使超过了这个时间，Sentinel节点也会最终配置从节点去同步最新的主节点。
     */
    mstime_t failover_timeout;      /* Max time to refresh failover state.     存储sentinel failover-timeout配置，指定故障转移最大超时时间 */
    mstime_t failover_delay_logged; /* For what failover_start_time value we
                                       logged the failover delay.              failover_delay_logged 故障转移延迟, 即下次什么时候爱是进行故障转移 */
    struct sentinelRedisInstance *promoted_slave; /* Promoted slave instance.  用于保存本次故障转移选择出来的从节点 */
    /* Scripts executed to notify admin or reconfigure clients: when they
     * are set to NULL no script is executed. */
    char *notification_script;
    char *client_reconfig_script;
    sds info; /* cached INFO output */
} sentinelRedisInstance;

/* Main state.
 * 每个sentinel都维护一份自己视角下的当前sentinel集群状态， 该状态信息存储在sentinelState结构体中
 * */
struct sentinelState {
    char myid[CONFIG_RUN_ID_SIZE+1]; /* This sentinel ID.  sentinelId， 用于唯一的区分一个sentinel集群中不同的sentinel节点。 这个id会体现在sentinel的配置文件中*/
    uint64_t current_epoch;         /* Current epoch. sentinel集群当前的配置纪元， 用户故障转移时使用raft算法选举leader节点*/
    dict *masters;      /* Dictionary of master sentinelRedisInstances.
                           Key is the instance name, value is the
                           sentinelRedisInstance structure pointer.  redis实例的主节点字典，key是实例名称，value是 sentinelRedisInstance 结构体指针*/
    int tilt;           /* Are we in TILT mode?  是否处于tilt模式。 Tilt模式是一种保护机制，处于该模式下`Sentinel`会认为检测失败的能力已经不再可信， 除了发送必要的`PING`及`INFO`命令外，不会主动做其他操作*/
    int running_scripts;    /* Number of scripts in execution right now. */
    mstime_t tilt_start_time;       /* When TITL started. */
    mstime_t previous_time;         /* Last time we ran the time handler. */
    list *scripts_queue;            /* Queue of user scripts to execute. */
    char *announce_ip;  /* IP addr that is gossiped to other sentinels if
                           not NULL. */
    int announce_port;  /* Port that is gossiped to other sentinels if
                           non zero. */
    unsigned long simfailure_flags; /* Failures simulation. SENTINEL SIMULATE-FAILURE 命令是 Redis Sentinel（哨兵）提供的一种用于模拟故障的命令。它可以用来测试和验证Redis Sentinel在发现主节点或从节点故障时的行为和处理机制。
                                                            具体来说，当我们执行 SENTINEL SIMULATE-FAILURE <master-name> 命令时，Redis Sentinel会将名为 <master-name> 的主节点标记为 DOWN 状态，这样就会触发自动故障转移流程，Redis Sentinel 将尝试选举一个新的主节点，并将其他从节点切换到新的主节点上工作。
                                                            需要注意的是，SENTINEL SIMULATE-FAILURE 命令并不会真正地使主节点或从节点进入故障状态，而只是模拟了一个故障事件。这个命令的主要作用是帮助我们测试和验证Redis Sentinel在实际场景中的表现，以便更好地了解其工作原理和特性。*/
    int deny_scripts_reconfig; /* Allow SENTINEL SET ... to change script
                                  paths at runtime? */
    char *sentinel_auth_pass;    /* Password to use for AUTH against other sentinel */
    char *sentinel_auth_user;    /* Username for ACLs AUTH against other sentinel. */
    int resolve_hostnames;       /* Support use of hostnames, assuming DNS is well configured. */
    int announce_hostnames;      /* Announce hostnames instead of IPs when we have them. */
} sentinel;

/* A script execution job. */
typedef struct sentinelScriptJob {
    int flags;              /* Script job flags: SENTINEL_SCRIPT_* */
    int retry_num;          /* Number of times we tried to execute it. */
    char **argv;            /* Arguments to call the script. */
    mstime_t start_time;    /* Script execution time if the script is running,
                               otherwise 0 if we are allowed to retry the
                               execution at any time. If the script is not
                               running and it's not 0, it means: do not run
                               before the specified time. */
    pid_t pid;              /* Script execution pid. */
} sentinelScriptJob;

/* ======================= hiredis ae.c adapters =============================
 * Note: this implementation is taken from hiredis/adapters/ae.h, however
 * we have our modified copy for Sentinel in order to use our allocator
 * and to have full control over how the adapter works. */

typedef struct redisAeEvents {
    redisAsyncContext *context;
    aeEventLoop *loop;
    int fd;
    int reading, writing;
} redisAeEvents;

static void redisAeReadEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAsyncHandleRead(e->context);
}

static void redisAeWriteEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAsyncHandleWrite(e->context);
}

static void redisAeAddRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->reading) {
        e->reading = 1;
        aeCreateFileEvent(loop,e->fd,AE_READABLE,redisAeReadEvent,e);
    }
}

static void redisAeDelRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->reading) {
        e->reading = 0;
        aeDeleteFileEvent(loop,e->fd,AE_READABLE);
    }
}

static void redisAeAddWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->writing) {
        e->writing = 1;
        aeCreateFileEvent(loop,e->fd,AE_WRITABLE,redisAeWriteEvent,e);
    }
}

static void redisAeDelWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->writing) {
        e->writing = 0;
        aeDeleteFileEvent(loop,e->fd,AE_WRITABLE);
    }
}

static void redisAeCleanup(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAeDelRead(privdata);
    redisAeDelWrite(privdata);
    zfree(e);
}

static int redisAeAttach(aeEventLoop *loop, redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisAeEvents *e;

    /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return C_ERR;

    /* Create container for context and r/w events */
    e = (redisAeEvents*)zmalloc(sizeof(*e));
    e->context = ac;
    e->loop = loop;
    e->fd = c->fd;
    e->reading = e->writing = 0;

    /* Register functions to start/stop listening for events */
    ac->ev.addRead = redisAeAddRead;
    ac->ev.delRead = redisAeDelRead;
    ac->ev.addWrite = redisAeAddWrite;
    ac->ev.delWrite = redisAeDelWrite;
    ac->ev.cleanup = redisAeCleanup;
    ac->ev.data = e;

    return C_OK;
}

/* ============================= Prototypes ================================= */

void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status);
void sentinelDisconnectCallback(const redisAsyncContext *c, int status);
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata);
sentinelRedisInstance *sentinelGetMasterByName(char *name);
char *sentinelGetSubjectiveLeader(sentinelRedisInstance *master);
char *sentinelGetObjectiveLeader(sentinelRedisInstance *master);
int yesnotoi(char *s);
void instanceLinkConnectionError(const redisAsyncContext *c);
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri);
void sentinelAbortFailover(sentinelRedisInstance *ri);
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri, const char *fmt, ...);
sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master);
void sentinelScheduleScriptExecution(char *path, ...);
void sentinelStartFailover(sentinelRedisInstance *master);
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata);
int sentinelSendSlaveOf(sentinelRedisInstance *ri, const sentinelAddr *addr);
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch);
void sentinelFlushConfig(void);
void sentinelGenerateInitialMonitorEvents(void);
int sentinelSendPing(sentinelRedisInstance *ri);
int sentinelForceHelloUpdateForMaster(sentinelRedisInstance *master);
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid);
void sentinelSimFailureCrash(void);

/* ========================= Dictionary types =============================== */

uint64_t dictSdsHash(const void *key);
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);
void releaseSentinelRedisInstance(sentinelRedisInstance *ri);

void dictInstancesValDestructor (void *privdata, void *obj) {
    UNUSED(privdata);
    releaseSentinelRedisInstance(obj);
}

/* Instance name (sds) -> instance (sentinelRedisInstance pointer)
 *
 * also used for: sentinelRedisInstance->sentinels dictionary that maps
 * sentinels ip:port to last seen time in Pub/Sub hello message. */
dictType instancesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    dictInstancesValDestructor,/* val destructor */
    NULL                       /* allow to expand */
};

/* Instance runid (sds) -> votes (long casted to void*)
 *
 * This is useful into sentinelGetObjectiveLeader() function in order to
 * count the votes and understand who is the leader. */
dictType leaderVotesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

/* Instance renamed commands table. */
dictType renamedCommandsDictType = {
    dictSdsCaseHash,           /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCaseCompare,     /* key compare */
    dictSdsDestructor,         /* key destructor */
    dictSdsDestructor,         /* val destructor */
    NULL                       /* allow to expand */
};

/* =========================== Initialization =============================== */

void sentinelCommand(client *c);
void sentinelInfoCommand(client *c);
void sentinelSetCommand(client *c);
void sentinelPublishCommand(client *c);
void sentinelRoleCommand(client *c);
void sentinelConfigGetCommand(client *c);
void sentinelConfigSetCommand(client *c);

struct redisCommand sentinelcmds[] = {
    {"ping",pingCommand,1,"fast @connection",0,NULL,0,0,0,0,0},
    {"sentinel",sentinelCommand,-2,"admin",0,NULL,0,0,0,0,0},
    {"subscribe",subscribeCommand,-2,"pub-sub",0,NULL,0,0,0,0,0},
    {"unsubscribe",unsubscribeCommand,-1,"pub-sub",0,NULL,0,0,0,0,0},
    {"psubscribe",psubscribeCommand,-2,"pub-sub",0,NULL,0,0,0,0,0},
    {"punsubscribe",punsubscribeCommand,-1,"pub-sub",0,NULL,0,0,0,0,0},
    {"publish",sentinelPublishCommand,3,"pub-sub fast",0,NULL,0,0,0,0,0},
    {"info",sentinelInfoCommand,-1,"random @dangerous",0,NULL,0,0,0,0,0},
    {"role",sentinelRoleCommand,1,"fast read-only @dangerous",0,NULL,0,0,0,0,0},
    {"client",clientCommand,-2,"admin random @connection",0,NULL,0,0,0,0,0},
    {"shutdown",shutdownCommand,-1,"admin",0,NULL,0,0,0,0,0},
    {"auth",authCommand,-2,"no-auth fast @connection",0,NULL,0,0,0,0,0},
    {"hello",helloCommand,-1,"no-auth fast @connection",0,NULL,0,0,0,0,0},
    {"acl",aclCommand,-2,"admin",0,NULL,0,0,0,0,0,0},
    {"command",commandCommand,-1, "random @connection", 0,NULL,0,0,0,0,0,0}
};

/* this array is used for sentinel config lookup, which need to be loaded
 * before monitoring masters config to avoid dependency issues */
const char *preMonitorCfgName[] = { 
    "announce-ip",
    "announce-port",
    "deny-scripts-reconfig",
    "sentinel-user",
    "sentinel-pass",
    "current-epoch",
    "myid",
    "resolve-hostnames",
    "announce-hostnames"
};

/* This function overwrites a few normal Redis config default with Sentinel
 * specific defaults. */
void initSentinelConfig(void) {
    server.port = REDIS_SENTINEL_PORT;
    server.protected_mode = 0; /* Sentinel must be exposed. */
}

void freeSentinelLoadQueueEntry(void *item);

/* Perform the Sentinel mode initialization. */
void initSentinel(void) {
    unsigned int j;

    /* Remove usual Redis commands from the command table, then just add
     * the SENTINEL command. */
    // 【1】 清空redis命令字典,只添加Sentinel相关的命令
    dictEmpty(server.commands,NULL);
    dictEmpty(server.orig_commands,NULL);
    ACLClearCommandID();
    for (j = 0; j < sizeof(sentinelcmds)/sizeof(sentinelcmds[0]); j++) {
        int retval;
        struct redisCommand *cmd = sentinelcmds+j;
        cmd->id = ACLGetCommandID(cmd->name); /* Assign the ID used for ACL. */
        retval = dictAdd(server.commands, sdsnew(cmd->name), cmd);
        serverAssert(retval == DICT_OK);
        retval = dictAdd(server.orig_commands, sdsnew(cmd->name), cmd);
        serverAssert(retval == DICT_OK);

        /* Translate the command string flags description into an actual
         * set of flags. */
        if (populateCommandTableParseFlags(cmd,cmd->sflags) == C_ERR)
            serverPanic("Unsupported command flag");
    }

    // 【2】 初始化全局变量Sentinel
    /* Initialize various data structures. */
    sentinel.current_epoch = 0;
    sentinel.masters = dictCreate(&instancesDictType,NULL);
    sentinel.tilt = 0;
    sentinel.tilt_start_time = 0;
    sentinel.previous_time = mstime();
    sentinel.running_scripts = 0;
    sentinel.scripts_queue = listCreate();
    sentinel.announce_ip = NULL;
    sentinel.announce_port = 0;
    sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
    sentinel.deny_scripts_reconfig = SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG;
    sentinel.sentinel_auth_pass = NULL;
    sentinel.sentinel_auth_user = NULL;
    sentinel.resolve_hostnames = SENTINEL_DEFAULT_RESOLVE_HOSTNAMES;
    sentinel.announce_hostnames = SENTINEL_DEFAULT_ANNOUNCE_HOSTNAMES;
    memset(sentinel.myid,0,sizeof(sentinel.myid));
    server.sentinel_config = NULL;
}

/* This function is for checking whether sentinel config file has been set,
 * also checking whether we have write permissions. */
void sentinelCheckConfigFile(void) {
    if (server.configfile == NULL) {
        serverLog(LL_WARNING,
            "Sentinel needs config file on disk to save state. Exiting...");
        exit(1);
    } else if (access(server.configfile,W_OK) == -1) {
        serverLog(LL_WARNING,
            "Sentinel config file %s is not writable: %s. Exiting...",
            server.configfile,strerror(errno));
        exit(1);
    }
}

/* This function gets called when the server is in Sentinel mode, started,
 * loaded the configuration, and is ready for normal operations.
 * 当服务器处于Sentinel模式, 启动、加载配置并准备进行正常操作时，调用该函数。
 * */
void sentinelIsRunning(void) {
    int j;

    /* If this Sentinel has yet no ID set in the configuration file, we
     * pick a random one and persist the config on disk. From now on this
     * will be this Sentinel ID across restarts.
     * 如果这个Sentinel还没有在配置文件中设置ID，我们将随机选择一个，并将配置保存在磁盘上。从现在起，这将是重新启动时的哨兵ID。
     * */
    for (j = 0; j < CONFIG_RUN_ID_SIZE; j++)
        if (sentinel.myid[j] != 0) break;

    if (j == CONFIG_RUN_ID_SIZE) {
        /* Pick ID and persist the config. 生成myid*/
        getRandomHexChars(sentinel.myid,CONFIG_RUN_ID_SIZE);
        sentinelFlushConfig();
    }

    /* Log its ID to make debugging of issues simpler. */
    serverLog(LL_WARNING,"Sentinel ID is %s", sentinel.myid);

    /* We want to generate a +monitor event for every configured master
     * at startup.
     * 我们希望在启动时为每个配置的主服务器生成一个+monitor事件。
     * */
    sentinelGenerateInitialMonitorEvents();
}

/* ============================== sentinelAddr ============================== */

/* Create a sentinelAddr object and return it on success.
 * On error NULL is returned and errno is set to:
 *  ENOENT: Can't resolve the hostname, unless accept_unresolved is non-zero.
 *  EINVAL: Invalid port number.
 */
sentinelAddr *createSentinelAddr(char *hostname, int port, int is_accept_unresolved) {
    char ip[NET_IP_STR_LEN];
    sentinelAddr *sa;

    if (port < 0 || port > 65535) {
        errno = EINVAL;
        return NULL;
    }
    if (anetResolve(NULL,hostname,ip,sizeof(ip),
                    sentinel.resolve_hostnames ? ANET_NONE : ANET_IP_ONLY) == ANET_ERR) {
        serverLog(LL_WARNING, "Failed to resolve hostname '%s'", hostname);
        if (sentinel.resolve_hostnames && is_accept_unresolved) {
            ip[0] = '\0';
        }
        else {
            errno = ENOENT;
            return NULL;
        }
    }
    sa = zmalloc(sizeof(*sa));
    sa->hostname = sdsnew(hostname);
    sa->ip = sdsnew(ip);
    sa->port = port;
    return sa;
}

/* Return a duplicate of the source address. */
sentinelAddr *dupSentinelAddr(sentinelAddr *src) {
    sentinelAddr *sa;

    sa = zmalloc(sizeof(*sa));
    sa->hostname = sdsnew(src->hostname);
    sa->ip = sdsnew(src->ip);
    sa->port = src->port;
    return sa;
}

/* Free a Sentinel address. Can't fail. */
void releaseSentinelAddr(sentinelAddr *sa) {
    sdsfree(sa->hostname);
    sdsfree(sa->ip);
    zfree(sa);
}

/* Return non-zero if two addresses are equal. */
int sentinelAddrIsEqual(sentinelAddr *a, sentinelAddr *b) {
    return a->port == b->port && !strcasecmp(a->ip,b->ip);
}

/* Return non-zero if the two addresses are equal, either by address
 * or by hostname if they could not have been resolved.
 */
int sentinelAddrOrHostnameEqual(sentinelAddr *a, sentinelAddr *b) {
    return a->port == b->port &&
            (!strcmp(a->ip, b->ip)  ||
            !strcasecmp(a->hostname, b->hostname));
}

/* Return non-zero if a hostname matches an address. */
int sentinelAddrEqualsHostname(sentinelAddr *a, char *hostname) {
    char ip[NET_IP_STR_LEN];

    /* We always resolve the hostname and compare it to the address */
    if (anetResolve(NULL, hostname, ip, sizeof(ip),
                    sentinel.resolve_hostnames ? ANET_NONE : ANET_IP_ONLY) == ANET_ERR)
        return 0;
    return !strcasecmp(a->ip, ip);
}

const char *announceSentinelAddr(const sentinelAddr *a) {
    return sentinel.announce_hostnames ? a->hostname : a->ip;
}

/* Return an allocated sds with hostname/address:port. IPv6
 * addresses are bracketed the same way anetFormatAddr() does.
 */
sds announceSentinelAddrAndPort(const sentinelAddr *a) {
    const char *addr = announceSentinelAddr(a);
    if (strchr(addr, ':') != NULL)
        return sdscatprintf(sdsempty(), "[%s]:%d", addr, a->port);
    else
        return sdscatprintf(sdsempty(), "%s:%d", addr, a->port);
}

/* =========================== Events notification ========================== */

/* Send an event to log, pub/sub, user notification script.
 *
 * 'level' is the log level for logging. Only LL_WARNING events will trigger
 * the execution of the user notification script.
 *
 * 'type' is the message type, also used as a pub/sub channel name.
 *
 * 'ri', is the redis instance target of this event if applicable, and is
 * used to obtain the path of the notification script to execute.
 *
 * The remaining arguments are printf-alike.
 * If the format specifier starts with the two characters "%@" then ri is
 * not NULL, and the message is prefixed with an instance identifier in the
 * following format:
 *
 *  <instance type> <instance name> <ip> <port>
 *
 *  If the instance type is not master, than the additional string is
 *  added to specify the originating master:
 *
 *  @ <master name> <master ip> <master port>
 *
 *  Any other specifier after "%@" is processed by printf itself.
 */
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri,
                   const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];
    robj *channel, *payload;

    /* Handle %@ */
    if (fmt[0] == '%' && fmt[1] == '@') {
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                                         NULL : ri->master;

        if (master) {
            snprintf(msg, sizeof(msg), "%s %s %s %d @ %s %s %d",
                sentinelRedisInstanceTypeStr(ri),
                ri->name, announceSentinelAddr(ri->addr), ri->addr->port,
                master->name, announceSentinelAddr(master->addr), master->addr->port);
        } else {
            snprintf(msg, sizeof(msg), "%s %s %s %d",
                sentinelRedisInstanceTypeStr(ri),
                ri->name, announceSentinelAddr(ri->addr), ri->addr->port);
        }
        fmt += 2;
    } else {
        msg[0] = '\0';
    }

    /* Use vsprintf for the rest of the formatting if any. */
    if (fmt[0] != '\0') {
        va_start(ap, fmt);
        vsnprintf(msg+strlen(msg), sizeof(msg)-strlen(msg), fmt, ap);
        va_end(ap);
    }

    /* Log the message if the log level allows it to be logged. */
    if (level >= server.verbosity)
        serverLog(level,"%s %s",type,msg);

    /* Publish the message via Pub/Sub if it's not a debugging one. */
    if (level != LL_DEBUG) {
        channel = createStringObject(type,strlen(type));
        payload = createStringObject(msg,strlen(msg));
        pubsubPublishMessage(channel,payload);
        decrRefCount(channel);
        decrRefCount(payload);
    }

    /* Call the notification script if applicable. */
    if (level == LL_WARNING && ri != NULL) {
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                                         ri : ri->master;
        if (master && master->notification_script) {
            sentinelScheduleScriptExecution(master->notification_script,
                type,msg,NULL);
        }
    }
}

/* This function is called only at startup and is used to generate a
 * +monitor event for every configured master. The same events are also
 * generated when a master to monitor is added at runtime via the
 * SENTINEL MONITOR command. */
void sentinelGenerateInitialMonitorEvents(void) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        sentinelEvent(LL_WARNING,"+monitor",ri,"%@ quorum %d",ri->quorum);
    }
    dictReleaseIterator(di);
}

/* ============================ script execution ============================ */

/* Release a script job structure and all the associated data. */
void sentinelReleaseScriptJob(sentinelScriptJob *sj) {
    int j = 0;

    while(sj->argv[j]) sdsfree(sj->argv[j++]);
    zfree(sj->argv);
    zfree(sj);
}

#define SENTINEL_SCRIPT_MAX_ARGS 16
void sentinelScheduleScriptExecution(char *path, ...) {
    va_list ap;
    char *argv[SENTINEL_SCRIPT_MAX_ARGS+1];
    int argc = 1;
    sentinelScriptJob *sj;

    va_start(ap, path);
    while(argc < SENTINEL_SCRIPT_MAX_ARGS) {
        argv[argc] = va_arg(ap,char*);
        if (!argv[argc]) break;
        argv[argc] = sdsnew(argv[argc]); /* Copy the string. */
        argc++;
    }
    va_end(ap);
    argv[0] = sdsnew(path);

    sj = zmalloc(sizeof(*sj));
    sj->flags = SENTINEL_SCRIPT_NONE;
    sj->retry_num = 0;
    sj->argv = zmalloc(sizeof(char*)*(argc+1));
    sj->start_time = 0;
    sj->pid = 0;
    memcpy(sj->argv,argv,sizeof(char*)*(argc+1));

    listAddNodeTail(sentinel.scripts_queue,sj);

    /* Remove the oldest non running script if we already hit the limit. */
    if (listLength(sentinel.scripts_queue) > SENTINEL_SCRIPT_MAX_QUEUE) {
        listNode *ln;
        listIter li;

        listRewind(sentinel.scripts_queue,&li);
        while ((ln = listNext(&li)) != NULL) {
            sj = ln->value;

            if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;
            /* The first node is the oldest as we add on tail. */
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
            break;
        }
        serverAssert(listLength(sentinel.scripts_queue) <=
                    SENTINEL_SCRIPT_MAX_QUEUE);
    }
}

/* Lookup a script in the scripts queue via pid, and returns the list node
 * (so that we can easily remove it from the queue if needed). */
listNode *sentinelGetScriptListNodeByPid(pid_t pid) {
    listNode *ln;
    listIter li;

    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if ((sj->flags & SENTINEL_SCRIPT_RUNNING) && sj->pid == pid)
            return ln;
    }
    return NULL;
}

/* Run pending scripts if we are not already at max number of running
 * scripts. */
void sentinelRunPendingScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    /* Find jobs that are not running and run them, from the top to the
     * tail of the queue, so we run older jobs first. */
    listRewind(sentinel.scripts_queue,&li);
    while (sentinel.running_scripts < SENTINEL_SCRIPT_MAX_RUNNING &&
           (ln = listNext(&li)) != NULL)
    {
        sentinelScriptJob *sj = ln->value;
        pid_t pid;

        /* Skip if already running. */
        if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;

        /* Skip if it's a retry, but not enough time has elapsed. */
        if (sj->start_time && sj->start_time > now) continue;

        sj->flags |= SENTINEL_SCRIPT_RUNNING;
        sj->start_time = mstime();
        sj->retry_num++;
        pid = fork();

        if (pid == -1) {
            /* Parent (fork error).
             * We report fork errors as signal 99, in order to unify the
             * reporting with other kind of errors. */
            sentinelEvent(LL_WARNING,"-script-error",NULL,
                          "%s %d %d", sj->argv[0], 99, 0);
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
        } else if (pid == 0) {
            /* Child */
            tlsCleanup();
            execve(sj->argv[0],sj->argv,environ);
            /* If we are here an error occurred. */
            _exit(2); /* Don't retry execution. */
        } else {
            sentinel.running_scripts++;
            sj->pid = pid;
            sentinelEvent(LL_DEBUG,"+script-child",NULL,"%ld",(long)pid);
        }
    }
}

/* How much to delay the execution of a script that we need to retry after
 * an error?
 *
 * We double the retry delay for every further retry we do. So for instance
 * if RETRY_DELAY is set to 30 seconds and the max number of retries is 10
 * starting from the second attempt to execute the script the delays are:
 * 30 sec, 60 sec, 2 min, 4 min, 8 min, 16 min, 32 min, 64 min, 128 min. */
mstime_t sentinelScriptRetryDelay(int retry_num) {
    mstime_t delay = SENTINEL_SCRIPT_RETRY_DELAY;

    while (retry_num-- > 1) delay *= 2;
    return delay;
}

/* Check for scripts that terminated, and remove them from the queue if the
 * script terminated successfully. If instead the script was terminated by
 * a signal, or returned exit code "1", it is scheduled to run again if
 * the max number of retries did not already elapsed. */
void sentinelCollectTerminatedScripts(void) {
    int statloc;
    pid_t pid;

    while ((pid = waitpid(-1, &statloc, WNOHANG)) > 0) {
        int exitcode = WEXITSTATUS(statloc);
        int bysignal = 0;
        listNode *ln;
        sentinelScriptJob *sj;

        if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);
        sentinelEvent(LL_DEBUG,"-script-child",NULL,"%ld %d %d",
            (long)pid, exitcode, bysignal);

        ln = sentinelGetScriptListNodeByPid(pid);
        if (ln == NULL) {
            serverLog(LL_WARNING,"waitpid() returned a pid (%ld) we can't find in our scripts execution queue!", (long)pid);
            continue;
        }
        sj = ln->value;

        /* If the script was terminated by a signal or returns an
         * exit code of "1" (that means: please retry), we reschedule it
         * if the max number of retries is not already reached. */
        if ((bysignal || exitcode == 1) &&
            sj->retry_num != SENTINEL_SCRIPT_MAX_RETRY)
        {
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
            sj->start_time = mstime() +
                             sentinelScriptRetryDelay(sj->retry_num);
        } else {
            /* Otherwise let's remove the script, but log the event if the
             * execution did not terminated in the best of the ways. */
            if (bysignal || exitcode != 0) {
                sentinelEvent(LL_WARNING,"-script-error",NULL,
                              "%s %d %d", sj->argv[0], bysignal, exitcode);
            }
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
        }
        sentinel.running_scripts--;
    }
}

/* Kill scripts in timeout, they'll be collected by the
 * sentinelCollectTerminatedScripts() function. */
void sentinelKillTimedoutScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if (sj->flags & SENTINEL_SCRIPT_RUNNING &&
            (now - sj->start_time) > SENTINEL_SCRIPT_MAX_RUNTIME)
        {
            sentinelEvent(LL_WARNING,"-script-timeout",NULL,"%s %ld",
                sj->argv[0], (long)sj->pid);
            kill(sj->pid,SIGKILL);
        }
    }
}

/* Implements SENTINEL PENDING-SCRIPTS command. */
void sentinelPendingScriptsCommand(client *c) {
    listNode *ln;
    listIter li;

    addReplyArrayLen(c,listLength(sentinel.scripts_queue));
    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;
        int j = 0;

        addReplyMapLen(c,5);

        addReplyBulkCString(c,"argv");
        while (sj->argv[j]) j++;
        addReplyArrayLen(c,j);
        j = 0;
        while (sj->argv[j]) addReplyBulkCString(c,sj->argv[j++]);

        addReplyBulkCString(c,"flags");
        addReplyBulkCString(c,
            (sj->flags & SENTINEL_SCRIPT_RUNNING) ? "running" : "scheduled");

        addReplyBulkCString(c,"pid");
        addReplyBulkLongLong(c,sj->pid);

        if (sj->flags & SENTINEL_SCRIPT_RUNNING) {
            addReplyBulkCString(c,"run-time");
            addReplyBulkLongLong(c,mstime() - sj->start_time);
        } else {
            mstime_t delay = sj->start_time ? (sj->start_time-mstime()) : 0;
            if (delay < 0) delay = 0;
            addReplyBulkCString(c,"run-delay");
            addReplyBulkLongLong(c,delay);
        }

        addReplyBulkCString(c,"retry-num");
        addReplyBulkLongLong(c,sj->retry_num);
    }
}

/* This function calls, if any, the client reconfiguration script with the
 * following parameters:
 *
 * <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
 *
 * It is called every time a failover is performed.
 *
 * <state> is currently always "failover".
 * <role> is either "leader" or "observer".
 *
 * from/to fields are respectively master -> promoted slave addresses for
 * "start" and "end".
 *
 * sentinel client-reconfig-script的作用是在故障转移结束后，会触发对应路径的脚本，并向脚本发送故障转移结果的相关参数。
 * 当故障转移结束，每个Sentinel节点会将故障转移的结果发送给对应的脚本，
 * 当前函数调用配置的脚本，脚本参数格式是：
 *      <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
 *
 * 例如以下内容分别是三个Sentinel节点发送给脚本的，其中一个是 leader，另外两个是observer：
 *      mymaster leader start 127.0.0.1 6379 127.0.0.1 6380
*       mymaster observer start 127.0.0.1 6379 127.0.0.1 6380
*       mymaster observer start 127.0.0.1 6379 127.0.0.1 6380
 * */
void sentinelCallClientReconfScript(sentinelRedisInstance *master, int role, char *state, sentinelAddr *from, sentinelAddr *to) {
    char fromport[32], toport[32];

    if (master->client_reconfig_script == NULL) return;
    ll2string(fromport,sizeof(fromport),from->port);
    ll2string(toport,sizeof(toport),to->port);
    sentinelScheduleScriptExecution(master->client_reconfig_script,
        master->name,
        (role == SENTINEL_LEADER) ? "leader" : "observer",
        state, announceSentinelAddr(from), fromport,
        announceSentinelAddr(to), toport, NULL);
}

/* =============================== instanceLink ============================= */

/* Create a not yet connected link object. */
instanceLink *createInstanceLink(void) {
    instanceLink *link = zmalloc(sizeof(*link));

    link->refcount = 1;
    link->disconnected = 1;
    link->pending_commands = 0;
    link->cc = NULL;
    link->pc = NULL;
    link->cc_conn_time = 0;
    link->pc_conn_time = 0;
    link->last_reconn_time = 0;
    link->pc_last_activity = 0;
    /* We set the act_ping_time to "now" even if we actually don't have yet
     * a connection with the node, nor we sent a ping.
     * This is useful to detect a timeout in case we'll not be able to connect
     * with the node at all. */
    link->act_ping_time = mstime();
    link->last_ping_time = 0;
    link->last_avail_time = mstime();
    link->last_pong_time = mstime();
    return link;
}

/* Disconnect a hiredis connection in the context of an instance link. */
void instanceLinkCloseConnection(instanceLink *link, redisAsyncContext *c) {
    if (c == NULL) return;

    if (link->cc == c) {
        link->cc = NULL;
        link->pending_commands = 0;
    }
    if (link->pc == c) link->pc = NULL;
    c->data = NULL;
    link->disconnected = 1;
    redisAsyncFree(c);
}

/* Decrement the refcount of a link object, if it drops to zero, actually
 * free it and return NULL. Otherwise don't do anything and return the pointer
 * to the object.
 *
 * If we are not going to free the link and ri is not NULL, we rebind all the
 * pending requests in link->cc (hiredis connection for commands) to a
 * callback that will just ignore them. This is useful to avoid processing
 * replies for an instance that no longer exists. */
instanceLink *releaseInstanceLink(instanceLink *link, sentinelRedisInstance *ri)
{
    serverAssert(link->refcount > 0);
    link->refcount--;
    if (link->refcount != 0) {
        if (ri && ri->link->cc) {
            /* This instance may have pending callbacks in the hiredis async
             * context, having as 'privdata' the instance that we are going to
             * free. Let's rewrite the callback list, directly exploiting
             * hiredis internal data structures, in order to bind them with
             * a callback that will ignore the reply at all. */
            redisCallback *cb;
            redisCallbackList *callbacks = &link->cc->replies;

            cb = callbacks->head;
            while(cb) {
                if (cb->privdata == ri) {
                    cb->fn = sentinelDiscardReplyCallback;
                    cb->privdata = NULL; /* Not strictly needed. */
                }
                cb = cb->next;
            }
        }
        return link; /* Other active users. */
    }

    instanceLinkCloseConnection(link,link->cc);
    instanceLinkCloseConnection(link,link->pc);
    zfree(link);
    return NULL;
}

/* This function will attempt to share the instance link we already have
 * for the same Sentinel in the context of a different master, with the
 * instance we are passing as argument.
 *
 * This way multiple Sentinel objects that refer all to the same physical
 * Sentinel instance but in the context of different masters will use
 * a single connection, will send a single PING per second for failure
 * detection and so forth.
 *
 * Return C_OK if a matching Sentinel was found in the context of a
 * different master and sharing was performed. Otherwise C_ERR
 * is returned. */
int sentinelTryConnectionSharing(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;

    if (ri->runid == NULL) return C_ERR; /* No way to identify it. */
    if (ri->link->refcount > 1) return C_ERR; /* Already shared. */

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master = dictGetVal(de), *match;
        /* We want to share with the same physical Sentinel referenced
         * in other masters, so skip our master. */
        if (master == ri->master) continue;
        match = getSentinelRedisInstanceByAddrAndRunID(master->sentinels,
                                                       NULL,0,ri->runid);
        if (match == NULL) continue; /* No match. */
        if (match == ri) continue; /* Should never happen but... safer. */

        /* We identified a matching Sentinel, great! Let's free our link
         * and use the one of the matching Sentinel. */
        releaseInstanceLink(ri->link,NULL);
        ri->link = match->link;
        match->link->refcount++;
        dictReleaseIterator(di);
        return C_OK;
    }
    dictReleaseIterator(di);
    return C_ERR;
}

/* Drop all connections to other sentinels. Returns the number of connections
 * dropped.*/
int sentinelDropConnections(void) {
    dictIterator *di;
    dictEntry *de;
    int dropped = 0;

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL) {
        dictIterator *sdi;
        dictEntry *sde;

        sentinelRedisInstance *ri = dictGetVal(de);
        sdi = dictGetIterator(ri->sentinels);
        while ((sde = dictNext(sdi)) != NULL) {
            sentinelRedisInstance *si = dictGetVal(sde);
            if (!si->link->disconnected) {
                instanceLinkCloseConnection(si->link, si->link->pc);
                instanceLinkCloseConnection(si->link, si->link->cc);
                dropped++;
            }
        }
        dictReleaseIterator(sdi);
    }
    dictReleaseIterator(di);

    return dropped;
}

/* When we detect a Sentinel to switch address (reporting a different IP/port
 * pair in Hello messages), let's update all the matching Sentinels in the
 * context of other masters as well and disconnect the links, so that everybody
 * will be updated.
 *
 * Return the number of updated Sentinel addresses. */
int sentinelUpdateSentinelAddressInAllMasters(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;
    int reconfigured = 0;

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master = dictGetVal(de), *match;
        match = getSentinelRedisInstanceByAddrAndRunID(master->sentinels,
                                                       NULL,0,ri->runid);
        /* If there is no match, this master does not know about this
         * Sentinel, try with the next one. */
        if (match == NULL) continue;

        /* Disconnect the old links if connected. */
        if (match->link->cc != NULL)
            instanceLinkCloseConnection(match->link,match->link->cc);
        if (match->link->pc != NULL)
            instanceLinkCloseConnection(match->link,match->link->pc);

        if (match == ri) continue; /* Address already updated for it. */

        /* Update the address of the matching Sentinel by copying the address
         * of the Sentinel object that received the address update. */
        releaseSentinelAddr(match->addr);
        match->addr = dupSentinelAddr(ri->addr);
        reconfigured++;
    }
    dictReleaseIterator(di);
    if (reconfigured)
        sentinelEvent(LL_NOTICE,"+sentinel-address-update", ri,
                    "%@ %d additional matching instances", reconfigured);
    return reconfigured;
}

/* This function is called when a hiredis connection reported an error.
 * We set it to NULL and mark the link as disconnected so that it will be
 * reconnected again.
 *
 * Note: we don't free the hiredis context as hiredis will do it for us
 * for async connections. */
void instanceLinkConnectionError(const redisAsyncContext *c) {
    instanceLink *link = c->data;
    int pubsub;

    if (!link) return;

    pubsub = (link->pc == c);
    if (pubsub)
        link->pc = NULL;
    else
        link->cc = NULL;
    link->disconnected = 1;
}

/* Hiredis connection established / disconnected callbacks. We need them
 * just to cleanup our link state. */
void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status) {
    if (status != C_OK) instanceLinkConnectionError(c);
}

void sentinelDisconnectCallback(const redisAsyncContext *c, int status) {
    UNUSED(status);
    instanceLinkConnectionError(c);
}

/* ========================== sentinelRedisInstance ========================= */

/* Create a redis instance, the following fields must be populated by the
 * caller if needed:
 * runid: set to NULL but will be populated once INFO output is received.
 * info_refresh: is set to 0 to mean that we never received INFO so far.
 *
 * If SRI_MASTER is set into initial flags the instance is added to
 * sentinel.masters table.
 *
 * if SRI_SLAVE or SRI_SENTINEL is set then 'master' must be not NULL and the
 * instance is added into master->slaves or master->sentinels table.
 *
 * If the instance is a slave or sentinel, the name parameter is ignored and
 * is created automatically as hostname:port.
 *
 * The function fails if hostname can't be resolved or port is out of range.
 * When this happens NULL is returned and errno is set accordingly to the
 * createSentinelAddr() function.
 *
 * The function may also fail and return NULL with errno set to EBUSY if
 * a master with the same name, a slave with the same address, or a sentinel
 * with the same ID already exists. */

sentinelRedisInstance *createSentinelRedisInstance(char *name, int flags, char *hostname, int port, int quorum, sentinelRedisInstance *master) {
    // sentinel monitor FGSMP_1 10.243.81.205 6379 2
    // name -> FGSMP_1
    // flags -> SRI_MASTER
    // hostname -> 10.243.81.205
    // port -> 6379
    // quorum -> 2
    // master -> NULL
    sentinelRedisInstance *ri;
    sentinelAddr *addr;
    dict *table = NULL; // 字典对象, masters, slaves, sentinels
    sds sdsname;

    serverAssert(flags & (SRI_MASTER|SRI_SLAVE|SRI_SENTINEL));
    serverAssert((flags & SRI_MASTER) || master != NULL);

    /* Check address validity. 以创建master节点对象为例, addr就是master节点的网络地址， 包括ip地址和端口信息 */
    addr = createSentinelAddr(hostname,port,1);
    if (addr == NULL) return NULL;

    /* For slaves use ip/host:port as name. */
    if (flags & SRI_SLAVE)
        // 从节点的名称是 ip:port
        sdsname = announceSentinelAddrAndPort(addr);
    else
        sdsname = sdsnew(name);

    /* Make sure the entry is not duplicated. This may happen when the same
     * name for a master is used multiple times inside the configuration or
     * if we try to add multiple times a slave or sentinel with same ip/port
     * to a master. */
    if (flags & SRI_MASTER) table = sentinel.masters;
    else if (flags & SRI_SLAVE) table = master->slaves;
    else if (flags & SRI_SENTINEL) table = master->sentinels;
    if (dictFind(table,sdsname)) {
        releaseSentinelAddr(addr);
        sdsfree(sdsname);
        errno = EBUSY;
        return NULL;
    }

    /* Create the instance object. */
    ri = zmalloc(sizeof(*ri));
    /* Note that all the instances are started in the disconnected state,
     * the event loop will take care of connecting them. */
    ri->flags = flags;
    ri->name = sdsname;
    ri->runid = NULL;
    ri->config_epoch = 0;
    ri->addr = addr;
    ri->link = createInstanceLink();
    ri->last_pub_time = mstime();
    ri->last_hello_time = mstime();
    ri->last_master_down_reply_time = mstime();
    ri->s_down_since_time = 0;
    ri->o_down_since_time = 0;
    ri->down_after_period = master ? master->down_after_period :
                            SENTINEL_DEFAULT_DOWN_AFTER;
    ri->master_link_down_time = 0;
    ri->auth_pass = NULL;
    ri->auth_user = NULL;
    ri->slave_priority = SENTINEL_DEFAULT_SLAVE_PRIORITY;
    ri->replica_announced = 1;
    ri->slave_reconf_sent_time = 0;
    ri->slave_master_host = NULL;
    ri->slave_master_port = 0;
    ri->slave_master_link_status = SENTINEL_MASTER_LINK_STATUS_DOWN;
    ri->slave_repl_offset = 0;
    ri->sentinels = dictCreate(&instancesDictType,NULL);
    ri->quorum = quorum;
    ri->parallel_syncs = SENTINEL_DEFAULT_PARALLEL_SYNCS;
    ri->master = master;
    ri->slaves = dictCreate(&instancesDictType,NULL);
    ri->info_refresh = 0;
    ri->renamed_commands = dictCreate(&renamedCommandsDictType,NULL);

    /* Failover state. */
    ri->leader = NULL;
    ri->leader_epoch = 0;
    ri->failover_epoch = 0;
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0;
    ri->failover_timeout = SENTINEL_DEFAULT_FAILOVER_TIMEOUT;
    ri->failover_delay_logged = 0;
    ri->promoted_slave = NULL;
    ri->notification_script = NULL;
    ri->client_reconfig_script = NULL;
    ri->info = NULL;

    /* Role */
    ri->role_reported = ri->flags & (SRI_MASTER|SRI_SLAVE);
    ri->role_reported_time = mstime();
    ri->slave_conf_change_time = mstime();

    /* Add into the right table. */
    dictAdd(table, ri->name, ri);
    return ri;
}

/* Release this instance and all its slaves, sentinels, hiredis connections.
 * This function does not take care of unlinking the instance from the main
 * masters table (if it is a master) or from its master sentinels/slaves table
 * if it is a slave or sentinel. */
void releaseSentinelRedisInstance(sentinelRedisInstance *ri) {
    /* Release all its slaves or sentinels if any. */
    dictRelease(ri->sentinels);
    dictRelease(ri->slaves);

    /* Disconnect the instance. */
    releaseInstanceLink(ri->link,ri);

    /* Free other resources. */
    sdsfree(ri->name);
    sdsfree(ri->runid);
    sdsfree(ri->notification_script);
    sdsfree(ri->client_reconfig_script);
    sdsfree(ri->slave_master_host);
    sdsfree(ri->leader);
    sdsfree(ri->auth_pass);
    sdsfree(ri->auth_user);
    sdsfree(ri->info);
    releaseSentinelAddr(ri->addr);
    dictRelease(ri->renamed_commands);

    /* Clear state into the master if needed. */
    if ((ri->flags & SRI_SLAVE) && (ri->flags & SRI_PROMOTED) && ri->master)
        ri->master->promoted_slave = NULL;

    zfree(ri);
}

/* Lookup a slave in a master Redis instance, by ip and port. */
sentinelRedisInstance *sentinelRedisInstanceLookupSlave(
                sentinelRedisInstance *ri, char *slave_addr, int port)
{
    sds key;
    sentinelRedisInstance *slave;
    sentinelAddr *addr;

    serverAssert(ri->flags & SRI_MASTER);

    /* We need to handle a slave_addr that is potentially a hostname.
     * If that is the case, depending on configuration we either resolve
     * it and use the IP addres or fail.
     */
    addr = createSentinelAddr(slave_addr, port, 0);
    if (!addr) return NULL;
    key = announceSentinelAddrAndPort(addr);
    releaseSentinelAddr(addr);

    slave = dictFetchValue(ri->slaves,key);
    sdsfree(key);
    return slave;
}

/* Return the name of the type of the instance as a string. */
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri) {
    if (ri->flags & SRI_MASTER) return "master";
    else if (ri->flags & SRI_SLAVE) return "slave";
    else if (ri->flags & SRI_SENTINEL) return "sentinel";
    else return "unknown";
}

/* This function remove the Sentinel with the specified ID from the
 * specified master.
 *
 * If "runid" is NULL the function returns ASAP.
 *
 * This function is useful because on Sentinels address switch, we want to
 * remove our old entry and add a new one for the same ID but with the new
 * address.
 *
 * The function returns 1 if the matching Sentinel was removed, otherwise
 * 0 if there was no Sentinel with this ID. */
int removeMatchingSentinelFromMaster(sentinelRedisInstance *master, char *runid) {
    dictIterator *di;
    dictEntry *de;
    int removed = 0;

    if (runid == NULL) return 0;

    di = dictGetSafeIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->runid && strcmp(ri->runid,runid) == 0) {
            dictDelete(master->sentinels,ri->name);
            removed++;
        }
    }
    dictReleaseIterator(di);
    return removed;
}

/* Search an instance with the same runid, ip and port into a dictionary
 * of instances. Return NULL if not found, otherwise return the instance
 * pointer.
 *
 * runid or addr can be NULL. In such a case the search is performed only
 * by the non-NULL field. */
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *addr, int port, char *runid) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *instance = NULL;
    sentinelAddr *ri_addr = NULL;

    serverAssert(addr || runid);   /* User must pass at least one search param. */
    if (addr != NULL) {
        /* Try to resolve addr. If hostnames are used, we're accepting an ri_addr
         * that contains an hostname only and can still be matched based on that.
         */
        ri_addr = createSentinelAddr(addr,port,1);
        if (!ri_addr) return NULL;
    }
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (runid && !ri->runid) continue;
        if ((runid == NULL || strcmp(ri->runid, runid) == 0) &&
            (addr == NULL || sentinelAddrOrHostnameEqual(ri->addr, ri_addr)))
        {
            instance = ri;
            break;
        }
    }
    dictReleaseIterator(di);
    if (ri_addr != NULL)
        releaseSentinelAddr(ri_addr);

    return instance;
}

/* Master lookup by name */
sentinelRedisInstance *sentinelGetMasterByName(char *name) {
    sentinelRedisInstance *ri;
    sds sdsname = sdsnew(name);

    ri = dictFetchValue(sentinel.masters,sdsname);
    sdsfree(sdsname);
    return ri;
}

/* Add the specified flags to all the instances in the specified dictionary. */
void sentinelAddFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags |= flags;
    }
    dictReleaseIterator(di);
}

/* Remove the specified flags to all the instances in the specified
 * dictionary. */
void sentinelDelFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags &= ~flags;
    }
    dictReleaseIterator(di);
}

/* Reset the state of a monitored master:
 * 1) Remove all slaves.
 * 2) Remove all sentinels.
 * 3) Remove most of the flags resulting from runtime operations.
 * 4) Reset timers to their default value. For example after a reset it will be
 *    possible to failover again the same master ASAP, without waiting the
 *    failover timeout delay.
 * 5) In the process of doing this undo the failover if in progress.
 * 6) Disconnect the connections with the master (will reconnect automatically).
 *
 * 重置被监控的主节点状态:
 *      1)移除所有从节点;
 *      2)移除所有哨兵;
 *      3)删除运行时操作产生的大多数flags;
 *      4)将计时器重置为默认值。例如，在重置之后，可以尽快对同一个主机进行故障转移，而无需等待故障转移超时延迟;
 *      5)在此过程中撤销正在进行的故障转移;
 *      6)断开与master的连接(将自动重新连接)。
 */

#define SENTINEL_RESET_NO_SENTINELS (1<<0)
void sentinelResetMaster(sentinelRedisInstance *ri, int flags) {
    // ri: 这里的这个参数是一个主节点实例
    // flags: SENTINEL_RESET_NO_SENTINELS， 表示并不清空sentinels字典
    serverAssert(ri->flags & SRI_MASTER);
    // 清空当前主实例的slave字典
    dictRelease(ri->slaves);
    ri->slaves = dictCreate(&instancesDictType,NULL);
    if (!(flags & SENTINEL_RESET_NO_SENTINELS)) {
        dictRelease(ri->sentinels);
        ri->sentinels = dictCreate(&instancesDictType,NULL);
    }
    // 断开与当前master的连接，使的当前sentinel可以自动连接到新的master
    instanceLinkCloseConnection(ri->link,ri->link->cc);
    instanceLinkCloseConnection(ri->link,ri->link->pc);
    ri->flags &= SRI_MASTER;
    if (ri->leader) {
        sdsfree(ri->leader);
        ri->leader = NULL;
    }
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0; /* We can failover again ASAP. */
    ri->promoted_slave = NULL;
    sdsfree(ri->runid);
    sdsfree(ri->slave_master_host);
    ri->runid = NULL;
    ri->slave_master_host = NULL;
    ri->link->act_ping_time = mstime();
    ri->link->last_ping_time = 0;
    ri->link->last_avail_time = mstime();
    ri->link->last_pong_time = mstime();
    ri->role_reported_time = mstime();
    ri->role_reported = SRI_MASTER;
    if (flags & SENTINEL_GENERATE_EVENT)
        sentinelEvent(LL_WARNING,"+reset-master",ri,"%@");
}

/* Call sentinelResetMaster() on every master with a name matching the specified
 * pattern. */
int sentinelResetMastersByPattern(char *pattern, int flags) {
    dictIterator *di;
    dictEntry *de;
    int reset = 0;

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->name) {
            if (stringmatch(pattern,ri->name,0)) {
                sentinelResetMaster(ri,flags);
                reset++;
            }
        }
    }
    dictReleaseIterator(di);
    return reset;
}

/* Reset the specified master with sentinelResetMaster(), and also change
 * the ip:port address, but take the name of the instance unmodified.
 *
 * This is used to handle the +switch-master event.
 *
 * The function returns C_ERR if the address can't be resolved for some
 * reason. Otherwise C_OK is returned.
 *
 * 使用sentinelResetMaster() Reset指定的master，并更改ip:port地址，但不修改实例名。
 * 这用于处理+switch-master事件。如果地址由于某种原因无法解析，函数返回C_ERR。否则返回C_OK。
 * */
int sentinelResetMasterAndChangeAddress(sentinelRedisInstance *master, char *hostname, int port) {
    sentinelAddr *oldaddr, *newaddr;
    sentinelAddr **slaves = NULL;
    int numslaves = 0, j;
    dictIterator *di;
    dictEntry *de;

    // 新的主节点的地址
    newaddr = createSentinelAddr(hostname,port,0);
    if (newaddr == NULL) return C_ERR;

    /* There can be only 0 or 1 slave that has the newaddr.
     * and It can add old master 1 more slave. 
     * so It allocates dictSize(master->slaves) + 1
     * 只能有0或1个具有newaddr的从服务器。它可以给旧的master添加更多的slaves。所以它分配dictSize(主->从)+ 1
     *
     * slaves 变量里包含了除晋升的从节点之外的其他从节点
     * */
    slaves = zmalloc(sizeof(sentinelAddr*)*(dictSize(master->slaves) + 1));
    
    /* Don't include the one having the address we are switching to.
     * 不要包括有我们要切换到的地址的那个。
     * */
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (sentinelAddrIsEqual(slave->addr,newaddr)) continue;
        slaves[numslaves++] = dupSentinelAddr(slave->addr);
    }
    dictReleaseIterator(di);

    /* If we are switching to a different address, include the old address
     * as a slave as well, so that we'll be able to sense / reconfigure
     * the old master.
     *
     * 将旧的主节点也放入slaves中。 注意这里，这是很关键的一点，旧的主节点是如何指向新的主节点的。                                 <-【1】
     * 这里将旧的主节点添加slaves字典中之后，下次
     * */
    if (!sentinelAddrIsEqual(newaddr,master->addr)) {
        slaves[numslaves++] = dupSentinelAddr(master->addr);
    }

    /* Reset and switch address. */
    // 重置master相关信息，主要操作是将master的slaves字典清空                                                               <-【1】
    sentinelResetMaster(master,SENTINEL_RESET_NO_SENTINELS);
    oldaddr = master->addr;
    master->addr = newaddr; // 将晋升的从节点在字典层面变为主节点，这里只变更一下地址，其他的由INFO命令自动更新.                 <-【2】
    master->o_down_since_time = 0;
    master->s_down_since_time = 0;

    /* Add slaves back.
     * 将所有的从节点添加到slaves字典中
     * */
    for (j = 0; j < numslaves; j++) {
        sentinelRedisInstance *slave;

        slave = createSentinelRedisInstance(NULL,SRI_SLAVE,slaves[j]->hostname,
                    slaves[j]->port, master->quorum, master);
        releaseSentinelAddr(slaves[j]);
        if (slave) sentinelEvent(LL_NOTICE,"+slave",slave,"%@");
    }
    zfree(slaves);

    /* Release the old address at the end so we are safe even if the function
     * gets the master->addr->ip and master->addr->port as arguments.
     * 在最后释放旧地址，这样即使函数获得master->addr->ip和master->addr->port作为参数，我们也是安全的。
     * */
    releaseSentinelAddr(oldaddr);
    sentinelFlushConfig(); // 重写配置
    return C_OK;
}

/* Return non-zero if there was no SDOWN or ODOWN error associated to this
 * instance in the latest 'ms' milliseconds.
 * 如果在最近的参数指定的毫秒内没有与此实例关联的SDOWN或ODOWN错误，则返回非零。
 * */
int sentinelRedisInstanceNoDownFor(sentinelRedisInstance *ri, mstime_t ms) {
    mstime_t most_recent;

    most_recent = ri->s_down_since_time;
    if (ri->o_down_since_time > most_recent)
        most_recent = ri->o_down_since_time;
    return most_recent == 0 || (mstime() - most_recent) > ms;
}

/* Return the current master address, that is, its address or the address
 * of the promoted slave if already operational. */
sentinelAddr *sentinelGetCurrentMasterAddress(sentinelRedisInstance *master) {
    /* If we are failing over the master, and the state is already
     * SENTINEL_FAILOVER_STATE_RECONF_SLAVES or greater, it means that we
     * already have the new configuration epoch in the master, and the
     * slave acknowledged the configuration switch. Advertise the new
     * address. */
    if ((master->flags & SRI_FAILOVER_IN_PROGRESS) &&
        master->promoted_slave &&
        master->failover_state >= SENTINEL_FAILOVER_STATE_RECONF_SLAVES)
    {
        return master->promoted_slave->addr;
    } else {
        return master->addr;
    }
}

/* This function sets the down_after_period field value in 'master' to all
 * the slaves and sentinel instances connected to this master. */
void sentinelPropagateDownAfterPeriod(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    int j;
    dict *d[] = {master->slaves, master->sentinels, NULL};

    for (j = 0; d[j]; j++) {
        di = dictGetIterator(d[j]);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            ri->down_after_period = master->down_after_period;
        }
        dictReleaseIterator(di);
    }
}

/* This function is used in order to send commands to Redis instances: the
 * commands we send from Sentinel may be renamed, a common case is a master
 * with CONFIG and SLAVEOF commands renamed for security concerns. In that
 * case we check the ri->renamed_command table (or if the instance is a slave,
 * we check the one of the master), and map the command that we should send
 * to the set of renamed commands. However, if the command was not renamed,
 * we just return "command" itself. */
char *sentinelInstanceMapCommand(sentinelRedisInstance *ri, char *command) {
    sds sc = sdsnew(command);
    if (ri->master) ri = ri->master;
    char *retval = dictFetchValue(ri->renamed_commands, sc);
    sdsfree(sc);
    return retval ? retval : command;
}

/* ============================ Config handling ============================= */

/* Generalise handling create instance error. Use SRI_MASTER, SRI_SLAVE or
 * SRI_SENTINEL as a role value. */
const char *sentinelCheckCreateInstanceErrors(int role) {
    switch(errno) {
    case EBUSY:
        switch (role) {
        case SRI_MASTER:
            return "Duplicate master name.";
        case SRI_SLAVE:
            return "Duplicate hostname and port for replica.";
        case SRI_SENTINEL:
            return "Duplicate runid for sentinel.";
        default:
            serverAssert(0);
            break;
        }
        break;
    case ENOENT:
        return "Can't resolve instance hostname.";
    case EINVAL:
        return "Invalid port number.";
    default:
        return "Unknown Error for creating instances.";
    }
}

/* init function for server.sentinel_config */
void initializeSentinelConfig() {
    server.sentinel_config = zmalloc(sizeof(struct sentinelConfig));
    server.sentinel_config->monitor_cfg = listCreate(); // 链表
    server.sentinel_config->pre_monitor_cfg = listCreate();
    server.sentinel_config->post_monitor_cfg = listCreate();
    listSetFreeMethod(server.sentinel_config->monitor_cfg,freeSentinelLoadQueueEntry);
    listSetFreeMethod(server.sentinel_config->pre_monitor_cfg,freeSentinelLoadQueueEntry);
    listSetFreeMethod(server.sentinel_config->post_monitor_cfg,freeSentinelLoadQueueEntry);
}

/* destroy function for server.sentinel_config */
void freeSentinelConfig() {
    /* release these three config queues since we will not use it anymore */
    listRelease(server.sentinel_config->pre_monitor_cfg);
    listRelease(server.sentinel_config->monitor_cfg);
    listRelease(server.sentinel_config->post_monitor_cfg);
    zfree(server.sentinel_config);
    server.sentinel_config = NULL;
}

/* Search config name in pre monitor config name array, return 1 if found,
 * 0 if not found. */
int searchPreMonitorCfgName(const char *name) {
    for (unsigned int i = 0; i < sizeof(preMonitorCfgName)/sizeof(preMonitorCfgName[0]); i++) {
        if (!strcasecmp(preMonitorCfgName[i],name)) return 1;
    }
    return 0;
}

/* free method for sentinelLoadQueueEntry when release the list */
void freeSentinelLoadQueueEntry(void *item) {
    struct sentinelLoadQueueEntry *entry = item;
    sdsfreesplitres(entry->argv,entry->argc);
    sdsfree(entry->line);
    zfree(entry);
}

/* This function is used for queuing sentinel configuration, the main
 * purpose of this function is to delay parsing the sentinel config option
 * in order to avoid the order dependent issue from the config.
 * 这个函数用于队列化哨兵配置，这个函数的主要目的是延迟哨兵配置选项的解析，以避免来自配置的顺序依赖问题。
 * 简单的说就是对配置项进行排序
 * */
void queueSentinelConfig(sds *argv, int argc, int linenum, sds line) {
    int i;
    struct sentinelLoadQueueEntry *entry;

    /* initialize sentinel_config for the first call */
    if (server.sentinel_config == NULL) initializeSentinelConfig();

    entry = zmalloc(sizeof(struct sentinelLoadQueueEntry));
    entry->argv = zmalloc(sizeof(char*)*argc);
    entry->argc = argc;
    entry->linenum = linenum;
    entry->line = sdsdup(line);
    for (i = 0; i < argc; i++) {
        entry->argv[i] = sdsdup(argv[i]);
    }
    /*  Separate config lines with pre monitor config, monitor config and
     *  post monitor config, in order to parsing config dependencies
     *  correctly. */
    if (!strcasecmp(argv[0],"monitor")) {
        // sentinel monitor EPPSPCCS_c37470_1 10.237.186.54 6379 2
        listAddNodeTail(server.sentinel_config->monitor_cfg,entry);
    } else if (searchPreMonitorCfgName(argv[0])) {
        // searchPreMonitorCfgName函数 用于判断当前配置是否需要在monitor相关配置前被应用, 这些需要在monitor相关配置前被应用的参数列表如下:
        //    "announce-ip",
        //    "announce-port",
        //    "deny-scripts-reconfig",
        //    "sentinel-user",
        //    "sentinel-pass",
        //    "current-epoch",
        //    "myid",
        //    "resolve-hostnames",
        //    "announce-hostnames"
        listAddNodeTail(server.sentinel_config->pre_monitor_cfg,entry);
    } else{
        // 这里剩下的就是类似下面这样的配置:
        //  sentinel config-epoch FGSMP_1 37567663
        //  sentinel leader-epoch FGSMP_1 43741262
        //  sentinel known-slave FGSMP_1 10.243.81.204 6379
        //  sentinel known-slave FGSMP_1 10.243.81.203 6379
        //  sentinel known-sentinel FGSMP_1 10.237.188.71 26379 36c91acb157a7cb11562859117145aa523ed04e5
        //  sentinel known-sentinel FGSMP_1 10.237.188.70 26379 61da927aabcc556e19610fba30345bdfcd50b7aa
        listAddNodeTail(server.sentinel_config->post_monitor_cfg,entry);
    }
}

/* This function is used for loading the sentinel configuration from
 * pre_monitor_cfg, monitor_cfg and post_monitor_cfg list
 * 该函数用于从pre_monitor_cfg、monitor_cfg和post_monitor_cfg列表中加载哨兵配置
 * */
void loadSentinelConfigFromQueue(void) {
    const char *err = NULL;
    listIter li;
    listNode *ln;
    int linenum = 0;
    sds line = NULL;

    /* if there is no sentinel_config entry, we can return immediately */
    if (server.sentinel_config == NULL) return;

    /* loading from pre monitor config queue first to avoid dependency issues
     * 先加载需要在monitor配置之前应用的相关配置
     * */
    listRewind(server.sentinel_config->pre_monitor_cfg,&li);
    while((ln = listNext(&li))) {
        struct sentinelLoadQueueEntry *entry = ln->value;
        err = sentinelHandleConfiguration(entry->argv,entry->argc);
        if (err) {
            linenum = entry->linenum;
            line = entry->line;
            goto loaderr;
        }
    }

    /* loading from monitor config queue */
    listRewind(server.sentinel_config->monitor_cfg,&li);
    while((ln = listNext(&li))) {
        struct sentinelLoadQueueEntry *entry = ln->value;
        // sentinelHandleConfiguration函数会解析sentinel配置,  当解析到sentinel monitor相关的配置时, 会调用createSentinelRedisInstance函数创建一个sentinelRedisInstance实例.
        // 这个实例中主要存储主节点的相关信息,名将其添加到sentinel.masters字典中. sentinel刚启动时,只有主节点信息
        err = sentinelHandleConfiguration(entry->argv,entry->argc);
        if (err) {
            linenum = entry->linenum;
            line = entry->line;
            goto loaderr;
        }
    }

    /* loading from the post monitor config queue */
    listRewind(server.sentinel_config->post_monitor_cfg,&li);
    while((ln = listNext(&li))) {
        struct sentinelLoadQueueEntry *entry = ln->value;
        err = sentinelHandleConfiguration(entry->argv,entry->argc);
        if (err) {
            linenum = entry->linenum;
            line = entry->line;
            goto loaderr;
        }
    }

    /* free sentinel_config when config loading is finished */
    freeSentinelConfig();
    return;

loaderr:
    fprintf(stderr, "\n*** FATAL CONFIG FILE ERROR (Redis %s) ***\n",
        REDIS_VERSION);
    fprintf(stderr, "Reading the configuration file, at line %d\n", linenum);
    fprintf(stderr, ">>> '%s'\n", line);
    fprintf(stderr, "%s\n", err);
    exit(1);
}

const char *sentinelHandleConfiguration(char **argv, int argc) {

    sentinelRedisInstance *ri;

    if (!strcasecmp(argv[0],"monitor") && argc == 5) {
        /* monitor <name> <host> <port> <quorum> */
        int quorum = atoi(argv[4]);

        if (quorum <= 0) return "Quorum must be 1 or greater.";
        // 对于sentinel monitor配置项而言,其格式如下:       <-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=--=-=--=-=-[1]
        //      sentinel monitor FGSMP_1 10.243.81.205 6379 2
        // 注意,这里的argv[1]指向的是 FGSMP_1 这个master节点名称, 因为在前面 loadServerConfig 函数中, 给Sentinel_config的相关属性中放入的配置内容是没有sentinel关键字的
        if (createSentinelRedisInstance(argv[1],SRI_MASTER,argv[2],
                                        atoi(argv[3]),quorum,NULL) == NULL)
        {
            return sentinelCheckCreateInstanceErrors(SRI_MASTER);
        }
    } else if (!strcasecmp(argv[0],"down-after-milliseconds") && argc == 3) {
        /* down-after-milliseconds <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->down_after_period = atoi(argv[2]);
        if (ri->down_after_period <= 0)
            return "negative or zero time parameter.";
        sentinelPropagateDownAfterPeriod(ri);
    } else if (!strcasecmp(argv[0],"failover-timeout") && argc == 3) {
        /* failover-timeout <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->failover_timeout = atoi(argv[2]);
        if (ri->failover_timeout <= 0)
            return "negative or zero time parameter.";
    } else if (!strcasecmp(argv[0],"parallel-syncs") && argc == 3) {
        /* parallel-syncs <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->parallel_syncs = atoi(argv[2]);
    } else if (!strcasecmp(argv[0],"notification-script") && argc == 3) {
        /* notification-script <name> <path> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if (access(argv[2],X_OK) == -1)
            return "Notification script seems non existing or non executable.";
        ri->notification_script = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0],"client-reconfig-script") && argc == 3) {
        /* client-reconfig-script <name> <path> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if (access(argv[2],X_OK) == -1)
            return "Client reconfiguration script seems non existing or "
                   "non executable.";
        ri->client_reconfig_script = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0],"auth-pass") && argc == 3) {
        /* auth-pass <name> <password> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->auth_pass = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0],"auth-user") && argc == 3) {
        /* auth-user <name> <username> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->auth_user = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0],"current-epoch") && argc == 2) {
        /* current-epoch <epoch> */
        unsigned long long current_epoch = strtoull(argv[1],NULL,10);
        if (current_epoch > sentinel.current_epoch)
            sentinel.current_epoch = current_epoch;
    } else if (!strcasecmp(argv[0],"myid") && argc == 2) {
        if (strlen(argv[1]) != CONFIG_RUN_ID_SIZE)
            return "Malformed Sentinel id in myid option.";
        memcpy(sentinel.myid,argv[1],CONFIG_RUN_ID_SIZE);
    } else if (!strcasecmp(argv[0],"config-epoch") && argc == 3) {
        /* config-epoch <name> <epoch> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->config_epoch = strtoull(argv[2],NULL,10);
        /* The following update of current_epoch is not really useful as
         * now the current epoch is persisted on the config file, but
         * we leave this check here for redundancy. */
        if (ri->config_epoch > sentinel.current_epoch)
            sentinel.current_epoch = ri->config_epoch;
    } else if (!strcasecmp(argv[0],"leader-epoch") && argc == 3) {
        /* leader-epoch <name> <epoch> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->leader_epoch = strtoull(argv[2],NULL,10);
    } else if ((!strcasecmp(argv[0],"known-slave") ||
                !strcasecmp(argv[0],"known-replica")) && argc == 4)
    {
        // 加载sentinel known-slave相关的配置,即读取known-slave后面的从节点信息,然后创建对应的sentinelRedisInstance  <-=-=-=-=-=-=-=-=-[3]
        sentinelRedisInstance *slave;

        /* known-replica <name> <ip> <port>
         *      eg: sentinel known-slave CDPOS_1 10.237.181.69 6379
         * */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if ((slave = createSentinelRedisInstance(NULL,SRI_SLAVE,argv[2],
                    atoi(argv[3]), ri->quorum, ri)) == NULL)
        {
            return sentinelCheckCreateInstanceErrors(SRI_SLAVE);
        }
    } else if (!strcasecmp(argv[0],"known-sentinel") &&
               (argc == 4 || argc == 5)) {
        // 加载sentinel known-sentinel相关的配置,即读取known-slave后面的从节点信息,然后创建对应的sentinelRedisInstance  <-=-=-=-=-=-=-=-=-[3]
        sentinelRedisInstance *si;

        if (argc == 5) { /* Ignore the old form without runid. */
            /* known-sentinel <name> <ip> <port> [runid]
             * eg: sentinel known-sentinel SCUS_c15675_2 10.237.188.71 26379 36c91acb157a7cb11562859117145aa523ed04e5
             * */
            ri = sentinelGetMasterByName(argv[1]);
            if (!ri) return "No such master with specified name.";
            if ((si = createSentinelRedisInstance(argv[4],SRI_SENTINEL,argv[2],
                        atoi(argv[3]), ri->quorum, ri)) == NULL)
            {
                return sentinelCheckCreateInstanceErrors(SRI_SENTINEL);
            }
            si->runid = sdsnew(argv[4]);
            sentinelTryConnectionSharing(si);
        }
    } else if (!strcasecmp(argv[0],"rename-command") && argc == 4) {
        /* rename-command <name> <command> <renamed-command> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        sds oldcmd = sdsnew(argv[2]);
        sds newcmd = sdsnew(argv[3]);
        if (dictAdd(ri->renamed_commands,oldcmd,newcmd) != DICT_OK) {
            sdsfree(oldcmd);
            sdsfree(newcmd);
            return "Same command renamed multiple times with rename-command.";
        }
    } else if (!strcasecmp(argv[0],"announce-ip") && argc == 2) {
        /* announce-ip <ip-address> */
        if (strlen(argv[1]))
            sentinel.announce_ip = sdsnew(argv[1]);
    } else if (!strcasecmp(argv[0],"announce-port") && argc == 2) {
        /* announce-port <port> */
        sentinel.announce_port = atoi(argv[1]);
    } else if (!strcasecmp(argv[0],"deny-scripts-reconfig") && argc == 2) {
        /* deny-scripts-reconfig <yes|no> */
        if ((sentinel.deny_scripts_reconfig = yesnotoi(argv[1])) == -1) {
            return "Please specify yes or no for the "
                   "deny-scripts-reconfig options.";
        }
    } else if (!strcasecmp(argv[0],"sentinel-user") && argc == 2) {
        /* sentinel-user <user-name> */
        if (strlen(argv[1]))
            sentinel.sentinel_auth_user = sdsnew(argv[1]);
    } else if (!strcasecmp(argv[0],"sentinel-pass") && argc == 2) {
        /* sentinel-pass <password> */
        if (strlen(argv[1]))
            sentinel.sentinel_auth_pass = sdsnew(argv[1]);
    } else if (!strcasecmp(argv[0],"resolve-hostnames") && argc == 2) {
        /* resolve-hostnames <yes|no> */
        if ((sentinel.resolve_hostnames = yesnotoi(argv[1])) == -1) {
            return "Please specify yes or no for the resolve-hostnames option.";
        }
    } else if (!strcasecmp(argv[0],"announce-hostnames") && argc == 2) {
        /* announce-hostnames <yes|no> */
        if ((sentinel.announce_hostnames = yesnotoi(argv[1])) == -1) {
            return "Please specify yes or no for the announce-hostnames option.";
        }
    } else {
        return "Unrecognized sentinel configuration statement.";
    }
    return NULL;
}

/* Implements CONFIG REWRITE for "sentinel" option.
 * This is used not just to rewrite the configuration given by the user
 * (the configured masters) but also in order to retain the state of
 * Sentinel across restarts: config epoch of masters, associated slaves
 * and sentinel instances, and so forth. */
void rewriteConfigSentinelOption(struct rewriteConfigState *state) {
    dictIterator *di, *di2;
    dictEntry *de;
    sds line;

    /* sentinel unique ID. */
    line = sdscatprintf(sdsempty(), "sentinel myid %s", sentinel.myid);
    rewriteConfigRewriteLine(state,"sentinel myid",line,1);

    /* sentinel deny-scripts-reconfig. */
    line = sdscatprintf(sdsempty(), "sentinel deny-scripts-reconfig %s",
        sentinel.deny_scripts_reconfig ? "yes" : "no");
    rewriteConfigRewriteLine(state,"sentinel deny-scripts-reconfig",line,
        sentinel.deny_scripts_reconfig != SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG);

    /* sentinel resolve-hostnames.
     * This must be included early in the file so it is already in effect
     * when reading the file.
     */
    line = sdscatprintf(sdsempty(), "sentinel resolve-hostnames %s",
                        sentinel.resolve_hostnames ? "yes" : "no");
    rewriteConfigRewriteLine(state,"sentinel resolve-hostnames",line,
                             sentinel.resolve_hostnames != SENTINEL_DEFAULT_RESOLVE_HOSTNAMES);

    /* sentinel announce-hostnames. */
    line = sdscatprintf(sdsempty(), "sentinel announce-hostnames %s",
                        sentinel.announce_hostnames ? "yes" : "no");
    rewriteConfigRewriteLine(state,"sentinel announce-hostnames",line,
                             sentinel.announce_hostnames != SENTINEL_DEFAULT_ANNOUNCE_HOSTNAMES);

    /* For every master emit a "sentinel monitor" config entry. */
    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master, *ri;
        sentinelAddr *master_addr;

        /* sentinel monitor */
        master = dictGetVal(de);
        master_addr = sentinelGetCurrentMasterAddress(master);
        line = sdscatprintf(sdsempty(),"sentinel monitor %s %s %d %d",
            master->name, announceSentinelAddr(master_addr), master_addr->port,
            master->quorum);
        rewriteConfigRewriteLine(state,"sentinel monitor",line,1);
        /* rewriteConfigMarkAsProcessed is handled after the loop */

        /* sentinel down-after-milliseconds */
        if (master->down_after_period != SENTINEL_DEFAULT_DOWN_AFTER) {
            line = sdscatprintf(sdsempty(),
                "sentinel down-after-milliseconds %s %ld",
                master->name, (long) master->down_after_period);
            rewriteConfigRewriteLine(state,"sentinel down-after-milliseconds",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        /* sentinel failover-timeout */
        if (master->failover_timeout != SENTINEL_DEFAULT_FAILOVER_TIMEOUT) {
            line = sdscatprintf(sdsempty(),
                "sentinel failover-timeout %s %ld",
                master->name, (long) master->failover_timeout);
            rewriteConfigRewriteLine(state,"sentinel failover-timeout",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */

        }

        /* sentinel parallel-syncs */
        if (master->parallel_syncs != SENTINEL_DEFAULT_PARALLEL_SYNCS) {
            line = sdscatprintf(sdsempty(),
                "sentinel parallel-syncs %s %d",
                master->name, master->parallel_syncs);
            rewriteConfigRewriteLine(state,"sentinel parallel-syncs",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        /* sentinel notification-script */
        if (master->notification_script) {
            line = sdscatprintf(sdsempty(),
                "sentinel notification-script %s %s",
                master->name, master->notification_script);
            rewriteConfigRewriteLine(state,"sentinel notification-script",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        /* sentinel client-reconfig-script */
        if (master->client_reconfig_script) {
            line = sdscatprintf(sdsempty(),
                "sentinel client-reconfig-script %s %s",
                master->name, master->client_reconfig_script);
            rewriteConfigRewriteLine(state,"sentinel client-reconfig-script",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        /* sentinel auth-pass & auth-user */
        if (master->auth_pass) {
            line = sdscatprintf(sdsempty(),
                "sentinel auth-pass %s %s",
                master->name, master->auth_pass);
            rewriteConfigRewriteLine(state,"sentinel auth-pass",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        if (master->auth_user) {
            line = sdscatprintf(sdsempty(),
                "sentinel auth-user %s %s",
                master->name, master->auth_user);
            rewriteConfigRewriteLine(state,"sentinel auth-user",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        /* sentinel config-epoch */
        line = sdscatprintf(sdsempty(),
            "sentinel config-epoch %s %llu",
            master->name, (unsigned long long) master->config_epoch);
        rewriteConfigRewriteLine(state,"sentinel config-epoch",line,1);
        /* rewriteConfigMarkAsProcessed is handled after the loop */


        /* sentinel leader-epoch */
        line = sdscatprintf(sdsempty(),
            "sentinel leader-epoch %s %llu",
            master->name, (unsigned long long) master->leader_epoch);
        rewriteConfigRewriteLine(state,"sentinel leader-epoch",line,1);
        /* rewriteConfigMarkAsProcessed is handled after the loop */

        /* sentinel known-slave */
        di2 = dictGetIterator(master->slaves);
        while((de = dictNext(di2)) != NULL) {
            sentinelAddr *slave_addr;

            ri = dictGetVal(de);
            slave_addr = ri->addr;

            /* If master_addr (obtained using sentinelGetCurrentMasterAddress()
             * so it may be the address of the promoted slave) is equal to this
             * slave's address, a failover is in progress and the slave was
             * already successfully promoted. So as the address of this slave
             * we use the old master address instead. */
            if (sentinelAddrIsEqual(slave_addr,master_addr))
                slave_addr = master->addr;
            line = sdscatprintf(sdsempty(),
                "sentinel known-replica %s %s %d",
                master->name, announceSentinelAddr(slave_addr), slave_addr->port);
            rewriteConfigRewriteLine(state,"sentinel known-replica",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }
        dictReleaseIterator(di2);

        /* sentinel known-sentinel */
        di2 = dictGetIterator(master->sentinels);
        while((de = dictNext(di2)) != NULL) {
            ri = dictGetVal(de);
            if (ri->runid == NULL) continue;
            line = sdscatprintf(sdsempty(),
                "sentinel known-sentinel %s %s %d %s",
                master->name, announceSentinelAddr(ri->addr), ri->addr->port, ri->runid);
            rewriteConfigRewriteLine(state,"sentinel known-sentinel",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }
        dictReleaseIterator(di2);

        /* sentinel rename-command */
        di2 = dictGetIterator(master->renamed_commands);
        while((de = dictNext(di2)) != NULL) {
            sds oldname = dictGetKey(de);
            sds newname = dictGetVal(de);
            line = sdscatprintf(sdsempty(),
                "sentinel rename-command %s %s %s",
                master->name, oldname, newname);
            rewriteConfigRewriteLine(state,"sentinel rename-command",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }
        dictReleaseIterator(di2);
    }

    /* sentinel current-epoch is a global state valid for all the masters. */
    line = sdscatprintf(sdsempty(),
        "sentinel current-epoch %llu", (unsigned long long) sentinel.current_epoch);
    rewriteConfigRewriteLine(state,"sentinel current-epoch",line,1);

    /* sentinel announce-ip. */
    if (sentinel.announce_ip) {
        line = sdsnew("sentinel announce-ip ");
        line = sdscatrepr(line, sentinel.announce_ip, sdslen(sentinel.announce_ip));
        rewriteConfigRewriteLine(state,"sentinel announce-ip",line,1);
    } else {
        rewriteConfigMarkAsProcessed(state,"sentinel announce-ip");
    }

    /* sentinel announce-port. */
    if (sentinel.announce_port) {
        line = sdscatprintf(sdsempty(),"sentinel announce-port %d",
                            sentinel.announce_port);
        rewriteConfigRewriteLine(state,"sentinel announce-port",line,1);
    } else {
        rewriteConfigMarkAsProcessed(state,"sentinel announce-port");
    }

    /* sentinel sentinel-user. */
    if (sentinel.sentinel_auth_user) {
        line = sdscatprintf(sdsempty(), "sentinel sentinel-user %s", sentinel.sentinel_auth_user);
        rewriteConfigRewriteLine(state,"sentinel sentinel-user",line,1);
    } else {
        rewriteConfigMarkAsProcessed(state,"sentinel sentinel-user");
    }

    /* sentinel sentinel-pass. */
    if (sentinel.sentinel_auth_pass) {
        line = sdscatprintf(sdsempty(), "sentinel sentinel-pass %s", sentinel.sentinel_auth_pass);
        rewriteConfigRewriteLine(state,"sentinel sentinel-pass",line,1);
    } else {
        rewriteConfigMarkAsProcessed(state,"sentinel sentinel-pass");  
    }

    dictReleaseIterator(di);

    /* NOTE: the purpose here is in case due to the state change, the config rewrite 
     does not handle the configs, however, previously the config was set in the config file, 
     rewriteConfigMarkAsProcessed should be put here to mark it as processed in order to 
     delete the old config entry.
    */
    rewriteConfigMarkAsProcessed(state,"sentinel monitor");
    rewriteConfigMarkAsProcessed(state,"sentinel down-after-milliseconds");
    rewriteConfigMarkAsProcessed(state,"sentinel failover-timeout");
    rewriteConfigMarkAsProcessed(state,"sentinel parallel-syncs");
    rewriteConfigMarkAsProcessed(state,"sentinel notification-script");
    rewriteConfigMarkAsProcessed(state,"sentinel client-reconfig-script");
    rewriteConfigMarkAsProcessed(state,"sentinel auth-pass");
    rewriteConfigMarkAsProcessed(state,"sentinel auth-user");
    rewriteConfigMarkAsProcessed(state,"sentinel config-epoch");
    rewriteConfigMarkAsProcessed(state,"sentinel leader-epoch");
    rewriteConfigMarkAsProcessed(state,"sentinel known-replica");
    rewriteConfigMarkAsProcessed(state,"sentinel known-sentinel");
    rewriteConfigMarkAsProcessed(state,"sentinel rename-command");
}

/* This function uses the config rewriting Redis engine in order to persist
 * the state of the Sentinel in the current configuration file.
 *
 * Before returning the function calls fsync() against the generated
 * configuration file to make sure changes are committed to disk.
 *
 * On failure the function logs a warning on the Redis log. */
void sentinelFlushConfig(void) {
    int fd = -1;
    int saved_hz = server.hz;
    int rewrite_status;

    server.hz = CONFIG_DEFAULT_HZ;
    rewrite_status = rewriteConfig(server.configfile, 0);
    server.hz = saved_hz;

    if (rewrite_status == -1) goto werr;
    if ((fd = open(server.configfile,O_RDONLY)) == -1) goto werr;
    if (fsync(fd) == -1) goto werr;
    if (close(fd) == EOF) goto werr;
    return;

werr:
    serverLog(LL_WARNING,"WARNING: Sentinel was not able to save the new configuration on disk!!!: %s", strerror(errno));
    if (fd != -1) close(fd);
}

/* ====================== hiredis connection handling ======================= */

/* Send the AUTH command with the specified master password if needed.
 * Note that for slaves the password set for the master is used.
 *
 * In case this Sentinel requires a password as well, via the "requirepass"
 * configuration directive, we assume we should use the local password in
 * order to authenticate when connecting with the other Sentinels as well.
 * So basically all the Sentinels share the same password and use it to
 * authenticate reciprocally.
 *
 * We don't check at all if the command was successfully transmitted
 * to the instance as if it fails Sentinel will detect the instance down,
 * will disconnect and reconnect the link and so forth. */
void sentinelSendAuthIfNeeded(sentinelRedisInstance *ri, redisAsyncContext *c) {
    char *auth_pass = NULL;
    char *auth_user = NULL;

    if (ri->flags & SRI_MASTER) {
        auth_pass = ri->auth_pass;
        auth_user = ri->auth_user;
    } else if (ri->flags & SRI_SLAVE) {
        auth_pass = ri->master->auth_pass;
        auth_user = ri->master->auth_user;
    } else if (ri->flags & SRI_SENTINEL) {
        /* If sentinel_auth_user is NULL, AUTH will use default user
           with sentinel_auth_pass to authenticate */
        if (sentinel.sentinel_auth_pass) {
            auth_pass = sentinel.sentinel_auth_pass;
            auth_user = sentinel.sentinel_auth_user;
        } else {
            /* Compatibility with old configs. requirepass is used
             * for both incoming and outgoing authentication. */
            auth_pass = server.requirepass;
            auth_user = NULL;
        }
    }

    if (auth_pass && auth_user == NULL) {
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s",
            sentinelInstanceMapCommand(ri,"AUTH"),
            auth_pass) == C_OK) ri->link->pending_commands++;
    } else if (auth_pass && auth_user) {
        /* If we also have an username, use the ACL-style AUTH command
         * with two arguments, username and password. */
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s %s",
            sentinelInstanceMapCommand(ri,"AUTH"),
            auth_user, auth_pass) == C_OK) ri->link->pending_commands++;
    }
}

/* Use CLIENT SETNAME to name the connection in the Redis instance as
 * sentinel-<first_8_chars_of_runid>-<connection_type>
 * The connection type is "cmd" or "pubsub" as specified by 'type'.
 *
 * This makes it possible to list all the sentinel instances connected
 * to a Redis server with CLIENT LIST, grepping for a specific name format. */
void sentinelSetClientName(sentinelRedisInstance *ri, redisAsyncContext *c, char *type) {
    char name[64];

    snprintf(name,sizeof(name),"sentinel-%.8s-%s",sentinel.myid,type);
    if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri,
        "%s SETNAME %s",
        sentinelInstanceMapCommand(ri,"CLIENT"),
        name) == C_OK)
    {
        ri->link->pending_commands++;
    }
}

static int instanceLinkNegotiateTLS(redisAsyncContext *context) {
#ifndef USE_OPENSSL
    (void) context;
#else
    if (!redis_tls_ctx) return C_ERR;
    SSL *ssl = SSL_new(redis_tls_client_ctx ? redis_tls_client_ctx : redis_tls_ctx);
    if (!ssl) return C_ERR;

    if (redisInitiateSSL(&context->c, ssl) == REDIS_ERR) {
        SSL_free(ssl);
        return C_ERR;
    }
#endif
    return C_OK;
}

/* Create the async connections for the instance link if the link
 * is disconnected. Note that link->disconnected is true even if just
 * one of the two links (commands and pub/sub) is missing.
 *
 *  当链路断开时，为实例链路创建异步连接。注意，即使两个链接(commands和pubsub)中只有一个丢失，link->disconnected也为true。
 * */
void sentinelReconnectInstance(sentinelRedisInstance *ri) {

    // 如果指定节点和当前sentinel的连接在，那么直接返回
    if (ri->link->disconnected == 0) return;
    if (ri->addr->port == 0) return; /* port == 0 means invalid address. */
    instanceLink *link = ri->link;
    mstime_t now = mstime();

    // 如果没到下一次ping时间，也不建立连接，直接返回
    // 每隔1秒， Sentinel节点会向主节点、从节点、其余Sentinel节点 发送一条ping命令做一次心跳检测，来确认这些节点当前是否可达
    if (now - ri->link->last_reconn_time < SENTINEL_PING_PERIOD) return;
    ri->link->last_reconn_time = now;

    /* Commands connection. */
    // 【1】 创建命令连接
    if (link->cc == NULL) {

        /* It might be that the instance is disconnected because it wasn't available earlier when the instance
         * allocated, say during failover, and therefore we failed to resolve its ip.
         * Another scenario is that the instance restarted with new ip, and we should resolve its new ip based on
         * its hostname */
        if (sentinel.resolve_hostnames) {
            sentinelAddr *tryResolveAddr = createSentinelAddr(ri->addr->hostname, ri->addr->port, 0);
            if (tryResolveAddr != NULL) {
                releaseSentinelAddr(ri->addr);
                ri->addr = tryResolveAddr;
            }
        }

        /*
         * redisAsyncConnectBind，redisAsyncCommand 等函数来自于hiredis库。 它是redis提供的一个轻量级的c语言客户端库，
         * 这两个函数负责建立异步连接并发送异步请求
         *
         * 说明一下 redisAsyncCommand 函数，它的函数原型为：
         *      int redisAsyncCommand(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, const char *format, ...)
         * 参数说明：
         *      ac: 连接上下文；
         *      fn: 回调函数；
         *      privdata: 用户回调函数的附加参数；
         */
        link->cc = redisAsyncConnectBind(ri->addr->ip,ri->addr->port,NET_FIRST_BIND_ADDR);

        if (link->cc && !link->cc->err) anetCloexec(link->cc->c.fd);
        if (!link->cc) {
            sentinelEvent(LL_DEBUG,"-cmd-link-reconnection",ri,"%@ #Failed to establish connection");
        } else if (!link->cc->err && server.tls_replication &&
                (instanceLinkNegotiateTLS(link->cc) == C_ERR)) {
            sentinelEvent(LL_DEBUG,"-cmd-link-reconnection",ri,"%@ #Failed to initialize TLS");
            instanceLinkCloseConnection(link,link->cc);
        } else if (link->cc->err) {
            sentinelEvent(LL_DEBUG,"-cmd-link-reconnection",ri,"%@ #%s",
                link->cc->errstr);
            instanceLinkCloseConnection(link,link->cc);
        } else {
            link->pending_commands = 0;
            link->cc_conn_time = mstime();
            link->cc->data = link;
            redisAeAttach(server.el,link->cc);
            redisAsyncSetConnectCallback(link->cc,
                    sentinelLinkEstablishedCallback);
            redisAsyncSetDisconnectCallback(link->cc,
                    sentinelDisconnectCallback);
            sentinelSendAuthIfNeeded(ri,link->cc);
            sentinelSetClientName(ri,link->cc,"cmd");

            /* Send a PING ASAP when reconnecting. */
            sentinelSendPing(ri);
        }
    }

    /* Pub / Sub */
    // 【2】 如果待连接的节点是主从节点，则创建订阅连接
    if ((ri->flags & (SRI_MASTER|SRI_SLAVE)) && link->pc == NULL) {
        link->pc = redisAsyncConnectBind(ri->addr->ip,ri->addr->port,NET_FIRST_BIND_ADDR);
        if (link->pc && !link->pc->err) anetCloexec(link->pc->c.fd);
        if (!link->pc) {
            sentinelEvent(LL_DEBUG,"-pubsub-link-reconnection",ri,"%@ #Failed to establish connection");
        } else if (!link->pc->err && server.tls_replication &&
                (instanceLinkNegotiateTLS(link->pc) == C_ERR)) {
            sentinelEvent(LL_DEBUG,"-pubsub-link-reconnection",ri,"%@ #Failed to initialize TLS");
        } else if (link->pc->err) {
            sentinelEvent(LL_DEBUG,"-pubsub-link-reconnection",ri,"%@ #%s",
                link->pc->errstr);
            instanceLinkCloseConnection(link,link->pc);
        } else {
            int retval;
            link->pc_conn_time = mstime();
            link->pc->data = link;
            redisAeAttach(server.el,link->pc);
            redisAsyncSetConnectCallback(link->pc,
                    sentinelLinkEstablishedCallback);
            redisAsyncSetDisconnectCallback(link->pc,
                    sentinelDisconnectCallback);
            sentinelSendAuthIfNeeded(ri,link->pc);
            sentinelSetClientName(ri,link->pc,"pubsub");
            /* Now we subscribe to the Sentinels "Hello" channel. */
            // 【3】 为订阅连接注册回调函数 sentinelReceiveHelloMessages ，负责处理sentinel频道收到的数据
            retval = redisAsyncCommand(link->pc,
                sentinelReceiveHelloMessages, ri, "%s %s",
                sentinelInstanceMapCommand(ri,"SUBSCRIBE"),
                SENTINEL_HELLO_CHANNEL);
            if (retval != C_OK) {
                /* If we can't subscribe, the Pub/Sub connection is useless
                 * and we can simply disconnect it and try again. */
                instanceLinkCloseConnection(link,link->pc);
                return;
            }
        }
    }
    /* Clear the disconnected status only if we have both the connections
     * (or just the commands connection if this is a sentinel instance). */
    if (link->cc && (ri->flags & SRI_SENTINEL || link->pc))
        link->disconnected = 0;
}

/* ======================== Redis instances pinging  ======================== */

/* Return true if master looks "sane", that is:
 * 1) It is actually a master in the current configuration.
 * 2) It reports itself as a master.
 * 3) It is not SDOWN or ODOWN.
 * 4) We obtained last INFO no more than two times the INFO period time ago.
 *
 * 判断指定的是否是正常的主节点
 **/
int sentinelMasterLooksSane(sentinelRedisInstance *master) {
    return
        master->flags & SRI_MASTER &&
        master->role_reported == SRI_MASTER &&
        (master->flags & (SRI_S_DOWN|SRI_O_DOWN)) == 0 &&
        (mstime() - master->info_refresh) < SENTINEL_INFO_PERIOD*2;
}

/* Process the INFO output from masters. */
void sentinelRefreshInstanceInfo(sentinelRedisInstance *ri, const char *info) {
    sds *lines;
    int numlines, j;
    int role = 0;

    /* cache full INFO output for instance */
    sdsfree(ri->info);
    ri->info = sdsnew(info);

    /* The following fields must be reset to a given value in the case they
     * are not found at all in the INFO output.
     * 如果在INFO输出中根本找不到以下字段，则必须将其重置为给定值。                                                            <-[1]
     * */
    ri->master_link_down_time = 0;

    /* Process line by line. 还是按行分割*/
    lines = sdssplitlen(info,strlen(info),"\r\n",2,&numlines);
    for (j = 0; j < numlines; j++) {
        sentinelRedisInstance *slave;
        sds l = lines[j];

        /* run_id:<40 hex chars>*/
        if (sdslen(l) >= 47 && !memcmp(l,"run_id:",7)) {
            if (ri->runid == NULL) {
                ri->runid = sdsnewlen(l+7,40);
            } else {
                if (strncmp(ri->runid,l+7,40) != 0) {
                    sentinelEvent(LL_NOTICE,"+reboot",ri,"%@");
                    sdsfree(ri->runid);
                    ri->runid = sdsnewlen(l+7,40);
                }
            }
        }

        /* old versions: slave0:<ip>,<port>,<state>
         * new versions: slave0:ip=127.0.0.1,port=9999,...
         *
         * slave0:ip=10.237.181.69,port=6379,state=online,offset=5906659286,lag=0
         * */
        if ((ri->flags & SRI_MASTER) &&
            sdslen(l) >= 7 &&
            !memcmp(l,"slave",5) && isdigit(l[5]))
        {
            char *ip, *port, *end;

            if (strstr(l,"ip=") == NULL) {
                /* Old format. */
                ip = strchr(l,':'); if (!ip) continue;
                ip++; /* Now ip points to start of ip address. */
                port = strchr(ip,','); if (!port) continue;
                *port = '\0'; /* nul term for easy access. */
                port++; /* Now port points to start of port number. */
                end = strchr(port,','); if (!end) continue;
                *end = '\0'; /* nul term for easy access. */
            } else {
                /* New format. */
                ip = strstr(l,"ip="); if (!ip) continue;
                ip += 3; /* Now ip points to start of ip address. */
                port = strstr(l,"port="); if (!port) continue;
                port += 5; /* Now port points to start of port number. */
                /* Nul term both fields for easy access. */
                end = strchr(ip,','); if (end) *end = '\0';
                end = strchr(port,','); if (end) *end = '\0';
            }

            /* Check if we already have this slave into our table,
             * otherwise add it.
             * 检查当前主节点的slaves字典里是否有该slave,如果没有,则创建一个对应的SentinelRedisInstance实例,并添加到slave字典中. <-[2]
             * 再此之前, 可能slave已经在前面通过sentinel.conf读入了, 这里就不再创建.
             * */
            if (sentinelRedisInstanceLookupSlave(ri,ip,atoi(port)) == NULL) {
                if ((slave = createSentinelRedisInstance(NULL,SRI_SLAVE,ip,
                            atoi(port), ri->quorum, ri)) != NULL)
                {
                    sentinelEvent(LL_NOTICE,"+slave",slave,"%@");
                    sentinelFlushConfig();
                }
            }
        }

        /* master_link_down_since_seconds:<seconds> */
        if (sdslen(l) >= 32 &&
            !memcmp(l,"master_link_down_since_seconds",30))
        {
            ri->master_link_down_time = strtoll(l+31,NULL,10)*1000;
        }

        /* role:<role> */
        if (sdslen(l) >= 11 && !memcmp(l,"role:master",11)) role = SRI_MASTER;
        else if (sdslen(l) >= 10 && !memcmp(l,"role:slave",10)) role = SRI_SLAVE;

        if (role == SRI_SLAVE) {
            /* master_host:<host>
             * # Replication
             * role:slave
             * master_host:10.237.181.23
             * master_port:6379
             * master_link_status:up
             * master_last_io_seconds_ago:1
             * master_sync_in_progress:0
             * slave_repl_offset:5906692510
             * slave_priority:100
             * slave_read_only:1
             * connected_slaves:0
             * master_repl_offset:0
             * repl_backlog_active:0
             * repl_backlog_size:1048576
             * repl_backlog_first_byte_offset:0
             * repl_backlog_histlen:0
             * */
            if (sdslen(l) >= 12 && !memcmp(l,"master_host:",12)) {
                if (ri->slave_master_host == NULL ||
                    strcasecmp(l+12,ri->slave_master_host))
                {
                    sdsfree(ri->slave_master_host);
                    ri->slave_master_host = sdsnew(l+12);
                    ri->slave_conf_change_time = mstime();
                }
            }

            /* master_port:<port> */
            if (sdslen(l) >= 12 && !memcmp(l,"master_port:",12)) {
                int slave_master_port = atoi(l+12);

                if (ri->slave_master_port != slave_master_port) {
                    ri->slave_master_port = slave_master_port;
                    ri->slave_conf_change_time = mstime();
                }
            }

            /* master_link_status:<status> */
            if (sdslen(l) >= 19 && !memcmp(l,"master_link_status:",19)) {
                ri->slave_master_link_status =
                    (strcasecmp(l+19,"up") == 0) ?
                    SENTINEL_MASTER_LINK_STATUS_UP :
                    SENTINEL_MASTER_LINK_STATUS_DOWN;
            }

            /* slave_priority:<priority> */
            if (sdslen(l) >= 15 && !memcmp(l,"slave_priority:",15))
                ri->slave_priority = atoi(l+15);

            /* slave_repl_offset:<offset> */
            if (sdslen(l) >= 18 && !memcmp(l,"slave_repl_offset:",18))
                ri->slave_repl_offset = strtoull(l+18,NULL,10);

            /* replica_announced:<announcement> */
            if (sdslen(l) >= 18 && !memcmp(l,"replica_announced:",18))
                ri->replica_announced = atoi(l+18);
        }
    }
    ri->info_refresh = mstime();
    sdsfreesplitres(lines,numlines);

    /* ---------------------------- Acting half -----------------------------
     * Some things will not happen if sentinel.tilt is true, but some will
     * still be processed. */

    /* Remember when the role changed. */
    if (role != ri->role_reported) {
        // 如果发现角色跟之前的不一样，说明可能发生了主从切换
        ri->role_reported_time = mstime();
        ri->role_reported = role;
        if (role == SRI_SLAVE) ri->slave_conf_change_time = mstime();
        /* Log the event with +role-change if the new role is coherent or
         * with -role-change if there is a mismatch with the current config. */
        sentinelEvent(LL_VERBOSE,
            ((ri->flags & (SRI_MASTER|SRI_SLAVE)) == role) ?
            "+role-change" : "-role-change",
            ri, "%@ new reported role is %s",
            role == SRI_MASTER ? "master" : "slave");
    }

    /* None of the following conditions are processed when in tilt mode, so
     * return asap. */
    if (sentinel.tilt) return;

    /* Handle master -> slave role switch. */
    if ((ri->flags & SRI_MASTER) && role == SRI_SLAVE) {
        /* Nothing to do, but masters claiming to be slaves are
         * considered to be unreachable by Sentinel, so eventually
         * a failover will be triggered.
         *
         * 什么都不做，但是声称是slave的master被哨兵认为是不可访问的，所以最终会触发故障转移。
         * */
    }

    /* Handle slave -> master role switch.
     * 处理主从切换场景。 当前实例中是从节点，但INFO信息中它是主节点。
     * */
    if ((ri->flags & SRI_SLAVE) && role == SRI_MASTER) {
        /* If this is a promoted slave we can change state to the
         * failover state machine.
         * 如果这是一个晋升的slave节点，我们可以将状态更改为故障转移状态机。                                                   <-[3]
         * */
        if ((ri->flags & SRI_PROMOTED) &&
            (ri->master->flags & SRI_FAILOVER_IN_PROGRESS) &&
            (ri->master->failover_state ==
                SENTINEL_FAILOVER_STATE_WAIT_PROMOTION)) // 主节点记录的故障转移状态是 等待从节点晋升
        {
            /* Now that we are sure the slave was reconfigured as a master
             * set the master configuration epoch to the epoch we won the
             * election to perform this failover. This will force the other
             * Sentinels to update their config (assuming there is not
             * a newer one already available).
             *
             * 现在我们确定从服务器被重新配置为主服务器，将原来的主服务器配置纪元设置为我们赢得选举后执行此故障转移的纪元。
             * 这将迫使其他哨兵更新他们的配置，因为一般来说，此时当前节点的配置纪元是最高的(假设没有更新的配置可用)。
             * */
            ri->master->config_epoch = ri->master->failover_epoch;
            ri->master->failover_state = SENTINEL_FAILOVER_STATE_RECONF_SLAVES; // 推进故障转移状态机，设置主节点的故障转移状态为 重新配置从节点 <-[4]
            ri->master->failover_state_change_time = mstime(); // 记录状态变更时间
            sentinelFlushConfig(); // 注意这一步，这里会重写sentinel配置文件，以记录最新的故障转移状态。                       <-[5]
            sentinelEvent(LL_WARNING,"+promoted-slave",ri,"%@");
            if (sentinel.simfailure_flags &
                SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION)
                sentinelSimFailureCrash();
            sentinelEvent(LL_WARNING,"+failover-state-reconf-slaves",
                ri->master,"%@");
            sentinelCallClientReconfScript(ri->master,SENTINEL_LEADER,
                "start",ri->master->addr,ri->addr);
            // 这里立即给其他sentinel节点发送publish hello消息的主要目的是让sentinel之间的信息尽快达成一致
            sentinelForceHelloUpdateForMaster(ri->master);
        } else {
            /* A slave turned into a master. We want to force our view and
             * reconfigure as slave. Wait some time after the change before
             * going forward, to receive new configs if any.
             *
             * slave变成了master。我们希望强制将我们的视图重新配置为slave视图。在更改之后等待一段时间，然后再继续，以接收新的配置(如果有的话)。
             * 为什么要等待一段时间呢？因为可能有其他sentinel节点也发现了这个slave节点变成了master，但是他们的配置纪元可能比当前节点的配置纪元要高（一般就是指上次故障转移的leader节点），
             *
             * 处理 slave变为master的场景。比如可能直接在slave上执行了一次slaveof no one命令。
             * 以及更重要的：在故障转移完成后，leader节点会将旧的master节点添加到故障转移实例的slaves字典中，此时在leader节点的视角，
             * 看来，旧的master节点是一个slave， 但是由于旧的master节点尚未执行slaveof命令，所以实际的info中返回的还是一个master,
             * 并且该实例目前也并未处于故障转移状态，所以会走进这个分支，由 sentinel leader 节点对old master节点执行一次slaveof命令。
             *
             * */
            mstime_t wait_time = SENTINEL_PUBLISH_PERIOD*4;

            if (!(ri->flags & SRI_PROMOTED) &&
                 sentinelMasterLooksSane(ri->master) &&
                 sentinelRedisInstanceNoDownFor(ri,wait_time) &&
                 mstime() - ri->role_reported_time > wait_time)
            {
                int retval = sentinelSendSlaveOf(ri,ri->master->addr);
                if (retval == C_OK)
                    sentinelEvent(LL_NOTICE,"+convert-to-slave",ri,"%@");
            }
        }
    }

    /* Handle slaves replicating to a different master address.
     *
     * 纠正和当前sentinel视角不符的slave节点的主节点
     * */
    if ((ri->flags & SRI_SLAVE) &&
        role == SRI_SLAVE &&
        (ri->slave_master_port != ri->master->addr->port ||
         !sentinelAddrEqualsHostname(ri->master->addr, ri->slave_master_host)))
    {
        mstime_t wait_time = ri->master->failover_timeout;

        /* Make sure the master is sane before reconfiguring this instance
         * into a slave.
         * 在将此实例重新配置为slave之前，请确保master是正常的
         * sentinelMasterLooksSane 用于判断主实例的状态是否正常
         * sentinelRedisInstanceNoDownFor 用于判断从实例是否已经连续一段时间没有sdown或odown
         * */
        if (sentinelMasterLooksSane(ri->master) &&
            sentinelRedisInstanceNoDownFor(ri,wait_time) &&
            mstime() - ri->slave_conf_change_time > wait_time)
        {
            // 这里发现如果INFO命令的目标从节点的主节点ip或端口和当前sentinel记录的主节点不一致，则向目标从节点发送slaveof命令，将  <-[6]
            // 其主节点纠正为当前sentinel认为的主节点
            // 故障转移不走这个分支(因为进入这里的条件有role == SRI_SLAVE)，所以不用担心影响故障转移
            int retval = sentinelSendSlaveOf(ri,ri->master->addr);
            if (retval == C_OK)
                sentinelEvent(LL_NOTICE,"+fix-slave-config",ri,"%@");
        }
    }

    /* Detect if the slave that is in the process of being reconfigured
     * changed state.
     *
     * 检测正在重新配置的slave服务器是否改变了状态. 这里主要处理故障转移过程中，从节点slaveof新的主节点的情况
     * */
    if ((ri->flags & SRI_SLAVE) && role == SRI_SLAVE &&
        (ri->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG)))
    {
        // 如果返回INFO响应的节点实例存在SRI_RECONF_SENT标记， 并且该节点的主节点已经是晋升的节点， 则将该节点实例的主从状态标记变更为<-【7】
        // SRI_RECONF_INPROG, 代表主从节点正在建立主从关系。
        /* SRI_RECONF_SENT -> SRI_RECONF_INPROG. */
        if ((ri->flags & SRI_RECONF_SENT) &&
            ri->slave_master_host &&
            sentinelAddrEqualsHostname(ri->master->promoted_slave->addr,
                ri->slave_master_host) &&
            ri->slave_master_port == ri->master->promoted_slave->addr->port)
        {
            // 这个状态说明当前从节点已经保存了新的主节点的信息，但是还没有建立主从关系
            ri->flags &= ~SRI_RECONF_SENT;
            ri->flags |= SRI_RECONF_INPROG;
            sentinelEvent(LL_NOTICE,"+slave-reconf-inprog",ri,"%@");
        }

        // 如果返回INFO响应的节点的实例存在 SRI_RECONF_INPROG 标记， 并且该节点的主节点的主从连接已经处于在线状态， 则将该节点     <-【8】
        // 实例的主从状态标记变更为 SRI_RECONF_DONE, 代表主从节点已经建立主从关系完成。
        /* SRI_RECONF_INPROG -> SRI_RECONF_DONE */
        if ((ri->flags & SRI_RECONF_INPROG) &&
            ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP)
        {
            // slave_master_link_status为up说明主从之间已经成功建立主从连接
            ri->flags &= ~SRI_RECONF_INPROG;
            ri->flags |= SRI_RECONF_DONE;
            sentinelEvent(LL_NOTICE,"+slave-reconf-done",ri,"%@");
        }
    }
}

void sentinelInfoReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    if (r->type == REDIS_REPLY_STRING)
        sentinelRefreshInstanceInfo(ri,r->str);
}

/* Just discard the reply. We use this when we are not monitoring the return
 * value of the command but its effects directly. */
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    instanceLink *link = c->data;
    UNUSED(reply);
    UNUSED(privdata);

    if (link) link->pending_commands--;
}

void sentinelPingReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    if (r->type == REDIS_REPLY_STATUS ||
        r->type == REDIS_REPLY_ERROR) {
        /* Update the "instance available" field only if this is an
         * acceptable reply. */
        if (strncmp(r->str,"PONG",4) == 0 ||
            strncmp(r->str,"LOADING",7) == 0 ||
            strncmp(r->str,"MASTERDOWN",10) == 0)
        {
            link->last_avail_time = mstime();
            link->act_ping_time = 0; /* Flag the pong as received. */
        } else {
            /* Send a SCRIPT KILL command if the instance appears to be
             * down because of a busy script. */
            if (strncmp(r->str,"BUSY",4) == 0 &&
                (ri->flags & SRI_S_DOWN) &&
                !(ri->flags & SRI_SCRIPT_KILL_SENT))
            {
                if (redisAsyncCommand(ri->link->cc,
                        sentinelDiscardReplyCallback, ri,
                        "%s KILL",
                        sentinelInstanceMapCommand(ri,"SCRIPT")) == C_OK)
                {
                    ri->link->pending_commands++;
                }
                ri->flags |= SRI_SCRIPT_KILL_SENT;
            }
        }
    }
    link->last_pong_time = mstime();
}

/* This is called when we get the reply about the PUBLISH command we send
 * to the master to advertise this sentinel. */
void sentinelPublishReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    /* Only update pub_time if we actually published our message. Otherwise
     * we'll retry again in 100 milliseconds. */
    if (r->type != REDIS_REPLY_ERROR)
        ri->last_pub_time = mstime();
}

/* Process a hello message received via Pub/Sub in master or slave instance,
 * or sent directly to this sentinel via the (fake) PUBLISH command of Sentinel.
 *
 * If the master name specified in the message is not known, the message is
 * discarded.
 *
 * 这个函数用于处理收到的hello消息. sentinel接受该消息以实现：
 *      发现新的Sentinel节点：通过订阅主节点的__sentinel__：hello了解其他的Sentinel节点信息，如果是新加入的Sentinel节点，将该Sentinel节点信息保存起来，并与该Sentinel节点创建连接。
 *      Sentinel节点之间交换主节点的状态，作为后面客观下线以及领导者选举的依据
 *      让除leader节点之外的其他sentinel节点发现故障转移以后的主节点
 * */
void sentinelProcessHelloMessage(char *hello, int hello_len) {
    /* Format is composed of 8 tokens:
     * 0=ip,1=port,2=runid,3=current_epoch,4=master_name, 5=master_ip,6=master_port,7=master_config_epoch. */
    int numtokens, port, removed, master_port;
    uint64_t current_epoch, master_config_epoch;
    char **token = sdssplitlen(hello, hello_len, ",", 1, &numtokens);
    sentinelRedisInstance *si, *master;

    if (numtokens == 8) {
        /* Obtain a reference to the master this hello message is about */
        master = sentinelGetMasterByName(token[4]);
        if (!master) goto cleanup; /* Unknown master, skip the message. */

        /* First, try to see if we already have this sentinel.
         * 1、先看看hello消息中的sentinel节点本地有没有. 要求runid和ip都相同才算有
         * */
        port = atoi(token[1]);
        master_port = atoi(token[6]);
        // si表示 发送该hello消息的sentinel
        si = getSentinelRedisInstanceByAddrAndRunID(
                        master->sentinels,token[0],port,token[2]);
        // 发送hello消息的sentinel节点的配置纪元
        current_epoch = strtoull(token[3],NULL,10);
        // master节点的配置纪元
        master_config_epoch = strtoull(token[7],NULL,10);

        if (!si) {
            /* If not, remove all the sentinels that have the same runid
             * because there was an address change, and add the same Sentinel
             * with the new address back.
             * 如果没有，删除所有具有相同runid的哨兵，因为有一个地址更改，并添加相同的哨兵与新地址。                              <-【1】
             * 这里解决了类似容器环境下，sentinel节点的地址变化的问题
             * */
            removed = removeMatchingSentinelFromMaster(master,token[2]);
            if (removed) {
                // removed为1表示sentinels字典中由这个runid，并且成功删除了
                sentinelEvent(LL_NOTICE,"+sentinel-address-switch",master,
                    "%@ ip %s port %d for %s", token[0],port,token[2]);
            } else {
                /* Check if there is another Sentinel with the same address this
                 * new one is reporting. What we do if this happens is to set its
                 * port to 0, to signal the address is invalid. We'll update it
                 * later if we get an HELLO message.
                 *
                 * 查一下是否还有另一个哨兵和这个新报告的地址相同。如果发生这种情况，我们要做的是将其端口设置为0，表示地址无效。如果稍后收到HELLO消息，我们将更新它。
                 * */
                sentinelRedisInstance *other =
                    getSentinelRedisInstanceByAddrAndRunID(
                        master->sentinels, token[0],port,NULL);
                if (other) {
                    sentinelEvent(LL_NOTICE,"+sentinel-invalid-addr",other,"%@");
                    other->addr->port = 0; /* It means: invalid address. */
                    sentinelUpdateSentinelAddressInAllMasters(other);
                }
            }

            /* Add the new sentinel.
             * 创建一个hello消息中指定的sentinel节点的实例添加到sentinel字典中                                               <-【1】
             * */
            si = createSentinelRedisInstance(token[2],SRI_SENTINEL,
                            token[0],port,master->quorum,master);

            if (si) {
                if (!removed) sentinelEvent(LL_NOTICE,"+sentinel",si,"%@");
                /* The runid is NULL after a new instance creation and
                 * for Sentinels we don't have a later chance to fill it,
                 * so do it now. */
                si->runid = sdsnew(token[2]);
                sentinelTryConnectionSharing(si);
                if (removed) sentinelUpdateSentinelAddressInAllMasters(si);
                sentinelFlushConfig();
            }
        }

        /* Update local current_epoch if received current_epoch is greater.
         * 如果收到的current_epoch较大，则更新本地current_epoch
         * */
        if (current_epoch > sentinel.current_epoch) {
            sentinel.current_epoch = current_epoch;
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING,"+new-epoch",master,"%llu",
                (unsigned long long) sentinel.current_epoch);
        }

        // 由于频道消息中的主节点config_epoch大于当前节点视图中的主节点config_epoch，说明发送节点完成了新的故障转移，更新          <-【2】
        // 当前sentinel节点的master字典和配置文件
        /* Update master info if received configuration is newer. */
        if (si && master->config_epoch < master_config_epoch) {
            // 更新主节点的配置纪元
            master->config_epoch = master_config_epoch;
            if (master_port != master->addr->port ||
                !sentinelAddrEqualsHostname(master->addr, token[5]))
            {
                // 如果发现主节点的地址变了
                sentinelAddr *old_addr;

                sentinelEvent(LL_WARNING,"+config-update-from",si,"%@");
                sentinelEvent(LL_WARNING,"+switch-master",
                    master,"%s %s %d %s %d",
                    master->name,
                    announceSentinelAddr(master->addr), master->addr->port,
                    token[5], master_port);

                old_addr = dupSentinelAddr(master->addr);
                // 变更master字典中的主节点为新的主节点，并重新发现从节点                                                     <-【3】
                sentinelResetMasterAndChangeAddress(master, token[5], master_port);
                sentinelCallClientReconfScript(master,
                    SENTINEL_OBSERVER,"start",
                    old_addr,master->addr);// 调用 sentinel client-reconfig-script 中配置的脚本                 <-【4】
                releaseSentinelAddr(old_addr);
            }
        }

        /* Update the state of the Sentinel. */
        if (si) si->last_hello_time = mstime();
    }

cleanup:
    sdsfreesplitres(token,numtokens);
}


/* This is our Pub/Sub callback for the Hello channel. It's useful in order
 * to discover other sentinels attached at the same master. */
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    redisReply *r;
    UNUSED(c);

    if (!reply || !ri) return;
    r = reply;

    /* Update the last activity in the pubsub channel. Note that since we
     * receive our messages as well this timestamp can be used to detect
     * if the link is probably disconnected even if it seems otherwise. */
    ri->link->pc_last_activity = mstime();

    /* Sanity check in the reply we expect, so that the code that follows
     * can avoid to check for details. */
    if (r->type != REDIS_REPLY_ARRAY ||
        r->elements != 3 ||
        r->element[0]->type != REDIS_REPLY_STRING ||
        r->element[1]->type != REDIS_REPLY_STRING ||
        r->element[2]->type != REDIS_REPLY_STRING ||
        strcmp(r->element[0]->str,"message") != 0) return;

    /* We are not interested in meeting ourselves */
    if (strstr(r->element[2]->str,sentinel.myid) != NULL) return;

    sentinelProcessHelloMessage(r->element[2]->str, r->element[2]->len);
}

/* Send a "Hello" message via Pub/Sub to the specified 'ri' Redis
 * instance in order to broadcast the current configuration for this
 * master, and to advertise the existence of this Sentinel at the same time.
 *
 * The message has the following format:
 *
 * sentinel_ip,sentinel_port,sentinel_runid,current_epoch,
 * master_name,master_ip,master_port,master_config_epoch.
 *
 * Returns C_OK if the PUBLISH was queued correctly, otherwise
 * C_ERR is returned. */
int sentinelSendHello(sentinelRedisInstance *ri) {
    char ip[NET_IP_STR_LEN];
    char payload[NET_IP_STR_LEN+1024];
    int retval;
    char *announce_ip;
    int announce_port;
    sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ? ri : ri->master;
    sentinelAddr *master_addr = sentinelGetCurrentMasterAddress(master);

    if (ri->link->disconnected) return C_ERR;

    /* Use the specified announce address if specified, otherwise try to
     * obtain our own IP address. */
    if (sentinel.announce_ip) {
        announce_ip = sentinel.announce_ip;
    } else {
        if (anetFdToString(ri->link->cc->c.fd,ip,sizeof(ip),NULL,FD_TO_SOCK_NAME) == -1)
            return C_ERR;
        announce_ip = ip;
    }
    if (sentinel.announce_port) announce_port = sentinel.announce_port;
    else if (server.tls_replication && server.tls_port) announce_port = server.tls_port;
    else announce_port = server.port;

    /* Format and send the Hello message. */
    snprintf(payload,sizeof(payload),
        "%s,%d,%s,%llu," /* Info about this sentinel. */
        "%s,%s,%d,%llu", /* Info about current master. */
        announce_ip, announce_port, sentinel.myid,
        (unsigned long long) sentinel.current_epoch,
        /* --- */
        master->name,announceSentinelAddr(master_addr),master_addr->port,
        (unsigned long long) master->config_epoch);
    retval = redisAsyncCommand(ri->link->cc,
        sentinelPublishReplyCallback, ri, "%s %s %s",
        sentinelInstanceMapCommand(ri,"PUBLISH"),
        SENTINEL_HELLO_CHANNEL,payload);
    if (retval != C_OK) return C_ERR;
    ri->link->pending_commands++;
    return C_OK;
}

/* Reset last_pub_time in all the instances in the specified dictionary
 * in order to force the delivery of a Hello update ASAP.
 * 重置指定字典中所有实例中的 last_pub_time，以便强制尽快交付Hello更新
 * */
void sentinelForceHelloUpdateDictOfRedisInstances(dict *instances) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        if (ri->last_pub_time >= (SENTINEL_PUBLISH_PERIOD+1))
            ri->last_pub_time -= (SENTINEL_PUBLISH_PERIOD+1);
    }
    dictReleaseIterator(di);
}

/* This function forces the delivery of a "Hello" message (see
 * sentinelSendHello() top comment for further information) to all the Redis
 * and Sentinel instances related to the specified 'master'.
 *
 * It is technically not needed since we send an update to every instance
 * with a period of SENTINEL_PUBLISH_PERIOD milliseconds, however when a
 * Sentinel upgrades a configuration it is a good idea to deliver an update
 * to the other Sentinels ASAP.
 *
 * 这个函数强制传递一个“Hello”消息(见sentinelSendHello() top comment了解更多信息)到所有与指定的“master”相关的Redis和Sentinel实例。
 * 这在技术上是不需要的，因为我们给每个实例发送一个周期为 SENTINEL_PUBLISH_PERIOD 毫秒的更新，但是当一个哨兵升级配置时，最好尽快向其他哨兵发送更新。
 * */
int sentinelForceHelloUpdateForMaster(sentinelRedisInstance *master) {
    if (!(master->flags & SRI_MASTER)) return C_ERR;
    if (master->last_pub_time >= (SENTINEL_PUBLISH_PERIOD+1))
        master->last_pub_time -= (SENTINEL_PUBLISH_PERIOD+1);
    sentinelForceHelloUpdateDictOfRedisInstances(master->sentinels);
    sentinelForceHelloUpdateDictOfRedisInstances(master->slaves);
    return C_OK;
}

/* Send a PING to the specified instance and refresh the act_ping_time
 * if it is zero (that is, if we received a pong for the previous ping).
 *
 * On error zero is returned, and we can't consider the PING command
 * queued in the connection. */
int sentinelSendPing(sentinelRedisInstance *ri) {
    int retval = redisAsyncCommand(ri->link->cc,
        sentinelPingReplyCallback, ri, "%s",
        sentinelInstanceMapCommand(ri,"PING"));
    if (retval == C_OK) {
        ri->link->pending_commands++;
        ri->link->last_ping_time = mstime();
        /* We update the active ping time only if we received the pong for
         * the previous ping, otherwise we are technically waiting since the
         * first ping that did not receive a reply. */
        if (ri->link->act_ping_time == 0)
            ri->link->act_ping_time = ri->link->last_ping_time;
        return 1;
    } else {
        return 0;
    }
}

/* Send periodic PING, INFO, and PUBLISH to the Hello channel to
 * the specified master or slave instance.
 * 定期向指定的主实例或从实例发送PING、INFO和PUBLISH到Hello通道。
 * */
void sentinelSendPeriodicCommands(sentinelRedisInstance *ri) {
    mstime_t now = mstime();
    mstime_t info_period, ping_period;
    int retval;

    /* Return ASAP if we have already a PING or INFO already pending, or
     * in the case the instance is not properly connected.
     * 如果我们已经有一个PING或INFO已经挂起，或者在实例没有正确连接的情况下，请尽快返回
     * 如果在前面的 sentinelReconnectInstance 确实建立了连接，因为是异步的，到这个地方可能确实并未成功连接。
     * 此时直接返回，防止阻塞serverCron函数
     * */
    if (ri->link->disconnected) return;

    /* For INFO, PING, PUBLISH that are not critical commands to send we
     * also have a limit of SENTINEL_MAX_PENDING_COMMANDS. We don't
     * want to use a lot of memory just because a link is not working
     * properly (note that anyway there is a redundant protection about this,
     * that is, the link will be disconnected and reconnected if a long
     * timeout condition is detected.
     *
     * 对于INFO, PING, PUBLISH这些不重要的命令，我们也有一个SENTINEL_MAX_PENDING_COMMANDS的限制。
     * 我们不希望仅仅因为链接不能正常工作就使用大量内存(请注意，无论如何都有冗余保护，即如果检测到长超时条件，链接将被断开并重新连接)。
     * 等待的命令大于 100，就不再发送命令
     * */
    if (ri->link->pending_commands >=
        SENTINEL_MAX_PENDING_COMMANDS * ri->link->refcount) return;

    /* If this is a slave of a master in O_DOWN condition we start sending
     * it INFO every second, instead of the usual SENTINEL_INFO_PERIOD
     * period. In this state we want to closely monitor slaves in case they
     * are turned into masters by another Sentinel, or by the sysadmin.
     *
     * Similarly we monitor the INFO output more often if the slave reports
     * to be disconnected from the master, so that we can have a fresh
     * disconnection time figure.
     *
     * 如果这是处于O_DOWN状态的主服务器的从服务器，我们开始每秒向它发送INFO，而不是通常的SENTINEL_INFO_PERIOD周期。
     * 因为在这种状态下，我们要密切监视从服务器，以防它们被另一个哨兵或系统管理员变成主服务器。
     * 类似地，如果从属服务器报告与主服务器断开连接，我们将更频繁地监视INFO输出，这样我们就可以获得一个新的断开连接时间数字。
     * */
    if ((ri->flags & SRI_SLAVE) &&
        ((ri->master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS)) ||
         (ri->master_link_down_time != 0)))
    {
        // INFO消息频率，在从节点的异常状态下，频率会提高到1s一次
        info_period = 1000;
    } else {
        info_period = SENTINEL_INFO_PERIOD;
    }

    /* We ping instances every time the last received pong is older than
     * the configured 'down-after-milliseconds' time, but every second
     * anyway if 'down-after-milliseconds' is greater than 1 second.
     *
     * 如果“down-after-milliseconds”小于1s, 则按照 down-after-milliseconds 的值作为频率发送ping命令。
     * 但如果“down-after-milliseconds”大于1秒，则按照1s一次的频率发送ping命令
     * */
    ping_period = ri->down_after_period;
    if (ping_period > SENTINEL_PING_PERIOD) ping_period = SENTINEL_PING_PERIOD;

    /* Send INFO to masters and slaves, not sentinels. */
    // 【1】 给所有的主从节点发送INFO命令 ，并注册 sentinelInfoReplyCallback 为回调函数。
    // INFO命令的发送间隔默认为10s。如果接受命令的节点为从节点，并且处于下线或者故障转移状态，则提高发送频率为is
    // INFO的响应中包含主从节点地址，是否在线，主从同步状态，复制偏移量等信息。Sentinel节点会从INFO响应中获取最新的主从节点信息
    if ((ri->flags & SRI_SENTINEL) == 0 &&
        (ri->info_refresh == 0 ||
        (now - ri->info_refresh) > info_period))
    {
        // sentinelInfoReplyCallback 函数回调用 sentinelRefreshInstanceInfo 函数解析INFO响应数据，该函数如果发现了INFO响应中返回了
        // 不存在于slave字典中的从节点实例，则调用 createSentinelRedisInstance 函数创建一个新的从节点实例，并放入主节点的slaves字典中
        retval = redisAsyncCommand(ri->link->cc,
            sentinelInfoReplyCallback, ri, "%s",
            sentinelInstanceMapCommand(ri,"INFO"));
        if (retval == C_OK) ri->link->pending_commands++;
    }

    /* Send PING to all the three kinds of instances. */
    // 【2】 对所有节点发送ping命令， sentinel通过发送ping命令来检测其他节点的状态， ping命令的发送间隔默认为1s
    if ((now - ri->link->last_pong_time) > ping_period &&
               (now - ri->link->last_ping_time) > ping_period/2) {
        sentinelSendPing(ri);
    }

    /* PUBLISH hello messages to all the three kinds of instances. */
    // 【3】 对所有类型的节点发送hello消息， hello消息的发送间隔2s
    // 由于sentinel只订阅了主节点的__sentinel__:hello频道，所以其他sentinel并不会处理当前sentinel发送的hello消息
    if ((now - ri->last_pub_time) > SENTINEL_PUBLISH_PERIOD) {
        sentinelSendHello(ri);
    }
}

/* =========================== SENTINEL command ============================= */

/* SENTINEL CONFIG SET <option> */
void sentinelConfigSetCommand(client *c) {
    robj *o = c->argv[3];
    robj *val = c->argv[4];
    long long numval;
    int drop_conns = 0;

    if (!strcasecmp(o->ptr, "resolve-hostnames")) {
        if ((numval = yesnotoi(val->ptr)) == -1) goto badfmt;
        sentinel.resolve_hostnames = numval;
    } else if (!strcasecmp(o->ptr, "announce-hostnames")) {
        if ((numval = yesnotoi(val->ptr)) == -1) goto badfmt;
        sentinel.announce_hostnames = numval;
    } else if (!strcasecmp(o->ptr, "announce-ip")) {
        if (sentinel.announce_ip) sdsfree(sentinel.announce_ip);
        sentinel.announce_ip = sdsnew(val->ptr);
    } else if (!strcasecmp(o->ptr, "announce-port")) {
        if (getLongLongFromObject(val, &numval) == C_ERR ||
            numval < 0 || numval > 65535)
            goto badfmt;
        sentinel.announce_port = numval;
    } else if (!strcasecmp(o->ptr, "sentinel-user")) {
        sdsfree(sentinel.sentinel_auth_user);
        sentinel.sentinel_auth_user = sdslen(val->ptr) == 0 ?
            NULL : sdsdup(val->ptr);
        drop_conns = 1;
    } else if (!strcasecmp(o->ptr, "sentinel-pass")) {
        sdsfree(sentinel.sentinel_auth_pass);
        sentinel.sentinel_auth_pass = sdslen(val->ptr) == 0 ?
            NULL : sdsdup(val->ptr);
        drop_conns = 1;
    } else {
        addReplyErrorFormat(c, "Invalid argument '%s' to SENTINEL CONFIG SET",
                            (char *) o->ptr);
        return;
    }

    sentinelFlushConfig();
    addReply(c, shared.ok);

    /* Drop Sentinel connections to initiate a reconnect if needed. */
    if (drop_conns)
        sentinelDropConnections();

    return;

badfmt:
    addReplyErrorFormat(c, "Invalid value '%s' to SENTINEL CONFIG SET '%s'",
                        (char *) val->ptr, (char *) o->ptr);
}

/* SENTINEL CONFIG GET <option> */
void sentinelConfigGetCommand(client *c) {
    robj *o = c->argv[3];
    const char *pattern = o->ptr;
    void *replylen = addReplyDeferredLen(c);
    int matches = 0;

    if (stringmatch(pattern,"resolve-hostnames",1)) {
        addReplyBulkCString(c,"resolve-hostnames");
        addReplyBulkCString(c,sentinel.resolve_hostnames ? "yes" : "no");
        matches++;
    }

    if (stringmatch(pattern, "announce-hostnames", 1)) {
        addReplyBulkCString(c,"announce-hostnames");
        addReplyBulkCString(c,sentinel.announce_hostnames ? "yes" : "no");
        matches++;
    }

    if (stringmatch(pattern, "announce-ip", 1)) {
        addReplyBulkCString(c,"announce-ip");
        addReplyBulkCString(c,sentinel.announce_ip ? sentinel.announce_ip : "");
        matches++;
    }

    if (stringmatch(pattern, "announce-port", 1)) {
        addReplyBulkCString(c, "announce-port");
        addReplyBulkLongLong(c, sentinel.announce_port);
        matches++;
    }

    if (stringmatch(pattern, "sentinel-user", 1)) {
        addReplyBulkCString(c, "sentinel-user");
        addReplyBulkCString(c, sentinel.sentinel_auth_user ? sentinel.sentinel_auth_user : "");
        matches++;
    }

    if (stringmatch(pattern, "sentinel-pass", 1)) {
        addReplyBulkCString(c, "sentinel-pass");
        addReplyBulkCString(c, sentinel.sentinel_auth_pass ? sentinel.sentinel_auth_pass : "");
        matches++;
    }

    setDeferredMapLen(c, replylen, matches);
}

const char *sentinelFailoverStateStr(int state) {
    switch(state) {
    case SENTINEL_FAILOVER_STATE_NONE: return "none";
    case SENTINEL_FAILOVER_STATE_WAIT_START: return "wait_start";
    case SENTINEL_FAILOVER_STATE_SELECT_SLAVE: return "select_slave";
    case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE: return "send_slaveof_noone";
    case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION: return "wait_promotion";
    case SENTINEL_FAILOVER_STATE_RECONF_SLAVES: return "reconf_slaves";
    case SENTINEL_FAILOVER_STATE_UPDATE_CONFIG: return "update_config";
    default: return "unknown";
    }
}

/* Redis instance to Redis protocol representation. */
void addReplySentinelRedisInstance(client *c, sentinelRedisInstance *ri) {
    char *flags = sdsempty();
    void *mbl;
    int fields = 0;

    mbl = addReplyDeferredLen(c);

    addReplyBulkCString(c,"name");
    addReplyBulkCString(c,ri->name);
    fields++;

    addReplyBulkCString(c,"ip");
    addReplyBulkCString(c,announceSentinelAddr(ri->addr));
    fields++;

    addReplyBulkCString(c,"port");
    addReplyBulkLongLong(c,ri->addr->port);
    fields++;

    addReplyBulkCString(c,"runid");
    addReplyBulkCString(c,ri->runid ? ri->runid : "");
    fields++;

    addReplyBulkCString(c,"flags");
    if (ri->flags & SRI_S_DOWN) flags = sdscat(flags,"s_down,");
    if (ri->flags & SRI_O_DOWN) flags = sdscat(flags,"o_down,");
    if (ri->flags & SRI_MASTER) flags = sdscat(flags,"master,");
    if (ri->flags & SRI_SLAVE) flags = sdscat(flags,"slave,");
    if (ri->flags & SRI_SENTINEL) flags = sdscat(flags,"sentinel,");
    if (ri->link->disconnected) flags = sdscat(flags,"disconnected,");
    if (ri->flags & SRI_MASTER_DOWN) flags = sdscat(flags,"master_down,");
    if (ri->flags & SRI_FAILOVER_IN_PROGRESS)
        flags = sdscat(flags,"failover_in_progress,");
    if (ri->flags & SRI_PROMOTED) flags = sdscat(flags,"promoted,");
    if (ri->flags & SRI_RECONF_SENT) flags = sdscat(flags,"reconf_sent,");
    if (ri->flags & SRI_RECONF_INPROG) flags = sdscat(flags,"reconf_inprog,");
    if (ri->flags & SRI_RECONF_DONE) flags = sdscat(flags,"reconf_done,");
    if (ri->flags & SRI_FORCE_FAILOVER) flags = sdscat(flags,"force_failover,");
    if (ri->flags & SRI_SCRIPT_KILL_SENT) flags = sdscat(flags,"script_kill_sent,");

    if (sdslen(flags) != 0) sdsrange(flags,0,-2); /* remove last "," */
    addReplyBulkCString(c,flags);
    sdsfree(flags);
    fields++;

    addReplyBulkCString(c,"link-pending-commands");
    addReplyBulkLongLong(c,ri->link->pending_commands);
    fields++;

    addReplyBulkCString(c,"link-refcount");
    addReplyBulkLongLong(c,ri->link->refcount);
    fields++;

    if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
        addReplyBulkCString(c,"failover-state");
        addReplyBulkCString(c,(char*)sentinelFailoverStateStr(ri->failover_state));
        fields++;
    }

    addReplyBulkCString(c,"last-ping-sent");
    addReplyBulkLongLong(c,
        ri->link->act_ping_time ? (mstime() - ri->link->act_ping_time) : 0);
    fields++;

    addReplyBulkCString(c,"last-ok-ping-reply");
    addReplyBulkLongLong(c,mstime() - ri->link->last_avail_time);
    fields++;

    addReplyBulkCString(c,"last-ping-reply");
    addReplyBulkLongLong(c,mstime() - ri->link->last_pong_time);
    fields++;

    if (ri->flags & SRI_S_DOWN) {
        addReplyBulkCString(c,"s-down-time");
        addReplyBulkLongLong(c,mstime()-ri->s_down_since_time);
        fields++;
    }

    if (ri->flags & SRI_O_DOWN) {
        addReplyBulkCString(c,"o-down-time");
        addReplyBulkLongLong(c,mstime()-ri->o_down_since_time);
        fields++;
    }

    addReplyBulkCString(c,"down-after-milliseconds");
    addReplyBulkLongLong(c,ri->down_after_period);
    fields++;

    /* Masters and Slaves */
    if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
        addReplyBulkCString(c,"info-refresh");
        addReplyBulkLongLong(c,
            ri->info_refresh ? (mstime() - ri->info_refresh) : 0);
        fields++;

        addReplyBulkCString(c,"role-reported");
        addReplyBulkCString(c, (ri->role_reported == SRI_MASTER) ? "master" :
                                                                   "slave");
        fields++;

        addReplyBulkCString(c,"role-reported-time");
        addReplyBulkLongLong(c,mstime() - ri->role_reported_time);
        fields++;
    }

    /* Only masters */
    if (ri->flags & SRI_MASTER) {
        addReplyBulkCString(c,"config-epoch");
        addReplyBulkLongLong(c,ri->config_epoch);
        fields++;

        addReplyBulkCString(c,"num-slaves");
        addReplyBulkLongLong(c,dictSize(ri->slaves));
        fields++;

        addReplyBulkCString(c,"num-other-sentinels");
        addReplyBulkLongLong(c,dictSize(ri->sentinels));
        fields++;

        addReplyBulkCString(c,"quorum");
        addReplyBulkLongLong(c,ri->quorum);
        fields++;

        addReplyBulkCString(c,"failover-timeout");
        addReplyBulkLongLong(c,ri->failover_timeout);
        fields++;

        addReplyBulkCString(c,"parallel-syncs");
        addReplyBulkLongLong(c,ri->parallel_syncs);
        fields++;

        if (ri->notification_script) {
            addReplyBulkCString(c,"notification-script");
            addReplyBulkCString(c,ri->notification_script);
            fields++;
        }

        if (ri->client_reconfig_script) {
            addReplyBulkCString(c,"client-reconfig-script");
            addReplyBulkCString(c,ri->client_reconfig_script);
            fields++;
        }
    }

    /* Only slaves */
    if (ri->flags & SRI_SLAVE) {
        addReplyBulkCString(c,"master-link-down-time");
        addReplyBulkLongLong(c,ri->master_link_down_time);
        fields++;

        addReplyBulkCString(c,"master-link-status");
        addReplyBulkCString(c,
            (ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP) ?
            "ok" : "err");
        fields++;

        addReplyBulkCString(c,"master-host");
        addReplyBulkCString(c,
            ri->slave_master_host ? ri->slave_master_host : "?");
        fields++;

        addReplyBulkCString(c,"master-port");
        addReplyBulkLongLong(c,ri->slave_master_port);
        fields++;

        addReplyBulkCString(c,"slave-priority");
        addReplyBulkLongLong(c,ri->slave_priority);
        fields++;

        addReplyBulkCString(c,"slave-repl-offset");
        addReplyBulkLongLong(c,ri->slave_repl_offset);
        fields++;

        addReplyBulkCString(c,"replica-announced");
        addReplyBulkLongLong(c,ri->replica_announced);
        fields++;
    }

    /* Only sentinels */
    if (ri->flags & SRI_SENTINEL) {
        addReplyBulkCString(c,"last-hello-message");
        addReplyBulkLongLong(c,mstime() - ri->last_hello_time);
        fields++;

        addReplyBulkCString(c,"voted-leader");
        addReplyBulkCString(c,ri->leader ? ri->leader : "?");
        fields++;

        addReplyBulkCString(c,"voted-leader-epoch");
        addReplyBulkLongLong(c,ri->leader_epoch);
        fields++;
    }

    setDeferredMapLen(c,mbl,fields);
}

/* Output a number of instances contained inside a dictionary as
 * Redis protocol. */
void addReplyDictOfRedisInstances(client *c, dict *instances) {
    dictIterator *di;
    dictEntry *de;
    long slaves = 0;
    void *replylen = addReplyDeferredLen(c);

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        /* don't announce unannounced replicas */
        if (ri->flags & SRI_SLAVE && !ri->replica_announced) continue;
        addReplySentinelRedisInstance(c,ri);
        slaves++;
    }
    dictReleaseIterator(di);
    setDeferredArrayLen(c, replylen, slaves);
}

/* Lookup the named master into sentinel.masters.
 * If the master is not found reply to the client with an error and returns
 * NULL. */
sentinelRedisInstance *sentinelGetMasterByNameOrReplyError(client *c,
                        robj *name)
{
    sentinelRedisInstance *ri;

    ri = dictFetchValue(sentinel.masters,name->ptr);
    if (!ri) {
        addReplyError(c,"No such master with that name");
        return NULL;
    }
    return ri;
}

#define SENTINEL_ISQR_OK 0
#define SENTINEL_ISQR_NOQUORUM (1<<0)
#define SENTINEL_ISQR_NOAUTH (1<<1)
int sentinelIsQuorumReachable(sentinelRedisInstance *master, int *usableptr) {
    dictIterator *di;
    dictEntry *de;
    int usable = 1; /* Number of usable Sentinels. Init to 1 to count myself. */
    int result = SENTINEL_ISQR_OK;
    int voters = dictSize(master->sentinels)+1; /* Known Sentinels + myself. */

    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->flags & (SRI_S_DOWN|SRI_O_DOWN)) continue;
        usable++;
    }
    dictReleaseIterator(di);

    if (usable < (int)master->quorum) result |= SENTINEL_ISQR_NOQUORUM;
    if (usable < voters/2+1) result |= SENTINEL_ISQR_NOAUTH;
    if (usableptr) *usableptr = usable;
    return result;
}

void sentinelCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"CKQUORUM <master-name>",
"    Check if the current Sentinel configuration is able to reach the quorum",
"    needed to failover a master and the majority needed to authorize the",
"    failover.",
"CONFIG SET <param> <value>",
"    Set a global Sentinel configuration parameter.",
"CONFIG GET <param>",
"    Get global Sentinel configuration parameter.",
"GET-MASTER-ADDR-BY-NAME <master-name>",
"    Return the ip and port number of the master with that name.",
"FAILOVER <master-name>",
"    Manually failover a master node without asking for agreement from other",
"    Sentinels",
"FLUSHCONFIG",
"    Force Sentinel to rewrite its configuration on disk, including the current",
"    Sentinel state.",
"INFO-CACHE <master-name>",
"    Return last cached INFO output from masters and all its replicas.",
"IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>",
"    Check if the master specified by ip:port is down from current Sentinel's",
"    point of view.",
"MASTER <master-name>",
"    Show the state and info of the specified master.",
"MASTERS",
"    Show a list of monitored masters and their state.",
"MONITOR <name> <ip> <port> <quorum>",
"    Start monitoring a new master with the specified name, ip, port and quorum.",
"MYID",
"    Return the ID of the Sentinel instance.",
"PENDING-SCRIPTS",
"    Get pending scripts information.",
"REMOVE <master-name>",
"    Remove master from Sentinel's monitor list.",
"REPLICAS <master-name>",
"    Show a list of replicas for this master and their state.",
"RESET <pattern>",
"    Reset masters for specific master name matching this pattern.",
"SENTINELS <master-name>",
"    Show a list of Sentinel instances for this master and their state.",
"SET <master-name> <option> <value>",
"    Set configuration paramters for certain masters.",
"SIMULATE-FAILURE (CRASH-AFTER-ELECTION|CRASH-AFTER-PROMOTION|HELP)",
"    Simulate a Sentinel crash.",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"masters")) {
        /* SENTINEL MASTERS */
        if (c->argc != 2) goto numargserr;
        addReplyDictOfRedisInstances(c,sentinel.masters);
    } else if (!strcasecmp(c->argv[1]->ptr,"master")) {
        /* SENTINEL MASTER <name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
            == NULL) return;
        addReplySentinelRedisInstance(c,ri);
    } else if (!strcasecmp(c->argv[1]->ptr,"slaves") ||
               !strcasecmp(c->argv[1]->ptr,"replicas"))
    {
        /* SENTINEL REPLICAS <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c,ri->slaves);
    } else if (!strcasecmp(c->argv[1]->ptr,"sentinels")) {
        /* SENTINEL SENTINELS <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c,ri->sentinels);
    } else if (!strcasecmp(c->argv[1]->ptr,"myid") && c->argc == 2) {
        /* SENTINEL MYID */
        addReplyBulkCBuffer(c,sentinel.myid,CONFIG_RUN_ID_SIZE);
    } else if (!strcasecmp(c->argv[1]->ptr,"is-master-down-by-addr")) {
        /* SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>
         *
         * Arguments:
         *
         * ip and port are the ip and port of the master we want to be
         * checked by Sentinel. Note that the command will not check by
         * name but just by master, in theory different Sentinels may monitor
         * different masters with the same name.
         *
         * current-epoch is needed in order to understand if we are allowed
         * to vote for a failover leader or not. Each Sentinel can vote just
         * one time per epoch.
         *
         * runid is "*" if we are not seeking for a vote from the Sentinel
         * in order to elect the failover leader. Otherwise it is set to the
         * runid we want the Sentinel to vote if it did not already voted.
         */
        sentinelRedisInstance *ri;
        long long req_epoch;
        uint64_t leader_epoch = 0;
        char *leader = NULL;
        long port;
        int isdown = 0;

        if (c->argc != 6) goto numargserr;
        // SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>
        if (getLongFromObjectOrReply(c,c->argv[3],&port,NULL) != C_OK ||
            getLongLongFromObjectOrReply(c,c->argv[4],&req_epoch,NULL)
                                                              != C_OK)
            return;
        // 使用sentinel is-master-down-by-addr命令中的ip和端口获取指定的主节点实例                                           <-[1]
        ri = getSentinelRedisInstanceByAddrAndRunID(sentinel.masters,
            c->argv[2]->ptr,port,NULL);

        /* It exists? Is actually a master? Is subjectively down? It's down.
         * Note: if we are in tilt mode we always reply with "0".
         * 如果当前sentinel正处于titl模式,那么sentinel认为当前节点的故障判定已经不可信了;                                      <-[2]
         * 此时对于其他sentinel节点发来的 is-master-down-by-addr 命令总是回复 0 在线
         *
         * 判断目标节点在当前sentinel视角下是否为主观下线状态
         * */
        if (!sentinel.tilt && ri && (ri->flags & SRI_S_DOWN) &&
                                    (ri->flags & SRI_MASTER))
            isdown = 1;

        /* Vote for the master (or fetch the previous vote) if the request
         * includes a runid, otherwise the sender is not seeking for a vote. */
        // 如果发送节点发送的是投票请求, 则调用sentinelVoteLeader函数尝试给发送节点投票                                        <-[3]
        if (ri && ri->flags & SRI_MASTER && strcasecmp(c->argv[5]->ptr,"*")) {
            leader = sentinelVoteLeader(ri,(uint64_t)req_epoch,
                                            c->argv[5]->ptr,
                                            &leader_epoch);
        }

        // [4] 返回结果给发送节点,返回内容为: 1.当前节点是否为主观下线状态 2.当前节点的投票给谁了 3.当前节点的投票的配置纪元
        /* Reply with a three-elements multi-bulk reply:
         * down state, leader, vote epoch. */
        addReplyArrayLen(c,3);
        addReply(c, isdown ? shared.cone : shared.czero);
        addReplyBulkCString(c, leader ? leader : "*");
        addReplyLongLong(c, (long long)leader_epoch);
        if (leader) sdsfree(leader);
    } else if (!strcasecmp(c->argv[1]->ptr,"reset")) {
        /* SENTINEL RESET <pattern> */
        if (c->argc != 3) goto numargserr;
        addReplyLongLong(c,sentinelResetMastersByPattern(c->argv[2]->ptr,SENTINEL_GENERATE_EVENT));
    } else if (!strcasecmp(c->argv[1]->ptr,"get-master-addr-by-name")) {
        /* SENTINEL GET-MASTER-ADDR-BY-NAME <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        ri = sentinelGetMasterByName(c->argv[2]->ptr);
        if (ri == NULL) {
            addReplyNullArray(c);
        } else {
            sentinelAddr *addr = sentinelGetCurrentMasterAddress(ri);

            addReplyArrayLen(c,2);
            addReplyBulkCString(c,announceSentinelAddr(addr));
            addReplyBulkLongLong(c,addr->port);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"failover")) {
        /* SENTINEL FAILOVER <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        // 已经有进行中的故障转移就退出
        if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
            addReplyError(c,"-INPROG Failover already in progress");
            return;
        }
        // 看看有没有合适的从节点
        if (sentinelSelectSlave(ri) == NULL) {
            addReplyError(c,"-NOGOODSLAVE No suitable replica to promote");
            return;
        }
        serverLog(LL_WARNING,"Executing user requested FAILOVER of '%s'",
            ri->name);
        // sentinel failover命令中的启动failover                                                                          <-【1】
        // 只是设置一些状态，剩下的交给sentinel的定时任务机制
        sentinelStartFailover(ri);
        ri->flags |= SRI_FORCE_FAILOVER;
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"pending-scripts")) {
        /* SENTINEL PENDING-SCRIPTS */

        if (c->argc != 2) goto numargserr;
        sentinelPendingScriptsCommand(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"monitor")) {
        /* SENTINEL MONITOR <name> <ip> <port> <quorum> */
        sentinelRedisInstance *ri;
        long quorum, port;
        char ip[NET_IP_STR_LEN];

        if (c->argc != 6) goto numargserr;
        if (getLongFromObjectOrReply(c,c->argv[5],&quorum,"Invalid quorum")
            != C_OK) return;
        if (getLongFromObjectOrReply(c,c->argv[4],&port,"Invalid port")
            != C_OK) return;

        if (quorum <= 0) {
            addReplyError(c, "Quorum must be 1 or greater.");
            return;
        }

        /* If resolve-hostnames is used, actual DNS resolution may take place.
         * Otherwise just validate address.
         */
        if (anetResolve(NULL,c->argv[3]->ptr,ip,sizeof(ip),
                        sentinel.resolve_hostnames ? ANET_NONE : ANET_IP_ONLY) == ANET_ERR) {
            addReplyError(c, "Invalid IP address or hostname specified");
            return;
        }

        /* Parameters are valid. Try to create the master instance. */
        ri = createSentinelRedisInstance(c->argv[2]->ptr,SRI_MASTER,
                c->argv[3]->ptr,port,quorum,NULL);
        if (ri == NULL) {
            addReplyError(c,sentinelCheckCreateInstanceErrors(SRI_MASTER));
        } else {
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING,"+monitor",ri,"%@ quorum %d",ri->quorum);
            addReply(c,shared.ok);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"flushconfig")) {
        if (c->argc != 2) goto numargserr;
        sentinelFlushConfig();
        addReply(c,shared.ok);
        return;
    } else if (!strcasecmp(c->argv[1]->ptr,"remove")) {
        /* SENTINEL REMOVE <name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
            == NULL) return;
        sentinelEvent(LL_WARNING,"-monitor",ri,"%@");
        dictDelete(sentinel.masters,c->argv[2]->ptr);
        sentinelFlushConfig();
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"ckquorum")) {
        /* SENTINEL CKQUORUM <name> */
        sentinelRedisInstance *ri;
        int usable;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
            == NULL) return;
        int result = sentinelIsQuorumReachable(ri,&usable);
        if (result == SENTINEL_ISQR_OK) {
            addReplySds(c, sdscatfmt(sdsempty(),
                "+OK %i usable Sentinels. Quorum and failover authorization "
                "can be reached\r\n",usable));
        } else {
            sds e = sdscatfmt(sdsempty(),
                "-NOQUORUM %i usable Sentinels. ",usable);
            if (result & SENTINEL_ISQR_NOQUORUM)
                e = sdscat(e,"Not enough available Sentinels to reach the"
                             " specified quorum for this master");
            if (result & SENTINEL_ISQR_NOAUTH) {
                if (result & SENTINEL_ISQR_NOQUORUM) e = sdscat(e,". ");
                e = sdscat(e, "Not enough available Sentinels to reach the"
                              " majority and authorize a failover");
            }
            addReplyErrorSds(c,e);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"set")) {
        if (c->argc < 3) goto numargserr;
        sentinelSetCommand(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"config")) {
        if (c->argc < 3) goto numargserr;
        if (!strcasecmp(c->argv[2]->ptr,"set") && c->argc == 5)
            sentinelConfigSetCommand(c);
        else if (!strcasecmp(c->argv[2]->ptr,"get") && c->argc == 4)
            sentinelConfigGetCommand(c);
        else
            addReplyError(c, "Only SENTINEL CONFIG GET <option> / SET <option> <value> are supported.");
    } else if (!strcasecmp(c->argv[1]->ptr,"info-cache")) {
        /* SENTINEL INFO-CACHE <name> */
        if (c->argc < 2) goto numargserr;
        mstime_t now = mstime();

        /* Create an ad-hoc dictionary type so that we can iterate
         * a dictionary composed of just the master groups the user
         * requested. */
        dictType copy_keeper = instancesDictType;
        copy_keeper.valDestructor = NULL;
        dict *masters_local = sentinel.masters;
        if (c->argc > 2) {
            masters_local = dictCreate(&copy_keeper, NULL);

            for (int i = 2; i < c->argc; i++) {
                sentinelRedisInstance *ri;
                ri = sentinelGetMasterByName(c->argv[i]->ptr);
                if (!ri) continue; /* ignore non-existing names */
                dictAdd(masters_local, ri->name, ri);
            }
        }

        /* Reply format:
         *   1.) master name
         *   2.) 1.) info from master
         *       2.) info from replica
         *       ...
         *   3.) other master name
         *   ...
         */
        addReplyArrayLen(c,dictSize(masters_local) * 2);

        dictIterator  *di;
        dictEntry *de;
        di = dictGetIterator(masters_local);
        while ((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            addReplyBulkCBuffer(c,ri->name,strlen(ri->name));
            addReplyArrayLen(c,dictSize(ri->slaves) + 1); /* +1 for self */
            addReplyArrayLen(c,2);
            addReplyLongLong(c,
                ri->info_refresh ? (now - ri->info_refresh) : 0);
            if (ri->info)
                addReplyBulkCBuffer(c,ri->info,sdslen(ri->info));
            else
                addReplyNull(c);

            dictIterator *sdi;
            dictEntry *sde;
            sdi = dictGetIterator(ri->slaves);
            while ((sde = dictNext(sdi)) != NULL) {
                sentinelRedisInstance *sri = dictGetVal(sde);
                addReplyArrayLen(c,2);
                addReplyLongLong(c,
                    ri->info_refresh ? (now - sri->info_refresh) : 0);
                if (sri->info)
                    addReplyBulkCBuffer(c,sri->info,sdslen(sri->info));
                else
                    addReplyNull(c);
            }
            dictReleaseIterator(sdi);
        }
        dictReleaseIterator(di);
        if (masters_local != sentinel.masters) dictRelease(masters_local);
    } else if (!strcasecmp(c->argv[1]->ptr,"simulate-failure")) {
        /* SENTINEL SIMULATE-FAILURE <flag> <flag> ... <flag> */
        int j;

        sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
        for (j = 2; j < c->argc; j++) {
            if (!strcasecmp(c->argv[j]->ptr,"crash-after-election")) {
                sentinel.simfailure_flags |=
                    SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION;
                serverLog(LL_WARNING,"Failure simulation: this Sentinel "
                    "will crash after being successfully elected as failover "
                    "leader");
            } else if (!strcasecmp(c->argv[j]->ptr,"crash-after-promotion")) {
                sentinel.simfailure_flags |=
                    SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION;
                serverLog(LL_WARNING,"Failure simulation: this Sentinel "
                    "will crash after promoting the selected replica to master");
            } else if (!strcasecmp(c->argv[j]->ptr,"help")) {
                addReplyArrayLen(c,2);
                addReplyBulkCString(c,"crash-after-election");
                addReplyBulkCString(c,"crash-after-promotion");
            } else {
                addReplyError(c,"Unknown failure simulation specified");
                return;
            }
        }
        addReply(c,shared.ok);
    } else {
        addReplySubcommandSyntaxError(c);
    }
    return;

numargserr:
    addReplyErrorFormat(c,"Wrong number of arguments for 'sentinel %s'",
                          (char*)c->argv[1]->ptr);
}

#define info_section_from_redis(section_name) do { \
    if (defsections || allsections || !strcasecmp(section,section_name)) { \
        sds redissection; \
        if (sections++) info = sdscat(info,"\r\n"); \
        redissection = genRedisInfoString(section_name); \
        info = sdscatlen(info,redissection,sdslen(redissection)); \
        sdsfree(redissection); \
    } \
} while(0)

/* SENTINEL INFO [section] */
void sentinelInfoCommand(client *c) {
    if (c->argc > 2) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    int defsections = 0, allsections = 0;
    char *section = c->argc == 2 ? c->argv[1]->ptr : NULL;
    if (section) {
        allsections = !strcasecmp(section,"all");
        defsections = !strcasecmp(section,"default");
    } else {
        defsections = 1;
    }

    int sections = 0;
    sds info = sdsempty();

    info_section_from_redis("server");
    info_section_from_redis("clients");
    info_section_from_redis("cpu");
    info_section_from_redis("stats");

    if (defsections || allsections || !strcasecmp(section,"sentinel")) {
        dictIterator *di;
        dictEntry *de;
        int master_id = 0;

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Sentinel\r\n"
            "sentinel_masters:%lu\r\n"
            "sentinel_tilt:%d\r\n"
            "sentinel_running_scripts:%d\r\n"
            "sentinel_scripts_queue_length:%ld\r\n"
            "sentinel_simulate_failure_flags:%lu\r\n",
            dictSize(sentinel.masters),
            sentinel.tilt,
            sentinel.running_scripts,
            listLength(sentinel.scripts_queue),
            sentinel.simfailure_flags);

        di = dictGetIterator(sentinel.masters);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            char *status = "ok";

            if (ri->flags & SRI_O_DOWN) status = "odown";
            else if (ri->flags & SRI_S_DOWN) status = "sdown";
            info = sdscatprintf(info,
                "master%d:name=%s,status=%s,address=%s:%d,"
                "slaves=%lu,sentinels=%lu\r\n",
                master_id++, ri->name, status,
                announceSentinelAddr(ri->addr), ri->addr->port,
                dictSize(ri->slaves),
                dictSize(ri->sentinels)+1);
        }
        dictReleaseIterator(di);
    }

    addReplyBulkSds(c, info);
}

/* Implements Sentinel version of the ROLE command. The output is
 * "sentinel" and the list of currently monitored master names. */
void sentinelRoleCommand(client *c) {
    dictIterator *di;
    dictEntry *de;

    addReplyArrayLen(c,2);
    addReplyBulkCBuffer(c,"sentinel",8);
    addReplyArrayLen(c,dictSize(sentinel.masters));

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        addReplyBulkCString(c,ri->name);
    }
    dictReleaseIterator(di);
}

/* SENTINEL SET <mastername> [<option> <value> ...] */
void sentinelSetCommand(client *c) {
    sentinelRedisInstance *ri;
    int j, changes = 0;
    int badarg = 0; /* Bad argument position for error reporting. */
    char *option;

    if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
        == NULL) return;

    /* Process option - value pairs. */
    for (j = 3; j < c->argc; j++) {
        int moreargs = (c->argc-1) - j;
        option = c->argv[j]->ptr;
        long long ll;
        int old_j = j; /* Used to know what to log as an event. */

        if (!strcasecmp(option,"down-after-milliseconds") && moreargs > 0) {
            /* down-after-millisecodns <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->down_after_period = ll;
            sentinelPropagateDownAfterPeriod(ri);
            changes++;
        } else if (!strcasecmp(option,"failover-timeout") && moreargs > 0) {
            /* failover-timeout <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->failover_timeout = ll;
            changes++;
        } else if (!strcasecmp(option,"parallel-syncs") && moreargs > 0) {
            /* parallel-syncs <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->parallel_syncs = ll;
            changes++;
        } else if (!strcasecmp(option,"notification-script") && moreargs > 0) {
            /* notification-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig) {
                addReplyError(c,
                    "Reconfiguration of scripts path is denied for "
                    "security reasons. Check the deny-scripts-reconfig "
                    "configuration directive in your Sentinel configuration");
                goto seterr;
            }

            if (strlen(value) && access(value,X_OK) == -1) {
                addReplyError(c,
                    "Notification script seems non existing or non executable");
                goto seterr;
            }
            sdsfree(ri->notification_script);
            ri->notification_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"client-reconfig-script") && moreargs > 0) {
            /* client-reconfig-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig) {
                addReplyError(c,
                    "Reconfiguration of scripts path is denied for "
                    "security reasons. Check the deny-scripts-reconfig "
                    "configuration directive in your Sentinel configuration");
                goto seterr;
            }

            if (strlen(value) && access(value,X_OK) == -1) {
                addReplyError(c,
                    "Client reconfiguration script seems non existing or "
                    "non executable");
                goto seterr;
            }
            sdsfree(ri->client_reconfig_script);
            ri->client_reconfig_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"auth-pass") && moreargs > 0) {
            /* auth-pass <password> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_pass);
            ri->auth_pass = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"auth-user") && moreargs > 0) {
            /* auth-user <username> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_user);
            ri->auth_user = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"quorum") && moreargs > 0) {
            /* quorum <count> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->quorum = ll;
            changes++;
        } else if (!strcasecmp(option,"rename-command") && moreargs > 1) {
            /* rename-command <oldname> <newname> */
            sds oldname = c->argv[++j]->ptr;
            sds newname = c->argv[++j]->ptr;

            if ((sdslen(oldname) == 0) || (sdslen(newname) == 0)) {
                badarg = sdslen(newname) ? j-1 : j;
                goto badfmt;
            }

            /* Remove any older renaming for this command. */
            dictDelete(ri->renamed_commands,oldname);

            /* If the target name is the same as the source name there
             * is no need to add an entry mapping to itself. */
            if (!dictSdsKeyCaseCompare(NULL,oldname,newname)) {
                oldname = sdsdup(oldname);
                newname = sdsdup(newname);
                dictAdd(ri->renamed_commands,oldname,newname);
            }
            changes++;
        } else {
            addReplyErrorFormat(c,"Unknown option or number of arguments for "
                                  "SENTINEL SET '%s'", option);
            goto seterr;
        }

        /* Log the event. */
        int numargs = j-old_j+1;
        switch(numargs) {
        case 2:
            sentinelEvent(LL_WARNING,"+set",ri,"%@ %s %s",(char*)c->argv[old_j]->ptr,
                                                          (char*)c->argv[old_j+1]->ptr);
            break;
        case 3:
            sentinelEvent(LL_WARNING,"+set",ri,"%@ %s %s %s",(char*)c->argv[old_j]->ptr,
                                                             (char*)c->argv[old_j+1]->ptr,
                                                             (char*)c->argv[old_j+2]->ptr);
            break;
        default:
            sentinelEvent(LL_WARNING,"+set",ri,"%@ %s",(char*)c->argv[old_j]->ptr);
            break;
        }
    }

    if (changes) sentinelFlushConfig();
    addReply(c,shared.ok);
    return;

badfmt: /* Bad format errors */
    addReplyErrorFormat(c,"Invalid argument '%s' for SENTINEL SET '%s'",
        (char*)c->argv[badarg]->ptr,option);
seterr:
    if (changes) sentinelFlushConfig();
    return;
}

/* Our fake PUBLISH command: it is actually useful only to receive hello messages
 * from the other sentinel instances, and publishing to a channel other than
 * SENTINEL_HELLO_CHANNEL is forbidden.
 *
 * Because we have a Sentinel PUBLISH, the code to send hello messages is the same
 * for all the three kind of instances: masters, slaves, sentinels. */
void sentinelPublishCommand(client *c) {
    if (strcmp(c->argv[1]->ptr,SENTINEL_HELLO_CHANNEL)) {
        addReplyError(c, "Only HELLO messages are accepted by Sentinel instances.");
        return;
    }
    sentinelProcessHelloMessage(c->argv[2]->ptr,sdslen(c->argv[2]->ptr));
    addReplyLongLong(c,1);
}

/* ===================== SENTINEL availability checks ======================= */

/* Is this instance down from our point of view? */
void sentinelCheckSubjectivelyDown(sentinelRedisInstance *ri) {
    mstime_t elapsed = 0;

    // 计算目标节点上次响应后过去的时间                                                                                     <-[1]
    if (ri->link->act_ping_time)
        // 当收到响应时, ri->link->act_ping_time会重置为0, 这里不为0, 说明是已发ping, 但是没有收到pong回复消息
        // 此时获取上次发ping时间到现在的时间差
        elapsed = mstime() - ri->link->act_ping_time;
    else if (ri->link->disconnected)
        // 如果上次ping命令已经收到响应了,但是当前连接已断开,则取目标实例上次收到ping响应的时间last_avail_time到现在的时间差
        elapsed = mstime() - ri->link->last_avail_time;

    /* Check if we are in need for a reconnection of one of the
     * links, because we are detecting low activity.
     * 检查我们是否需要重新连接其中一个链接，因为我们检测到低活动。
     *
     * 1) Check if the command link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have a
     *    pending ping for more than half the timeout.
     *    检查命令链接是否已连接，是否连接不少于SENTINEL_MIN_LINK_RECONNECT_PERIOD，但我们仍然有超过一半的超时等待ping。
     *    */
    if (ri->link->cc &&
        (mstime() - ri->link->cc_conn_time) >
        SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        ri->link->act_ping_time != 0 && /* There is a pending ping... */
        /* The pending ping is delayed, and we did not receive
         * error replies as well. */
        (mstime() - ri->link->act_ping_time) > (ri->down_after_period/2) &&
        (mstime() - ri->link->last_pong_time) > (ri->down_after_period/2))
    {
        instanceLinkCloseConnection(ri->link,ri->link->cc);
    }

    /* 2) Check if the pubsub link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have no
     *    activity in the Pub/Sub channel for more than
     *    SENTINEL_PUBLISH_PERIOD * 3.
     */
    if (ri->link->pc &&
        (mstime() - ri->link->pc_conn_time) >
         SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        (mstime() - ri->link->pc_last_activity) > (SENTINEL_PUBLISH_PERIOD*3))
    {
        instanceLinkCloseConnection(ri->link,ri->link->pc);
    }

    /* Update the SDOWN flag. We believe the instance is SDOWN if:
     * 更新SDOWN标志。我们认为实例是SDOWN，如果
     *
     * 1) It is not replying.
     * 2) We believe it is a master, it reports to be a slave for enough time
     *    to meet the down_after_period, plus enough time to get two times
     *    INFO report from the instance. */
    // 满足以下条件之一则判定目标节点主观下线 :                                                                              <-[2]
    //      1. 如果上次收到ping响应到现在的时间超过了down_after_period属性指定的时间,则认为主观下线
    //      2. 当前sentinel节点认为目标节点是主节点, 但它在INFO响应中报告自己是从节点,而且目标节点已经很长时间没有报告自己的角色了,则认为主观下线
    if (elapsed > ri->down_after_period ||
        (ri->flags & SRI_MASTER &&
         ri->role_reported == SRI_SLAVE &&
         mstime() - ri->role_reported_time >
          (ri->down_after_period+SENTINEL_INFO_PERIOD*2)))
    {
        /* Is subjectively down */
        if ((ri->flags & SRI_S_DOWN) == 0) {
            sentinelEvent(LL_WARNING,"+sdown",ri,"%@");
            ri->s_down_since_time = mstime();
            // 给当前实例添加 SRI_S_DOWN 标志
            // 实际操作是将ri->flags的指定二进制位置为1, 使用改位来表示其是否主观下线的状态.
            // 这里通过执行ri->flags |= SRI_S_DOWN语句，我们将flags字段中的SRI_S_DOWN位设置为1，表示该redisInstance节点已经处于“DOWN”状态。
            // 其他位的值则保持不变，因为该语句使用了按位或运算符(|=)，只会对指定位进行修改，而不影响其他位的值
            ri->flags |= SRI_S_DOWN;
        }
    } else {
        /* Is subjectively up */
        // 如果条件都不满足则清除 SRI_S_DOWN和SRI_SCRIPT_KILL_SENT标识
        if (ri->flags & SRI_S_DOWN) {
            sentinelEvent(LL_WARNING,"-sdown",ri,"%@");
            // 在这个例子中，我们将flags字段中的SRI_S_DOWN位和SRI_SCRIPT_KILL_SENT位都设置为0，即清除这两个标识位。这样就可以将该redisInstance节点从DOWN状态和脚本kill发送状态中恢复过来，使其能够正常工作。
            //需要注意的是，该语句使用了按位与运算符(&=)和按位取反运算符(~)，它们结合起来的作用是清除指定位上的标识。具体来说，
            // ~(SRI_S_DOWN|SRI_SCRIPT_KILL_SENT)语句首先对SRI_S_DOWN位和SRI_SCRIPT_KILL_SENT位取反，
            // 然后再使用按位与运算符将flags字段中的这两个标识位清除为0，而其他位则不变。
            ri->flags &= ~(SRI_S_DOWN|SRI_SCRIPT_KILL_SENT);
        }
    }
}

/* Is this instance down according to the configured quorum?
 *
 * Note that ODOWN is a weak quorum, it only means that enough Sentinels
 * reported in a given time range that the instance was not reachable.
 * However messages can be delayed so there are no strong guarantees about
 * N instances agreeing at the same time about the down state. */
void sentinelCheckObjectivelyDown(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    unsigned int quorum = 0, odown = 0;

    if (master->flags & SRI_S_DOWN) {
        /* Is down for enough sentinels? */
        // 如果目标节点已主观下线(对于当前sentinel来说), 则遍历其sentinels字典, 统计集群中其他判断目标主节点下线的Sentinel节点的数量, <-[1]
        // 如果这个数量大于等于配置文件中的quorum属性值, 则认为目标节点已经客观下线
        quorum = 1; /* the current sentinel. 由于当前sentinel已判定其主观下线,所以quorum从1开始*/
        /* Count all the other sentinels. */
        di = dictGetIterator(master->sentinels);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);

            if (ri->flags & SRI_MASTER_DOWN) quorum++;
        }
        dictReleaseIterator(di);
        if (quorum >= master->quorum) odown = 1;
    }

    /* Set the flag accordingly to the outcome. */
    // [2] 根据判定结果, 给目标节点添加或清除SRI_O_DOWN标记
    if (odown) {
        if ((master->flags & SRI_O_DOWN) == 0) {
            sentinelEvent(LL_WARNING,"+odown",master,"%@ #quorum %d/%d",
                quorum, master->quorum);
            master->flags |= SRI_O_DOWN;
            master->o_down_since_time = mstime();
        }
    } else {
        if (master->flags & SRI_O_DOWN) {
            sentinelEvent(LL_WARNING,"-odown",master,"%@");
            master->flags &= ~SRI_O_DOWN;
        }
    }
}

/* Receive the SENTINEL is-master-down-by-addr reply, see the
 * sentinelAskMasterStateToOtherSentinels() function for more information. */
void sentinelReceiveIsMasterDownReply(redisAsyncContext *c, void *reply, void *privdata) {
    // privdata是sentinelAskMasterStateToOtherSentinels函数中给redisAsyncCommand回调函数设置的附加参数, 即sentinel is-master-down-by-addr命令的接收节点sentinel实例
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    /**
     * 返回结果包含三个参数，如下所示：
     *     down_state：目标Sentinel节点对于主节点的下线判断，1是下线，0是在线。
     *     leader_runid：当leader_runid等于“*”时，代表返回结果是用来做主节点是否不可达，当leader_runid等于具体的runid，代表目标节点同意runid成为领导者。
     *     leader_epoch：领导者纪元。
     */

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    /* Ignore every error or unexpected reply.
     * Note that if the command returns an error for any reason we'll
     * end clearing the SRI_MASTER_DOWN flag for timeout anyway. */
    if (r->type == REDIS_REPLY_ARRAY && r->elements == 3 &&
        r->element[0]->type == REDIS_REPLY_INTEGER &&
        r->element[1]->type == REDIS_REPLY_STRING &&
        r->element[2]->type == REDIS_REPLY_INTEGER)
    {
        ri->last_master_down_reply_time = mstime();
        // 如果返回的down_state标识为1,则说明目标sentinel节点也认为目标主节点已下线, 这时候在目标sentinel实例的flags中添加SRI_MASTER_DOWN标识, 以标识该sentinel节点也认为目标主节点已下线
        if (r->element[0]->integer == 1) { // down_state
            // 设置目标sentinel节点对于当前主节点的下线判定                                                                  <-[1]
            ri->flags |= SRI_MASTER_DOWN;
        } else {
            ri->flags &= ~SRI_MASTER_DOWN;
        }
        if (strcmp(r->element[1]->str,"*")) { // leader_runid
            /* If the runid in the reply is not "*" the Sentinel actually
             * replied with a vote. */
            // 这里如果返回的runid不是一个*号,说明目标sentinel节点投票给当前节点同意当前节点成为领导者                           <-[2]
            sdsfree(ri->leader);
            if ((long long)ri->leader_epoch != r->element[2]->integer) // leader_epoch
                serverLog(LL_WARNING,
                    "%s voted for %s %llu", ri->name,
                    r->element[1]->str,
                    (unsigned long long) r->element[2]->integer);
            // 将sentinel字典中目标sentinel节点的leader属性设置为当前节点的runid, 表示该节点同意当前节点成为领导者               <-[3]
            ri->leader = sdsnew(r->element[1]->str);
            ri->leader_epoch = r->element[2]->integer;
        }
    }
}

/* If we think the master is down, we start sending
 * SENTINEL IS-MASTER-DOWN-BY-ADDR requests to other sentinels
 * in order to get the replies that allow to reach the quorum
 * needed to mark the master in ODOWN state and trigger a failover. */
#define SENTINEL_ASK_FORCED (1<<0)
void sentinelAskMasterStateToOtherSentinels(sentinelRedisInstance *master, int flags) {
    dictIterator *di;
    dictEntry *de;

    /**
     * 命令格式: sentinel is-master-down-by-addr <ip> <port> <current_epoch> <runid>
     *      current_epoch：当前配置纪元。
     *      runid：此参数有两种类型，不同类型决定了此API作用的不同。 当runid等于“*”时，作用是Sentinel节点直接交换对主节点下线的判定。 当runid等于当前Sentinel节点的runid时，作用是当前Sentinel节点希望目标Sentinel节点同意自己成为领导者的请求，
     * eg: sentinel is-master-down-by-addr 127.0.0.1 6379 0 691b795360f0a5957ffa3537bc27d2327ed87e49
     * 返回结果包含三个参数，如下所示：
     *      down_state：目标Sentinel节点对于主节点的下线判断，1是下线，0是在线。
     *      leader_runid：当leader_runid等于“*”时，代表返回结果是用来做主节点是否不可达，当leader_runid等于具体的runid，代表目标节点同意runid成为领导者。
     *      leader_epoch：领导者纪元。
     */
    // 遍历主节点实例的sentinel字典[1]
    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        // 目标节点上次回复 SENTINEL is-master-down 命令的时间
        mstime_t elapsed = mstime() - ri->last_master_down_reply_time;
        char port[32];
        int retval;

        /* If the master state from other sentinel is too old, we clear it. */
        // 如果目标sentinel已经超过5s没有回复过针对当前主节点的 SENTINEL is-master-down 命令, 那么就清除目标sentinel实例上的 SRI_MASTER_DOWN 标识
        if (elapsed > SENTINEL_ASK_PERIOD*5) {
            ri->flags &= ~SRI_MASTER_DOWN;
            sdsfree(ri->leader);
            ri->leader = NULL;
        }

        /* Only ask if master is down to other sentinels if:
         *
         * 1) We believe it is down, or there is a failover in progress.
         * 2) Sentinel is connected.
         * 3) We did not receive the info within SENTINEL_ASK_PERIOD ms.
         * 只有在以下情况同时满足才向其他sentinel节点询问master是否已关闭:                                                    <-[2]
         *      1)我们认为它已关闭，或者正在进行故障转移。
         *      2)哨兵已连接。
         *      3)我们没有在SENTINEL_ASK_PERIOD ms内收到信息。
         * */
        if ((master->flags & SRI_S_DOWN) == 0) continue; // 没有宕机不发
        if (ri->link->disconnected) continue; // 和目标sentinel断开连接不发
        if (!(flags & SENTINEL_ASK_FORCED) &&
            mstime() - ri->last_master_down_reply_time < SENTINEL_ASK_PERIOD)
            continue;

        /* Ask */
        ll2string(port,sizeof(port),master->addr->port);
        // 发送  sentinel is-master-down-by-addr 命令                                                                    <-[3]
        // 这里使用raft算法选举leader节点, 由于每个节点的主逻辑函数的运行时间间隙有差异,  所有通常会有一个节点先行发送投票请求, 并当选为leader节点
        retval = redisAsyncCommand(ri->link->cc,
                    sentinelReceiveIsMasterDownReply, ri,
                    "%s is-master-down-by-addr %s %s %llu %s",
                    sentinelInstanceMapCommand(ri,"SENTINEL"),
                    announceSentinelAddr(master->addr), port,
                    sentinel.current_epoch,
                    (master->failover_state > SENTINEL_FAILOVER_STATE_NONE) ?
                    sentinel.myid : "*"); // 注意这里, 在进入failover状态时, runid的值为sentinel.myid,否则为"*"
        if (retval == C_OK) ri->link->pending_commands++;
    }
    dictReleaseIterator(di);
}

/* =============================== FAILOVER ================================= */

/* Crash because of user request via SENTINEL simulate-failure command. */
void sentinelSimFailureCrash(void) {
    serverLog(LL_WARNING,
        "Sentinel CRASH because of SENTINEL simulate-failure");
    exit(99);
}

/* Vote for the sentinel with 'req_runid' or return the old vote if already
 * voted for the specified 'req_epoch' or one greater.
 *
 * If a vote is not available returns NULL, otherwise return the Sentinel
 * runid and populate the leader_epoch with the epoch of the vote.
 *
 * 为带有'req_runid'的哨兵投票，如果已经为指定的'req_epoch'或更大的一个投票，则返回旧的投票。
 * 如果投票不可用，则返回NULL，否则返回Sentinel runid并使用投票的epoch填充leader_epoch。
 *
 * 这个函数用于判断是否可以给目标节点投票
 * */
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch) {
    // master: 当前sentinel视角下的目标实例
    // req_epoch,req_runid: Sentinel is-master-down-by-addr 命令中指定的配置纪元和myid, 即发送该命令的节点的配置纪元和myid
    // leader_epoch: 用于记录获得当前节点最新投票的节点的纪元

    // [1] 如果发送节点的配置纪元比接收节点的配置纪元大,那么接收节点更新自己的配置纪元,并将配置纪元写入配置文件
    if (req_epoch > sentinel.current_epoch) {
        sentinel.current_epoch = req_epoch;
        sentinelFlushConfig();
        sentinelEvent(LL_WARNING,"+new-epoch",master,"%llu",
            (unsigned long long) sentinel.current_epoch);
    }

    // [2] 如果发送节点的配置纪元比当前sentinel节点记录的目标master节点的领导者纪元大, 说明发送节点的指令具有更高的优先级
    // 此时可以给发送节点投票,具体执行如下操作:
    //      1. 更新目标实例属性中的leader和leader_epoch属性, 将leader和leader_epoch写入配置文件
    //      2. 更新目标实例属性failover_start_time,防止接收节点重复发起故障转移
    //      3. 设置leader_epoch变量,返回leader节点的myid
    if (master->leader_epoch < req_epoch && sentinel.current_epoch <= req_epoch)
    {
        sdsfree(master->leader);
        master->leader = sdsnew(req_runid);
        master->leader_epoch = sentinel.current_epoch;
        sentinelFlushConfig();
        sentinelEvent(LL_WARNING,"+vote-for-leader",master,"%s %llu",
            master->leader, (unsigned long long) master->leader_epoch);
        /* If we did not voted for ourselves, set the master failover start
         * time to now, in order to force a delay before we can start a
         * failover for the same master. */
        if (strcasecmp(master->leader,sentinel.myid))
            master->failover_start_time = mstime()+rand()%SENTINEL_MAX_DESYNC;
    }

    *leader_epoch = master->leader_epoch;
    return master->leader ? sdsnew(master->leader) : NULL;
}

struct sentinelLeader {
    char *runid;
    unsigned long votes;
};

/* Helper function for sentinelGetLeader, increment the counter
 * relative to the specified runid. */
int sentinelLeaderIncr(dict *counters, char *runid) {
    dictEntry *existing, *de;
    uint64_t oldval;

    de = dictAddRaw(counters,runid,&existing);
    if (existing) {
        oldval = dictGetUnsignedIntegerVal(existing);
        dictSetUnsignedIntegerVal(existing,oldval+1);
        return oldval+1;
    } else {
        serverAssert(de != NULL);
        dictSetUnsignedIntegerVal(de,1);
        return 1;
    }
}

/* Scan all the Sentinels attached to this master to check if there
 * is a leader for the specified epoch.
 *
 * To be a leader for a given epoch, we should have the majority of
 * the Sentinels we know (ever seen since the last SENTINEL RESET) that
 * reported the same instance as leader for the same epoch.
 * 扫描所有连接到这个master的哨兵, 检查指定的纪元是否存在一个leader.
 * 要成为给定纪元的领导者，我们应该让我们知道的大多数哨兵(自上次哨兵重置以来)报告相同纪元的相同sentinel实例作为领导者。
 * */
char *sentinelGetLeader(sentinelRedisInstance *master, uint64_t epoch) {
    // epoch: 本次故障转移的配置纪元, 这个值主要用来辅助统计投票结果，因为只有在 failover_epoch 所属的纪元内，其投票才是有效的
    dict *counters;
    dictIterator *di;
    dictEntry *de;
    unsigned int voters = 0, voters_quorum;
    char *myvote;
    char *winner = NULL;
    uint64_t leader_epoch;
    uint64_t max_votes = 0;

    serverAssert(master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS));
    counters = dictCreate(&leaderVotesDictType,NULL);

    // 总票数，包括自己在内
    voters = dictSize(master->sentinels)+1; /* All the other sentinels and me.*/

    /* Count other sentinels votes */
    // 每一个sentinel实例的leader、leader_epoch属性都存放了获得该sentinel节点最新投票的节点的myidh和纪元，使用counters字典统计每个Sentinel <-[1]
    // 节点当前纪元获得票数， 并将counters字典获得最多票数的节点存放到winner变量中。
    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        if (ri->leader != NULL && ri->leader_epoch == sentinel.current_epoch)
            sentinelLeaderIncr(counters,ri->leader);
    }
    dictReleaseIterator(di);

    /* Check what's the winner. For the winner to win, it needs two conditions:
     * 1) Absolute majority between voters (50% + 1).
     * 2) And anyway at least master->quorum votes.
     * 看看谁是赢家。对于获胜者来说，它需要两个条件:
     *      1)投票者之间的绝对多数(50% + 1)。
     *      2)无论如何投票数至少达到master->quorum。
     * */
    di = dictGetIterator(counters);
    while((de = dictNext(di)) != NULL) {
        uint64_t votes = dictGetUnsignedIntegerVal(de);

        if (votes > max_votes) {
            max_votes = votes;
            winner = dictGetKey(de);
        }
    }
    dictReleaseIterator(di);

    /* Count this Sentinel vote:
     * if this Sentinel did not voted yet, either vote for the most
     * common voted sentinel, or for itself if no vote exists at all.
     * 计算这个哨兵的投票:
     * 如果这个哨兵还没有投票，要么投票给最常见的哨兵，要么投票给自己 。
     *
     * 如果当前节点现在未投票， 则投票给获票最多的节点。如果没有选出最多的节点， 则投给自己.                                      <-[2]
     * */
    if (winner)
        // 如果发现了得票数最多的节点， 就把自己的票投给它
        myvote = sentinelVoteLeader(master,epoch,winner,&leader_epoch);
    else
        // 否则的化，投票给自己
        myvote = sentinelVoteLeader(master,epoch,sentinel.myid,&leader_epoch);

    if (myvote && leader_epoch == epoch) {
        uint64_t votes = sentinelLeaderIncr(counters,myvote);

        if (votes > max_votes) {
            max_votes = votes;
            winner = myvote;
        }
    }

    // 获票最多的节点其获票数如果不少于集群Sentinel节点数量的一半， 并且不小于目标实例属性quorum的值， 那么这个节点就是leader节点    <-[3]
    voters_quorum = voters/2+1;
    if (winner && (max_votes < voters_quorum || max_votes < master->quorum))
        winner = NULL;

    winner = winner ? sdsnew(winner) : NULL;
    sdsfree(myvote);
    dictRelease(counters);
    return winner;
}

/* Send SLAVEOF to the specified instance, always followed by a
 * CONFIG REWRITE command in order to store the new configuration on disk
 * when possible (that is, if the Redis instance is recent enough to support
 * config rewriting, and if the server was started with a configuration file).
 *
 * If Host is NULL the function sends "SLAVEOF NO ONE".
 *
 * The command returns C_OK if the SLAVEOF command was accepted for
 * (later) delivery otherwise C_ERR. The command replies are just
 * discarded. */
int sentinelSendSlaveOf(sentinelRedisInstance *ri, const sentinelAddr *addr) {
    // ri：注意，slaveof 命令只能发给从节点，所以这里的这个ri是晋升为主节点的slave实例                                          <-[1]
    char portstr[32];
    const char *host;
    int retval;

    /* If host is NULL we send SLAVEOF NO ONE that will turn the instance
    * into a master. */
    if (!addr) {
        host = "NO";
        memcpy(portstr,"ONE",4);
    } else {
        host = announceSentinelAddr(addr);
        ll2string(portstr,sizeof(portstr),addr->port);
    }

    /* In order to send SLAVEOF in a safe way, we send a transaction performing
     * the following tasks:
     * 1) Reconfigure the instance according to the specified host/port params.
     * 2) Rewrite the configuration.
     * 3) Disconnect all clients (but this one sending the command) in order
     *    to trigger the ask-master-on-reconnection protocol for connected
     *    clients.
     *
     * Note that we don't check the replies returned by commands, since we
     * will observe instead the effects in the next INFO output.
     *
     * 为了以安全的方式发送SLAVEOF，我们发送一个事务，执行以下任务:                                                            <-[2]
     *      1)根据指定的主机端口参数重新配置实例。
     *      2)执行Rewrite重写配置。
     *      3)断开所有客户端(除了这个发送命令的sentinel客户端)，以便为已连接的客户端触发 ask-master-on-reconnection 协议。
     *
     * 注意，我们不检查命令返回的应答，因为我们将在下一个INFO输出中观察其效果。
     * */
    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s",
        sentinelInstanceMapCommand(ri,"MULTI"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s %s %s",
        sentinelInstanceMapCommand(ri,"SLAVEOF"),
        host, portstr);
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s REWRITE",
        sentinelInstanceMapCommand(ri,"CONFIG"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    /* CLIENT KILL TYPE <type> is only supported starting from Redis 2.8.12,
     * however sending it to an instance not understanding this command is not
     * an issue because CLIENT is variadic command, so Redis will not
     * recognized as a syntax error, and the transaction will not fail (but
     * only the unsupported command will fail).
     * CLIENT KILL TYPE <type>只支持从Redis 2.8.12开始，但是发送它到一个不理解这个命令的实例不是问题，因为CLIENT是可变命令，所以Redis不会识别为语法错误，事务不会失败(但只有不支持的命令会失败)。
     * CLIENT KILL TYPE normal 表示杀死所有普通客户端， CLIENT KILL TYPE pubsub 表示杀死所有订阅客户端
     * */
    for (int type = 0; type < 2; type++) {
        retval = redisAsyncCommand(ri->link->cc,
            sentinelDiscardReplyCallback, ri, "%s KILL TYPE %s",
            sentinelInstanceMapCommand(ri,"CLIENT"),
            type == 0 ? "normal" : "pubsub");
        if (retval == C_ERR) return retval;
        ri->link->pending_commands++;
    }

    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s",
        sentinelInstanceMapCommand(ri,"EXEC"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    return C_OK;
}

/* Setup the master state to start a failover. */
void sentinelStartFailover(sentinelRedisInstance *master) {
    serverAssert(master->flags & SRI_MASTER);

    // 代表已经开始对该主节点进行故障转移                                                                                   <-[1]
    master->failover_state = SENTINEL_FAILOVER_STATE_WAIT_START;
    master->flags |= SRI_FAILOVER_IN_PROGRESS;
    // 当前配置纪元的值+1, 并将其复制给failover_epoch, 代表当前纪元正在进行故障转移                                            <-[2]
    master->failover_epoch = ++sentinel.current_epoch;
    sentinelEvent(LL_WARNING,"+new-epoch",master,"%llu",
        (unsigned long long) sentinel.current_epoch);
    sentinelEvent(LL_WARNING,"+try-failover",master,"%@");
    // 注意,这里更新了目标实例的failover_start_time, 在当前Sentinel开始故障转移或者投票给其他Sentinel时, 都会更新该值            <-[3]
    master->failover_start_time = mstime()+rand()%SENTINEL_MAX_DESYNC;
    master->failover_state_change_time = mstime();
}

/* This function checks if there are the conditions to start the failover,
 * that is:
 *
 * 1) Master must be in ODOWN condition.
 * 2) No failover already in progress.
 * 3) No failover already attempted recently.
 *
 * We still don't know if we'll win the election so it is possible that we
 * start the failover but that we'll not be able to act.
 *
 * Return non-zero if a failover was started. */
int sentinelStartFailoverIfNeeded(sentinelRedisInstance *master) {
    /* We can't failover if the master is not in O_DOWN state. */
    // 非客观下线的主节点不进行故障转移                                                                                     <-[1]
    if (!(master->flags & SRI_O_DOWN)) return 0;

    /* Failover already in progress? */
    // 已经在进行古装转移的,不再进行故障转移                                                                                 <-[2]
    if (master->flags & SRI_FAILOVER_IN_PROGRESS) return 0;

    /* Last failover attempt started too little time ago? */
    // 两次故障转移操作时间的时间间隔必须大于 failover_timeout*2, 即6分钟                                                     <-[3]
    if (mstime() - master->failover_start_time <
        master->failover_timeout*2)
    {
        // failover_delay_logged 故障转移延迟, 即下次什么时候爱是进行故障转移
        if (master->failover_delay_logged != master->failover_start_time) {
            // 计算下次故障转移的时间
            time_t clock = (master->failover_start_time +
                            master->failover_timeout*2) / 1000;
            char ctimebuf[26];

            ctime_r(&clock,ctimebuf);
            ctimebuf[24] = '\0'; /* Remove newline. */
            master->failover_delay_logged = master->failover_start_time;
            serverLog(LL_WARNING,
                "Next failover delay: I will not start a failover before %s",
                ctimebuf);
        }
        return 0;
    }

    // 启动故障转移. 看起来只是设置了一些故障转移前的状态.                                                                    <-[4]
    sentinelStartFailover(master);
    return 1;
}

/* Select a suitable slave to promote. The current algorithm only uses
 * the following parameters:
 *
 * 1) None of the following conditions: S_DOWN, O_DOWN, DISCONNECTED.
 * 2) Last time the slave replied to ping no more than 5 times the PING period.
 * 3) info_refresh not older than 3 times the INFO refresh period.
 * 4) master_link_down_time no more than:
 *     (now - master->s_down_since_time) + (master->down_after_period * 10).
 *    Basically since the master is down from our POV, the slave reports
 *    to be disconnected no more than 10 times the configured down-after-period.
 *    This is pretty much black magic but the idea is, the master was not
 *    available so the slave may be lagging, but not over a certain time.
 *    Anyway we'll select the best slave according to replication offset.
 * 5) Slave priority can't be zero, otherwise the slave is discarded.
 *
 * Among all the slaves matching the above conditions we select the slave
 * with, in order of sorting key:
 *
 * - lower slave_priority.
 * - bigger processed replication offset.
 * - lexicographically smaller runid.
 *
 * Basically if runid is the same, the slave that processed more commands
 * from the master is selected.
 *
 * The function returns the pointer to the selected slave, otherwise
 * NULL if no suitable slave was found.
 */

/* Helper for sentinelSelectSlave(). This is used by qsort() in order to
 * sort suitable slaves in a "better first" order, to take the first of
 * the list. */
int compareSlavesForPromotion(const void *a, const void *b) {
    sentinelRedisInstance **sa = (sentinelRedisInstance **)a,
                          **sb = (sentinelRedisInstance **)b;
    char *sa_runid, *sb_runid;

    if ((*sa)->slave_priority != (*sb)->slave_priority)
        return (*sa)->slave_priority - (*sb)->slave_priority;

    /* If priority is the same, select the slave with greater replication
     * offset (processed more data from the master). */
    if ((*sa)->slave_repl_offset > (*sb)->slave_repl_offset) {
        return -1; /* a < b */
    } else if ((*sa)->slave_repl_offset < (*sb)->slave_repl_offset) {
        return 1; /* a > b */
    }

    /* If the replication offset is the same select the slave with that has
     * the lexicographically smaller runid. Note that we try to handle runid
     * == NULL as there are old Redis versions that don't publish runid in
     * INFO. A NULL runid is considered bigger than any other runid. */
    sa_runid = (*sa)->runid;
    sb_runid = (*sb)->runid;
    if (sa_runid == NULL && sb_runid == NULL) return 0;
    else if (sa_runid == NULL) return 1;  /* a > b */
    else if (sb_runid == NULL) return -1; /* a < b */
    return strcasecmp(sa_runid, sb_runid);
}

sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master) {
    // 该数组用于存储具备晋升资格的从节点列表
    sentinelRedisInstance **instance =
        zmalloc(sizeof(instance[0])*dictSize(master->slaves));
    sentinelRedisInstance *selected = NULL;
    int instances = 0;
    dictIterator *di;
    dictEntry *de;
    // max_master_down_time用于存储最大宕机时间，如果从节点的宕机时间超过了该时间，则当前从节点不能作为故障转移的主节点
    mstime_t max_master_down_time = 0;

    if (master->flags & SRI_S_DOWN)
        // s_down_since_time 主观宕机时间
        max_master_down_time += mstime() - master->s_down_since_time;
    // down_after_period用于存储sentinel down-after-milliseconds 配置，用于判定节点是否已经主观下线
    max_master_down_time += master->down_after_period * 10;

    di = dictGetIterator(master->slaves);

    // 过滤不健康的节点
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);
        mstime_t info_validity_time;

        if (slave->flags & (SRI_S_DOWN|SRI_O_DOWN)) continue; // 过滤 “不健康”（主观下线、客观下线）的从节点
        if (slave->link->disconnected) continue; // 过滤 “不健康”（断线）的从节点
        if (mstime() - slave->link->last_avail_time > SENTINEL_PING_PERIOD*5) continue; // 5秒内没有回复过Sentinel节点ping响应的从节点
        if (slave->slave_priority == 0) continue; // 过滤掉优先级为0的从节点

        /* If the master is in SDOWN state we get INFO for slaves every second.
         * Otherwise we get it with the usual period so we need to account for
         * a larger delay.
         * 如果主服务器处于SDOWN状态，我们每秒都会获取从服务器的INFO。如果非SDOWN状态（可能是客观宕机），则是10s一次INFO， 则所以我们需要考虑更大的延迟。
         * */
        if (master->flags & SRI_S_DOWN)
            // 如果主节点是主观宕机， 则从节点的INFO有效期为5s，超过该时间的从节点将不具备故障转移晋升的资格
            info_validity_time = SENTINEL_PING_PERIOD*5;
        else
            // 如果主节点非主观宕机（到了这个环节，那也只能是odown了）， 则从节点的INFO有效期为30s，超过该时间的从节点将不具备故障转移晋升的资格
            info_validity_time = SENTINEL_INFO_PERIOD*3;
        if (mstime() - slave->info_refresh > info_validity_time) continue; // 上一次从改从节点收到INFO输出的时间超过5s
        if (slave->master_link_down_time > max_master_down_time) continue; // 与主节点失联超过 down-after-milliseconds * 10的时间
        instance[instances++] = slave;
    }
    dictReleaseIterator(di);
    if (instances) {
        // 如果有具备晋升资格的从节点，那么调用compareSlavesForPromotion函数进行排序：
        //      1、优先按照 slave_priority 排序，slave_priority最大的从节点将作为晋升节点
        //      2、slave_priority一样，则按照slave_repl_offset排序，slave_repl_offset最大的从节点将作为晋升节点
        //      3、slave_repl_offset也一样，则按照runid的字典序进行排序， runid最小的从节点将作为晋升节点
        qsort(instance,instances,sizeof(sentinelRedisInstance*),
            compareSlavesForPromotion);
        selected = instance[0];
    }
    zfree(instance);
    return selected;
}

/* ---------------- Failover state machine implementation ------------------- */
void sentinelFailoverWaitStart(sentinelRedisInstance *ri) {
    // ri： 要进行故障转移的master实例
    // 这个函数会统计当前sentinel的得票结果，如果得票超过半数，那么当前节点就会发起故障转移
    char *leader;
    int isleader;

    /* Check if we are the leader for the failover epoch. */
    // sentinelGetLeader函数会统计当前纪元的投票结果， 返回当选leader节点的myid属性。                                         <-[1]
    leader = sentinelGetLeader(ri, ri->failover_epoch);
    // 当前节点是否当选为leader节点
    isleader = leader && strcasecmp(leader,sentinel.myid) == 0;
    sdsfree(leader);

    /* If I'm not the leader, and it is not a forced failover via
     * SENTINEL FAILOVER, then I can't continue with the failover. */
    // 如果我不是领导者，并且它不是通过SENTINEL failover强制进行的故障转移，那么我就不能继续进行故障转移。                        <-[2]

    // 如果当前sentinel节点非leader节点，则说明当前sentinel节点没有赢得选举或选举流程还没完成（可能部分sentinel节点未返回投票响应）， 此时退出函数。
    if (!isleader && !(ri->flags & SRI_FORCE_FAILOVER)) {
        int election_timeout = SENTINEL_ELECTION_TIMEOUT;

        /* The election timeout is the MIN between SENTINEL_ELECTION_TIMEOUT
         * and the configured failover timeout. */
        // 选举超时时间（election_timeout）取目标实例属性failover_timeout与常量SENTINEL_ELECTION_TIMEOUT这两个值中的较小值。
        if (election_timeout > ri->failover_timeout)
            election_timeout = ri->failover_timeout;
        /* Abort the failover if I'm not the leader after some time. */
        // 如果故障转移开始后过去的时间超过选举超时时间（election_timeout），则本次选举可能发生了选票瓜分，这是需要中止当前故障转移
        // 注意，这里并没有更新目标实例属性failover_start_time， 所以下次开始故障转移时仍需要等待本次故障转移开始后过去时间超过目标实例属性failover_timeout*2的时间
        if (mstime() - ri->failover_start_time > election_timeout) {
            sentinelEvent(LL_WARNING,"-failover-abort-not-elected",ri,"%@");
            // 退出故障转移(主要是更新master的flags)
            sentinelAbortFailover(ri);
        }
        return;
    }

    // 这个消息表示sentinel之间已经选出来leader节点
    sentinelEvent(LL_WARNING,"+elected-leader",ri,"%@");
    if (sentinel.simfailure_flags & SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION)
        sentinelSimFailureCrash();
    // [3] 如果当前sentinel节点是leader节点， 则进入 SENTINEL_FAILOVER_STATE_SELECT_SLAVE 状态
    ri->failover_state = SENTINEL_FAILOVER_STATE_SELECT_SLAVE;
    ri->failover_state_change_time = mstime();
    sentinelEvent(LL_WARNING,"+failover-state-select-slave",ri,"%@");
}

// 这个函数用于选择晋升的从节点
void sentinelFailoverSelectSlave(sentinelRedisInstance *ri) {
    sentinelRedisInstance *slave = sentinelSelectSlave(ri);

    /* We don't handle the timeout in this state as the function aborts
     * the failover or go forward in the next state. */
    if (slave == NULL) {
        sentinelEvent(LL_WARNING,"-failover-abort-no-good-slave",ri,"%@");
        sentinelAbortFailover(ri);
    } else {
        sentinelEvent(LL_WARNING,"+selected-slave",slave,"%@");
        slave->flags |= SRI_PROMOTED;
        ri->promoted_slave = slave; // 将选择出来的从节点记录                                                              <-[1]
        ri->failover_state = SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE; // 流转到下一个阶段,将选择的从节点晋升为主节点
        ri->failover_state_change_time = mstime();
        sentinelEvent(LL_NOTICE,"+failover-state-send-slaveof-noone",
            slave, "%@");
    }
}
// 在SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE状态下， sentinelFailoverSendSlaveOfNoOne 函数会给选出来的从节点发送 slaveof no one 命令
// 将该从节点取消复制关系，晋升为主节点
void sentinelFailoverSendSlaveOfNoOne(sentinelRedisInstance *ri) {
    int retval;

    /* We can't send the command to the promoted slave if it is now
     * disconnected. Retry again and again with this state until the timeout
     * is reached, then abort the failover.
     * 如果已断开连接，则无法将命令发送到已提升的从属服务器。在此状态下反复重试，直到达到超时，然后中止故障转移。
     * */
    if (ri->promoted_slave->link->disconnected) {
        // 如果sentinel针对被晋升的从节点，在failover_timeout时间内，依旧未能成功发送slaveof no one命令， 则中止故障转移         <-[1]
        if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
            sentinelEvent(LL_WARNING,"-failover-abort-slave-timeout",ri,"%@");
            sentinelAbortFailover(ri);
        }
        return;
    }

    /* Send SLAVEOF NO ONE command to turn the slave into a master.
     * We actually register a generic callback for this command as we don't
     * really care about the reply. We check if it worked indirectly observing
     * if INFO returns a different role (master instead of slave).
     * 发送 SLAVEOF NO ONE 命令将slave变为master。我们实际上为这个命令注册了一个通用回调，因为我们并不真正关心回复。
     * 我们通过观察INFO是否返回不同的角色(master而不是slave)来间接检查它是否工作。
     * */
    retval = sentinelSendSlaveOf(ri->promoted_slave,NULL);
    if (retval != C_OK) return;
    sentinelEvent(LL_NOTICE, "+failover-state-wait-promotion",
        ri->promoted_slave,"%@");
    ri->failover_state = SENTINEL_FAILOVER_STATE_WAIT_PROMOTION; // 下一个阶段，等待从节点晋升成功
    ri->failover_state_change_time = mstime();
}

/* We actually wait for promotion indirectly checking with INFO when the
 * slave turns into a master.
 * 实际上，当从机变为主机时，我们会通过INFO间接地等待升级。
 * */
void sentinelFailoverWaitPromotion(sentinelRedisInstance *ri) {
    /* Just handle the timeout. Switching to the next state is handled
     * by the function parsing the INFO command of the promoted slave.
     * 只处理超时时间。切换到下一个状态是由解析提升的slave的INFO命令的函数处理的。
     * */
    if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
        sentinelEvent(LL_WARNING,"-failover-abort-slave-timeout",ri,"%@");
        sentinelAbortFailover(ri);
    }
}

/**
 * 检测故障转移是否完成。正常情况下，故障转移完成的标志是：
 *      1. 从节点晋升为主节点
 *      2. 排除断线的节点，所有旧的从节点都已经成功连接到新的主节点（即从节点的master_host为新的master，并且master_link_state为up）
 * 此时，sentinel就认为故障转移完成了。
 * 注意：
 *      这里的从节点是不包含被故障转移的主节点的！！！ 此时当前sentinel尚未更新自己的master和slave字典(要在进入 SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 状态之后才会更新，此时还是SENTINEL_FAILOVER_STATE_RECONF_SLAVES状态)
 *      所以即使旧的主节点完全不可达，但丝毫不影响故障转移结束
 *
 * 但是如果从变更为 SENTINEL_FAILOVER_STATE_RECONF_SLAVES 状态开始，经过failover_timeout长的时间之后，
 * 从节点依旧没有成功指向主节点（包括slaveof命令无法成功发送给从节点，或者从节点收到了，但无法和主节点连接等），则认为故障转移超时，此时也会强制结束故障转移
 * @param master
 */
void sentinelFailoverDetectEnd(sentinelRedisInstance *master) {
    int not_reconfigured = 0, timeout = 0;
    dictIterator *di;
    dictEntry *de;
    // 上一次主从切换状态变更到现在的时间。其实就是 SENTINEL_FAILOVER_STATE_RECONF_SLAVES 阶段开始到现在的时间
    mstime_t elapsed = mstime() - master->failover_state_change_time;

    /* We can't consider failover finished if the promoted slave is
     * not reachable.
     * 如果提升的从服务器不可达，我们不能认为故障转移已经完成。
     * */
    if (master->promoted_slave == NULL ||
        master->promoted_slave->flags & SRI_S_DOWN) return;

    /* The failover terminates once all the reachable slaves are properly
     * configured.
     * 一旦正确配置（slaveof）了所有可访问的从服务器，故障转移就会终止。
     *
     * 注意：这里的这个slave字典包含了原本所有的从节点以及晋升的从节点，但不包含宕机的主节点
     * */
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;
        if (slave->flags & SRI_S_DOWN) continue;
        not_reconfigured++;
    }
    dictReleaseIterator(di);

    /* Force end of failover on timeout.
     * 命令其余从节点复制新的主节点的执行时间超过了failover-timeout（不包含复制时间）， 则故障转移失败。
     * 注意即使超过了这个时间，Sentinel节点也会最终配置从节点去同步最新的主节点
     * */
    if (elapsed > master->failover_timeout) {
        not_reconfigured = 0;
        timeout = 1;
        sentinelEvent(LL_WARNING,"+failover-end-for-timeout",master,"%@");
    }

    // not_reconfigured意味着没有从节点需要发送slaveof指令了
    if (not_reconfigured == 0) {
        sentinelEvent(LL_WARNING,"+failover-end",master,"%@");
        master->failover_state = SENTINEL_FAILOVER_STATE_UPDATE_CONFIG;
        master->failover_state_change_time = mstime();
    }

    /* If I'm the leader it is a good idea to send a best effort SLAVEOF
     * command to all the slaves still not reconfigured to replicate with
     * the new master.
     * 如果我是leader，那么最好发送一个best effort SLAVEOF命令给所有仍然没有重新配置的slave，以便与新的master进行复制。
     * */
    if (timeout) {
        dictIterator *di;
        dictEntry *de;

        di = dictGetIterator(master->slaves);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *slave = dictGetVal(de);
            int retval;

            if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE|SRI_RECONF_SENT)) continue;
            if (slave->link->disconnected) continue;

            retval = sentinelSendSlaveOf(slave,master->promoted_slave->addr);
            if (retval == C_OK) {
                sentinelEvent(LL_NOTICE,"+slave-reconf-sent-be",slave,"%@");
                slave->flags |= SRI_RECONF_SENT;
            }
        }
        dictReleaseIterator(di);
    }
}

/* Send SLAVE OF <new master address> to all the remaining slaves that
 * still don't appear to have the configuration updated.
 *
 * 给所有发送slaveof 命令的剩余从服务器发送SLAVE OF <new master address>命令
 * */
void sentinelFailoverReconfNextSlave(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    // 正在建立复制关系的从节点数量
    int in_progress = 0;

    di = dictGetIterator(master->slaves);
    // 统计当前正在建立主从关系的节点数量                                                                                   <-【1】
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);
        // 已发送slaveof命令的从节点和正在建立主从关系的从节点数量
        if (slave->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG))
            in_progress++;
    }
    dictReleaseIterator(di);

    di = dictGetIterator(master->slaves);
    // 如果当前正在建立主从关系的从节点数量大于故障转移实例的parallel_syncs属性，则当前不允许再与新的从节点建立主从关系             <-【2】
    while(in_progress < master->parallel_syncs &&
          (de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *slave = dictGetVal(de);
        int retval;

        // 过滤晋升节点及已经建立主从关系的从节点                                                                            <-【3】
        /* Skip the promoted slave, and already configured slaves. */
        if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;

        /* If too much time elapsed without the slave moving forward to
         * the next state, consider it reconfigured even if it is not.
         * Sentinels will detect the slave as misconfigured and fix its
         * configuration later.
         *
         * 如果过了很长时间，从服务器还没有进入下一个状态，即使它没有重新配置，也要考虑重新配置它。哨兵将检测到slave配置错误，并稍后修复其配置。
         * */
        if ((slave->flags & SRI_RECONF_SENT) &&
            (mstime() - slave->slave_reconf_sent_time) >
            SENTINEL_SLAVE_RECONF_TIMEOUT)
        {
            sentinelEvent(LL_NOTICE,"-slave-reconf-sent-timeout",slave,"%@");
            slave->flags &= ~SRI_RECONF_SENT; // 去除该从节点的【已发送slaveof命令】状态
            slave->flags |= SRI_RECONF_DONE;  // 添加该从节点的【已完成slaveof命令】状态
        }

        /* Nothing to do for instances that are disconnected or already
         * in RECONF_SENT state.
         * 已经发送过slave of命令的从节点或者正在建立主从关系的从节点跳过
         * 已断线的从节点跳过                                                                                             <-【4】
         * */
        if (slave->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG)) continue;
        if (slave->link->disconnected) continue;

        /* Send SLAVEOF <new master>. */
        // 发送slaveof命令， 发送成功了才给从节点实例添加SRI_RECONF_SENT标记                                                  <-【5】
        retval = sentinelSendSlaveOf(slave,master->promoted_slave->addr);
        if (retval == C_OK) {
            slave->flags |= SRI_RECONF_SENT;
            slave->slave_reconf_sent_time = mstime();
            sentinelEvent(LL_NOTICE,"+slave-reconf-sent",slave,"%@");
            in_progress++;
        }
    }
    dictReleaseIterator(di);

    /* Check if all the slaves are reconfigured and handle timeout. */
    //                                                                                                                  <-【6】
    // 统计所有从节点的主从状态， 如果所有从节点（剔除下线的节点）都存在 SRI_RECONF_DONE 标记，则故障转移状态切换到 SENTINEL_FAILOVER_STATE_UPDATE_CONFIG
    sentinelFailoverDetectEnd(master);
}

/* This function is called when the slave is in
 * SENTINEL_FAILOVER_STATE_UPDATE_CONFIG state. In this state we need
 * to remove it from the master table and add the promoted slave instead.
 *
 * 当从机处于SENTINEL_FAILOVER_STATE_UPDATE_CONFIG状态时调用该函数。
 * 在这种状态下，我们需要从masters字典中中删除旧的master节点，并添加提升的从到masters中。
 * */
void sentinelFailoverSwitchToPromotedSlave(sentinelRedisInstance *master) {
    // master： 旧的master节点
    sentinelRedisInstance *ref = master->promoted_slave ?
                                 master->promoted_slave : master;

    sentinelEvent(LL_WARNING,"+switch-master",master,"%s %s %d %s %d",
        master->name, announceSentinelAddr(master->addr), master->addr->port,
        announceSentinelAddr(ref->addr), ref->addr->port);

    sentinelResetMasterAndChangeAddress(master,ref->addr->hostname,ref->addr->port);
}

void sentinelFailoverStateMachine(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_MASTER);

    if (!(ri->flags & SRI_FAILOVER_IN_PROGRESS)) return;

    switch(ri->failover_state) {
        case SENTINEL_FAILOVER_STATE_WAIT_START: // 统计投票选举结果
            sentinelFailoverWaitStart(ri);
            break;
        case SENTINEL_FAILOVER_STATE_SELECT_SLAVE: // 选择晋升的从节点
            sentinelFailoverSelectSlave(ri);
            break;
        case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE: // 将选择的从节点晋升为主节点
            sentinelFailoverSendSlaveOfNoOne(ri);
            break;
        case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION: // 等待晋升主节点完成
            sentinelFailoverWaitPromotion(ri);
            break;
        case SENTINEL_FAILOVER_STATE_RECONF_SLAVES: // 使其它从节点与晋升的从节点建立复制关系
            sentinelFailoverReconfNextSlave(ri);
            break;
    }
}

/* Abort a failover in progress:
 *
 * This function can only be called before the promoted slave acknowledged
 * the slave -> master switch. Otherwise the failover can't be aborted and
 * will reach its end (possibly by timeout). */
void sentinelAbortFailover(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_FAILOVER_IN_PROGRESS);
    serverAssert(ri->failover_state <= SENTINEL_FAILOVER_STATE_WAIT_PROMOTION);

    ri->flags &= ~(SRI_FAILOVER_IN_PROGRESS|SRI_FORCE_FAILOVER);
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = mstime();
    if (ri->promoted_slave) {
        ri->promoted_slave->flags &= ~SRI_PROMOTED;
        ri->promoted_slave = NULL;
    }
}

/* ======================== SENTINEL timer handler ==========================
 * This is the "main" our Sentinel, being sentinel completely non blocking
 * in design. The function is called every second.
 * 这是我们的哨兵的main函数，哨兵在设计上完全不阻塞。这个函数每秒被调用一次。
 * -------------------------------------------------------------------------- */

/* Perform scheduled operations for the specified Redis instance. */
void sentinelHandleRedisInstance(sentinelRedisInstance *ri) {
    // 这里的ri可能是master, slave, sentinel
    /* ========== MONITORING HALF ============ */
    /* Every kind of instance */
    // 【1】 sentinelReconnectInstance 函数负责建立网络连接
    sentinelReconnectInstance(ri);
    // sentinelSendPeriodicCommands 函数负责发送定时消息给redis的主从节点和其他sentinel节点
    // 1OS一次INFO
    // 2s一次PUBLISH __sentinel__:hello
    // 1s一次PING
    sentinelSendPeriodicCommands(ri);

    /* ============== ACTING HALF ============= */
    /* We don't proceed with the acting half if we are in TILT mode.
     * TILT happens when we find something odd with the time, like a
     * sudden change in the clock. */
    if (sentinel.tilt) {
        if (mstime()-sentinel.tilt_start_time < SENTINEL_TILT_PERIOD) return;
        sentinel.tilt = 0;
        sentinelEvent(LL_WARNING,"-tilt",NULL,"#tilt mode exited");
    }

    /* Every kind of instance */
    // 【2】 检查是否存在主观下线的节点
    sentinelCheckSubjectivelyDown(ri);

    /* Masters and slaves */
    if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
        /* Nothing so far. */
    }

    /* Only masters  3~6步只对主节点执行， 这是负责实现故障转移*/
    if (ri->flags & SRI_MASTER) {
        // 【3】 检查是否存在客观下线的节点
        sentinelCheckObjectivelyDown(ri);
        // 【4】 sentinelStartFailoverIfNeeded 用于判断是否可以进行故障转移，如果是,则进行故障转移
        if (sentinelStartFailoverIfNeeded(ri))
            // 如果返回是，则调用 sentinelAskMasterStateToOtherSentinels 函数发送投票请求。
            // 这个函数被调用了两次，本次调用只有在故障转移开始后才会进行。发送投票请求， 要求其他sentinel节点投票给当前节点
            // sentinel is-master-down-by-addr 127.0.0.1 6379 0 *
            sentinelAskMasterStateToOtherSentinels(ri,SENTINEL_ASK_FORCED);
        // 【5】 sentinelFailoverStateMachine 函数实现了一个故障转移状态机，实现故障转移逻辑
        sentinelFailoverStateMachine(ri);
        // 【6】调用 sentinelAskMasterStateToOtherSentinels 函数询问其他sentinel节点对该主节点的主观下线的判定结果
        // 这个函数被调用了两次，本次调用会在每次定时调用时触发，负责询问其他sentinel节点对指定主节点的主观下线意见
        sentinelAskMasterStateToOtherSentinels(ri,SENTINEL_NO_FLAGS);
    }
}

/* Perform scheduled operations for all the instances in the dictionary.
 * Recursively call the function against dictionaries of slaves.
 * 对字典中的所有实例执行计划操作。针对从服务器的字典递归调用该函数。
 * */
void sentinelHandleDictOfRedisInstances(dict *instances) {
    // instances 节点字典, 可以是sentinel.master字典, 或者某个sentinelRedisInstance实例的slave字典, sentinel节点字典
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *switch_to_promoted = NULL;

    /* There are a number of things we need to perform agai nst every master. */
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        // 【1】 调用主逻辑函数 sentinelHandleRedisInstance
        sentinelHandleRedisInstance(ri);
        // 【2】 如果当前处理的是主节点,那么还需要递归调用 sentinelHandleDictOfRedisInstances 函数处理该节点实例下的slaves字典和sentinel节点字典
        if (ri->flags & SRI_MASTER) {
            sentinelHandleDictOfRedisInstances(ri->slaves);
            sentinelHandleDictOfRedisInstances(ri->sentinels);
            if (ri->failover_state == SENTINEL_FAILOVER_STATE_UPDATE_CONFIG) {
                // 故障转移进入更新配置状态， 此时所有的从节点都已经指向了新的主节点，但旧的主节点还未指向新的主节点
                switch_to_promoted = ri;
            }
        }
    }
    // 【3】 完成故障转移的最后一步,
    if (switch_to_promoted)
        // 更新视图数据和配置文件
        sentinelFailoverSwitchToPromotedSlave(switch_to_promoted);
    dictReleaseIterator(di);
}

/* This function checks if we need to enter the TITL mode.
 *
 * The TILT mode is entered if we detect that between two invocations of the
 * timer interrupt, a negative amount of time, or too much time has passed.
 * Note that we expect that more or less just 100 milliseconds will pass
 * if everything is fine. However we'll see a negative number or a
 * difference bigger than SENTINEL_TILT_TRIGGER milliseconds if one of the
 * following conditions happen:
 *
 * 1) The Sentinel process for some time is blocked, for every kind of
 * random reason: the load is huge, the computer was frozen for some time
 * in I/O or alike, the process was stopped by a signal. Everything.
 * 2) The system clock was altered significantly.
 *
 * Under both this conditions we'll see everything as timed out and failing
 * without good reasons. Instead we enter the TILT mode and wait
 * for SENTINEL_TILT_PERIOD to elapse before starting to act again.
 *
 * During TILT time we still collect information, we just do not act. */
void sentinelCheckTiltCondition(void) {
    mstime_t now = mstime();
    mstime_t delta = now - sentinel.previous_time;

    if (delta < 0 || delta > SENTINEL_TILT_TRIGGER) {
        sentinel.tilt = 1;
        sentinel.tilt_start_time = mstime();
        sentinelEvent(LL_WARNING,"+tilt",NULL,"#tilt mode entered");
    }
    sentinel.previous_time = mstime();
}

void sentinelTimer(void) {
    // 【1】 检查是否需要进入titl模式.Redis Sentinel 严重依赖于计算机时间：例如，为了了解一个实例是否可用，Sentinel 会记住最近成功回复 PING 命令的时间，与当前时间对比来了解这有多久,以判定该节点是否主观下线。
    //      但是，如果计算机时间以不可预知的方式改变了，或者计算机非常繁忙，或者某些原因进程阻塞了，Sentinel 可能会开始表现得不可预知。
    sentinelCheckTiltCondition();
    // 【2】 调用sentinel机制的主逻辑触发函数
    sentinelHandleDictOfRedisInstances(sentinel.masters);
    // 【3】 定时执行Sentinel脚本
    sentinelRunPendingScripts();
    sentinelCollectTerminatedScripts();
    sentinelKillTimedoutScripts();

    /* We continuously change the frequency of the Redis "timer interrupt"
     * in order to desynchronize every Sentinel from every other.
     * This non-determinism avoids that Sentinels started at the same time
     * exactly continue to stay synchronized asking to be voted at the
     * same time again and again (resulting in nobody likely winning the
     * election because of split brain voting). */
    // 【4】 随机化sentinelTimer的下次执行时间,该操作是为了避免故障转移中使用Raft算法选举leader节点时有多个节点同时发送投票请求
    server.hz = CONFIG_DEFAULT_HZ + rand() % CONFIG_DEFAULT_HZ;
}
