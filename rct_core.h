#ifndef _RCT_CORE_H_
#define _RCT_CORE_H_

#include "config.h"

#ifdef HAVE_DEBUG_LOG
# define RCT_DEBUG_LOG 1
#endif

#ifdef HAVE_ASSERT_PANIC
# define RCT_ASSERT_PANIC 1
#endif

#ifdef HAVE_ASSERT_LOG
# define RCT_ASSERT_LOG 1
#endif

#ifdef HAVE_STATS
# define RCT_STATS 1
#else
# define RCT_STATS 0
#endif

#ifdef HAVE_LITTLE_ENDIAN
# define RCT_LITTLE_ENDIAN 1
#endif

#ifdef HAVE_BACKTRACE
# define RCT_HAVE_BACKTRACE 1
#endif

#define RCT_OK        0
#define RCT_ERROR    -1
#define RCT_EAGAIN   -2
#define RCT_ENOMEM   -3

/* reserved fds for std streams, log etc. */
#define RESERVED_FDS 32

typedef int r_status; /* return type */
typedef int err_t;     /* error type */

#define RCT_REDIS_ROLE_NULL     0
#define RCT_REDIS_ROLE_ALL      1
#define RCT_REDIS_ROLE_MASTER   2
#define RCT_REDIS_ROLE_SLAVE    3

#define RCT_REDIS_ROLE_NAME_NODE    "node"
#define RCT_REDIS_ROLE_NAME_MASTER  "master"
#define RCT_REDIS_ROLE_NAME_SLAVE   "slave"

#include <stdio.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <ctype.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <getopt.h>
#include <pthread.h>

#include <hircluster.h>
#include <hiarray.h>
#include <sds.h>
#include <dict.c>

#include "rct_util.h"
#include "rct_option.h"
#include "rct_log.h"
#include "rct_command.h"
#include "rct_mttlist.h"
#include "rct_locklist.h"

struct instance {
    int             log_level;                   /* log level */
    char            *log_filename;               /* log filename */
    char            *conf_filename;              /* configuration filename */
    int             interval;                    /* stats aggregation interval */
    char            *addr;                       /* stats monitoring addr */
    char            hostname[RCT_MAXHOSTNAMELEN]; /* hostname */
    pid_t           pid;                         /* process id */
    char            *pid_filename;               /* pid filename */
    unsigned        pidfile:1;                   /* pid file created? */
    
    int             show_help;
    int             show_version;
    int             daemonize;
    
    char            *command;
    char            *role;
    uint64_t        start;
    uint64_t        end;
    int             simple;
    int             thread_count;
    uint64_t        buffer_size;
};

typedef struct rctContext {
    redisClusterContext *cc;
    dict *commands;             /* Command table */
    char *address;
    char *cmd;
    uint8_t redis_role;
    uint8_t simple;
    int             thread_count;
    uint64_t        buffer_size;
    struct hiarray args;
}rctContext;

void nodes_get_state(rctContext *ctx, int type);
void slots_state(rctContext *ctx, int type);
void show_nodes_hold_slot_num(rctContext *ctx, int type);
void show_new_nodes_name(rctContext *ctx, int type);
void show_nodes_list(rctContext *ctx, int type);
void cluster_rebalance(rctContext *ctx, int type);
void do_command(rctContext *ctx, int type);
void do_command_node_by_node(rctContext *ctx, int type);
void cluster_del_keys(rctContext *ctx, int type);

#endif

