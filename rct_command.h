#ifndef _RCT_COMMAND_H_
#define _RCT_COMMAND_H_

#define RCT_CMD_CLUSTER_STATE			"cluster_state"
#define RCT_CMD_CLUSTER_CREATE		    "cluster_create"
#define RCT_CMD_CLUSTER_DESTROY		    "cluster_destroy"
#define RCT_CMD_CLUSTER_DEL_ALL_SLAVES	"cluster_delete_all_slaves"
#define RCT_CMD_CLUSTER_CHECK		    "cluster_check"
#define RCT_CMD_CLUSTER_KEYS_NUM		"cluster_keys_num"
#define RCT_CMD_CLUSTER_USED_MEMORY		"cluster_used_memory"
#define RCT_CMD_CLUSTER_NODE_INFO		"cluster_node_info"
#define RCT_CMD_CLUSTER_CLUSTER_INFO	"cluster_cluster_info"
#define RCT_CMD_SLOTS_STATE				"slots_state"
#define RCT_CMD_NODE_SLOT_NUM			"node_slot_num"
#define RCT_CMD_NEW_NODES_NAME			"new_nodes_name"
#define RCT_CMD_CLUSTER_REBALANCE		"cluster_rebalance"
#define RCT_CMD_FLUSHALL				"flushall"
#define RCT_CMD_CLUSTER_CONFIG_GET		"cluster_config_get"
#define RCT_CMD_CLUSTER_CONFIG_SET		"cluster_config_set"
#define RCT_CMD_CLUSTER_CONFIG_REWRITE	"cluster_config_rewrite"
#define RCT_CMD_CLUSTER_DO_COMMAND		"cluster_do_command"
#define RCT_CMD_CLUSTER_GET_STATE		"cluster_get_state"
#define RCT_CMD_NODE_LIST				"node_list"
#define RCT_CMD_DEL_KEYS				"del_keys"


#define CMD_FLAG_NEED_CONFIRM 			(1<<0)
#define CMD_FLAG_NOT_NEED_ADDRESS		(1<<1)
#define CMD_FLAG_NEED_SYNCHRONOUS		(1<<2)

struct rctContext;

typedef enum redis_command_type{
	REDIS_COMMAND_FLUSHALL,
	REDIS_COMMAND_CONFIG_GET,
	REDIS_COMMAND_CONFIG_SET,
	REDIS_COMMAND_CONFIG_REWRITE,
	REDIS_COMMAND_GET,
	REDIS_COMMAND_SET
} redis_command_type_t;

typedef enum node_state_type{
	REDIS_KEY_NUM,
	REDIS_MEMORY,
	NODES_CLUSTER_STATE
} node_state_type_t;

typedef void RCTCommandProc(struct rctContext *rct, int type);

typedef struct RCTCommand {
	char *name;
	char *description;
	RCTCommandProc *proc;
	int type;
	int min_arg_count;
	int max_arg_count;
	int flag;
}RCTCommand;

void rct_show_command_usage(void);

void populateCommandTable(dict *commands);

void cluster_state(struct rctContext *ctx , int type);
void cluster_cluster_state(struct rctContext *ctx , int type);
void cluster_used_memory(struct rctContext *ctx , int type);
void cluster_keys_num(struct rctContext *ctx , int type);
void cluster_node_state(struct rctContext *ctx , int type);
void cluster_flushall(struct rctContext *ctx, int type);
void cluster_config_get(struct rctContext *ctx , int type);
void cluster_config_set(struct rctContext *ctx , int type);
void cluster_config_rewrite(struct rctContext *ctx , int type);
void cluster_check(struct rctContext *ctx , int type);
void cluster_create(struct rctContext *ctx , int type);
void cluster_destroy(struct rctContext *ctx , int type);
void cluster_delete_all_slaves(struct rctContext *ctx , int type);

#endif
