#ifndef _RCT_COMMAND_H_
#define _RCT_COMMAND_H_

#define RCT_CMD_CLUSTER_STATE			"cluster_state"
#define RCT_CMD_CLUSTER_KEYS_NUM		"cluster_keys_num"
#define RCT_CMD_CLUSTER_USED_MEMORY		"cluster_used_memory"
#define RCT_CMD_SLOTS_STATE				"slots_state"
#define RCT_CMD_NODE_SLOT_NUM			"node_slot_num"
#define RCT_CMD_NEW_NODES_NAME			"new_nodes_name"
#define RCT_CMD_CLUSTER_REBALANCE		"cluster_rebalance"
#define RCT_CMD_FLUSHALL				"flushall"
#define RCT_CMD_CLUSTER_CONFIG_GET		"cluster_config_get"
#define RCT_CMD_CLUSTER_CONFIG_SET		"cluster_config_set"
#define RCT_CMD_CLUSTER_DO_COMMAND		"cluster_do_command"
#define RCT_CMD_CLUSTER_GET_STATE		"cluster_get_state"

struct rctContext;

typedef enum redis_command_type{
	REDIS_COMMAND_FLUSHALL,
	REDIS_COMMAND_CONFIG_GET,
	REDIS_COMMAND_CONFIG_SET,
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
}RCTCommand;

void rct_show_command_usage(void);

void populateCommandTable(dict *commands);

#endif