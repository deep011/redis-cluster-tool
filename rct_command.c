#include "rct_core.h"

#define COMMAND_NAME_MAX_LENGTH 30

struct RCTCommand rctCommandTable[] = {
    {RCT_CMD_CLUSTER_STATE, "Show the cluster state.", 
        nodes_get_state, NODES_CLUSTER_STATE, 0, 0, 0},
    {RCT_CMD_CLUSTER_USED_MEMORY, "Show the cluster used memory.", 
        nodes_get_state, REDIS_MEMORY, 0, 0, 0},
    {RCT_CMD_CLUSTER_KEYS_NUM, "Show the cluster holds keys num.", 
        nodes_get_state, REDIS_KEY_NUM, 0, 0, 0},
    {RCT_CMD_SLOTS_STATE, "Show the slots state.", 
        slots_state, -1, 0, 0, 0},
    {RCT_CMD_NODE_SLOT_NUM, "Show the node hold slots number.", 
        show_nodes_hold_slot_num, -1, 0, 0, 0},
    {RCT_CMD_NEW_NODES_NAME, "Show the new nodes name that not covered slots.", 
        show_new_nodes_name, -1, 0, 0, 0},
    {RCT_CMD_CLUSTER_REBALANCE, "Show the cluster how to rebalance.", 
        cluster_rebalance, -1, 0, 0, 0},
    {RCT_CMD_FLUSHALL, "Flush all the cluster.", 
        do_command_node_by_node, REDIS_COMMAND_FLUSHALL, 0, 0, 1},
    {RCT_CMD_CLUSTER_CONFIG_GET, "Get config from every node in the cluster and check consistency.", 
        do_command_node_by_node, REDIS_COMMAND_CONFIG_GET, 1, 1, 0},
    {RCT_CMD_CLUSTER_CONFIG_SET, "Set config to every node in the cluster.", 
        do_command_node_by_node, REDIS_COMMAND_CONFIG_SET, 2, 2, 1},
    {RCT_CMD_CLUSTER_CONFIG_REWRITE, "Rewrite every node config to echo node for the cluster.", 
        do_command_node_by_node, REDIS_COMMAND_CONFIG_REWRITE, 0, 0, 1},
    {RCT_CMD_NODE_LIST, "List the nodes", 
        show_nodes_list, -1, 0, 0, 0},
    {RCT_CMD_DEL_KEYS, "Delete keys in the cluster. The keys must match a given glob-style pattern.(This command not block the redis)", 
        cluster_del_keys, -1, 1, 1, 1}
};


void
rct_show_command_usage(void)
{
    int j,k;
    int numcommands;
    RCTCommand *c;
    int command_name_len;
    char command_name_with_space[COMMAND_NAME_MAX_LENGTH + 1];

    numcommands = sizeof(rctCommandTable)/sizeof(RCTCommand);

    log_stdout("Commands:");

    for (j = 0; j < numcommands; j++) {
        c = rctCommandTable+j;

        command_name_len = strlen(c->name);
        if(command_name_len > COMMAND_NAME_MAX_LENGTH)
        {
            return;
        }

        memset(command_name_with_space, ' ', COMMAND_NAME_MAX_LENGTH);
        command_name_with_space[COMMAND_NAME_MAX_LENGTH] = '\0';
        memcpy(command_name_with_space, c->name, command_name_len);
        log_stdout("    %s:%s", command_name_with_space, c->description);       
    }
}


/* Populates the Redis Command Table starting from the hard coded list
 * we have in the rct_command.h file. */
void populateCommandTable(dict *commands) {
    
    int ret;
    int j;
    int numcommands;

    if(commands == NULL)
    {
        return;
    }

    numcommands = sizeof(rctCommandTable)/sizeof(RCTCommand);

    for (j = 0; j < numcommands; j++) {
        RCTCommand *c = rctCommandTable+j;

        ret = dictAdd(commands, sdsnew(c->name), c);
    }
}


