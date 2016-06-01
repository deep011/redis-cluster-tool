#include "rct_core.h"

#define COMMAND_NAME_MAX_LENGTH 30

struct RCTCommand rctCommandTable[] = {
    {RCT_CMD_CLUSTER_STATE, "Show the cluster state.", 
        cluster_state, -1, 0, 0, 4},
    {RCT_CMD_CLUSTER_CREATE, "Create a cluster.", 
        cluster_create, -1, 3, -1, 2},
    {RCT_CMD_CLUSTER_DESTROY, "Destroy the cluster.", 
        cluster_destroy, -1, 0, 0, 1},
    {RCT_CMD_CLUSTER_DEL_ALL_SLAVES, "Delete all the slaves in the cluster.", 
        cluster_delete_all_slaves, -1, 0, 0, 1},
    {RCT_CMD_CLUSTER_CHECK, "Check the cluster.", 
        cluster_check, -1, 0, 0, 0},
    {RCT_CMD_CLUSTER_USED_MEMORY, "Show the cluster used memory.", 
        cluster_used_memory, -1, 0, 0, 0},
    {RCT_CMD_CLUSTER_KEYS_NUM, "Show the cluster holds keys num.", 
        cluster_keys_num, -1, 0, 0, 0},
    {RCT_CMD_CLUSTER_NODE_INFO, "Show the cluster nodes state in the \"info\" command.", 
        cluster_node_state, -1, 1, 1, 0},
    {RCT_CMD_CLUSTER_CLUSTER_INFO, "Show the cluster state in the \"cluster info\" command.", 
        cluster_cluster_state, -1, 1, 1, 0},
    {RCT_CMD_SLOTS_STATE, "Show the slots state.", 
        slots_state, -1, 0, 0, 4},
    {RCT_CMD_NODE_SLOT_NUM, "Show the node hold slots number.", 
        show_nodes_hold_slot_num, -1, 0, 0, 4},
    {RCT_CMD_NEW_NODES_NAME, "Show the new nodes name that not covered slots.", 
        show_new_nodes_name, -1, 0, 0, 4},
    {RCT_CMD_CLUSTER_REBALANCE, "Show the cluster how to rebalance.", 
        cluster_rebalance, -1, 0, 0, 4},
    {RCT_CMD_FLUSHALL, "Flush all the cluster.", 
        cluster_flushall, -1, 0, 0, 1},
    {RCT_CMD_CLUSTER_CONFIG_GET, "Get config from every node in the cluster and check consistency.", 
        cluster_config_get, -1, 1, 1, 0},
    {RCT_CMD_CLUSTER_CONFIG_SET, "Set config to every node in the cluster.", 
        cluster_config_set, -1, 2, 10, 1},
    {RCT_CMD_CLUSTER_CONFIG_REWRITE, "Rewrite every node config to echo node for the cluster.", 
        cluster_config_rewrite, -1, 0, 0, 1},
    {RCT_CMD_NODE_LIST, "List the nodes", 
        show_nodes_list, -1, 0, 0, 4},
    {RCT_CMD_DEL_KEYS, "Delete keys in the cluster. The keys must match a given glob-style pattern.(This command not block the redis)", 
        cluster_del_keys, -1, 1, 1, 5}
};

void cluster_state(rctContext *ctx , int type)
{
    sds *str;
    
    if(hiarray_n(&ctx->args) != 0){
        log_error("Error: there can not have args for command %s", ctx->cmd);
        return;
    }

    str = hiarray_push(&ctx->args);
    *str = sdsnew("cluster_state");
    
    cluster_async_call(ctx, "cluster info", NULL, 
        ctx->redis_role, async_reply_info_display_check);
}

void cluster_create(rctContext *ctx , int type)
{   
    int ret;
    uint32_t i, j, k;
    sds *str;
    int replicas;
    redisContext *con_m, *con_s;
    struct hiarray *nodes = NULL;
    redis_instance *master, *slave, *node, *node_slave;
    int master_count;
    int slot_begin, slot_step, remainder;
    sds *master_slaves = NULL, *slaves_str = NULL;
    int master_slaves_count, slaves_str_count;
    redisReply *reply = NULL;
    listIter *it = NULL, *it_node = NULL;
    listNode *ln;
    sds command_addslot = NULL;
    
    if(hiarray_n(&ctx->args) < 3){
        log_error("Error: there must have at least three args for command %s", ctx->cmd);
        return;
    }

    replicas = 0;
    slot_begin = 0;
    slot_step = 0;

    str = hiarray_get(&ctx->args, 0);
    if(strcmp(*str, "--replicas") == 0){
        str = hiarray_get(&ctx->args, 1);
        if(str_is_integer(*str, sdslen(*str))){
            replicas = rct_atoi(str);
        }else{
            log_error("Error: there must have an integer behind --replicas in the command %s", ctx->cmd);
            return;
        }
    }

    master_count = hiarray_n(&ctx->args);
    slot_step = REDIS_CLUSTER_SLOTS/master_count;
    remainder = REDIS_CLUSTER_SLOTS%master_count;

    nodes = hiarray_create(master_count, sizeof(redis_instance));
    if(nodes == NULL){
        log_stdout("Out of memory");
        goto error;
    }

    for(i = 0; i < master_count; i ++){
        str = hiarray_get(&ctx->args, i);
        master_slaves = sdssplitlen(*str, sdslen(*str), "[", 1, &master_slaves_count);
        if(master_slaves == NULL || (master_slaves_count != 1 && 
            master_slaves_count != 2)){
            log_stdout("The address is error.");
            goto error;
        }
        
        master = hiarray_push(nodes);
        ret = redis_instance_init(master, master_slaves[0], RCT_REDIS_ROLE_MASTER);
        if(ret != RCT_OK){
            log_stdout("Init redis master instance error.");
            goto error;
        }

        master->slots_start = slot_begin;
        master->slots_count = slot_step + (remainder>0?1:0);
        slot_begin += master->slots_count;
        if(remainder > 0)   remainder--;

        if(master_slaves_count > 1){
            sdsrange(master_slaves[1], 0, -2);
            slaves_str = sdssplitlen(master_slaves[1], sdslen(master_slaves[1]), 
                "|", 1, &slaves_str_count);
            if(slaves_str == NULL || slaves_str_count <= 0){
                log_stdout("Slaves address %s is error.", master_slaves[1]);
                goto error;
            }

            for(k = 0; k < slaves_str_count; k ++){
                slave = redis_instance_create(slaves_str[k], RCT_REDIS_ROLE_SLAVE);
                if(slave == NULL){
                    log_stdout("Init redis slave instance error.");
                    goto error;
                }

                listAddNodeTail(master->slaves, slave);
            }

            sdsfreesplitres(slaves_str, slaves_str_count);
            slaves_str = NULL;
        }

        sdsfreesplitres(master_slaves, master_slaves_count);
        master_slaves = NULL;
    }

    RCT_ASSERT(master_count == hiarray_n(nodes));

    command_addslot = sdsnew("cluster addslots");

    for (i = 0; i < hiarray_n(nodes); i++) {
        master = hiarray_get(nodes, i);
        con_m = cxt_get_by_redis_instance(master);
        if (con_m == NULL || con_m->err) {
            log_stdout("Connect to %s failed: %s", 
                master->addr, con_m==NULL?"NULL":con_m->errstr);
            goto error;
        }

        for (j = 0; j < hiarray_n(nodes); j++) {
            node = hiarray_get(nodes, j);
            if (node != master) {
                reply = redisCommand(con_m, "cluster meet %s %d", node->host, node->port);
                if(reply == NULL || reply->type != REDIS_REPLY_STATUS || 
                    strcmp(reply->str, "OK") != 0){
                    log_stdout("Command \"cluster meet\" reply error: %s.",
                        reply==NULL?"NULL":(reply->type == REDIS_REPLY_ERROR?reply->str:"other"));
                    goto error;
                }

                freeReplyObject(reply);
                reply = NULL;
            }

            if(node->slaves){            
                it_node = listGetIterator(node->slaves, AL_START_HEAD);
                while((ln = listNext(it_node)) != NULL){
                    node_slave = listNodeValue(ln);
                    if(node_slave != master){
                        reply = redisCommand(con_m, "cluster meet %s %d", 
                            node_slave->host, node_slave->port);
                        if(reply == NULL || reply->type != REDIS_REPLY_STATUS || 
                            strcmp(reply->str, "OK") != 0){
                            log_stdout("Command \"cluster meet\" reply error: %s.",
                                reply==NULL?"NULL":(reply->type == REDIS_REPLY_ERROR?reply->str:"other"));
                            goto error;
                        }

                        freeReplyObject(reply);
                        reply = NULL;
                    }
                }
                
                listReleaseIterator(it_node);
                it_node = NULL;
            }
        }

        sdsrange(command_addslot, 0, 15);
        for(k = master->slots_start; k < master->slots_start+master->slots_count; k ++){
            command_addslot = sdscatfmt(command_addslot, " %i", k);
        }

        reply = redisCommand(con_m, command_addslot);
        if(reply == NULL || reply->type != REDIS_REPLY_STATUS || 
            strcmp(reply->str, "OK") != 0){
            log_stdout("Command \"cluster addslot\" reply error: %s.", 
                reply==NULL?"NULL":(reply->type == REDIS_REPLY_ERROR?reply->str:"other"));
            goto error;
        }

        freeReplyObject(reply);
        reply = NULL;

        if (master->slaves) {            
            it = listGetIterator(master->slaves, AL_START_HEAD);
            while((ln = listNext(it)) != NULL){
                slave = listNodeValue(ln);
                con_s = cxt_get_by_redis_instance(slave);
                if(con_s == NULL || con_s->err){
                    log_stdout("Connect to %s failed: %s", 
                        slave->addr, con_s==NULL?"NULL":con_s->errstr);
                    goto error;
                }

                
                for(j = 0; j < hiarray_n(nodes); j++){
                    node = hiarray_get(nodes, j);
                    if(node != slave){
                        reply = redisCommand(con_s, "cluster meet %s %d", node->host, node->port);
                        if(reply == NULL || reply->type != REDIS_REPLY_STATUS || 
                            strcmp(reply->str, "OK") != 0){
                            log_stdout("Command \"cluster meet\" reply error: %s.",
                                reply==NULL?"NULL":(reply->type == REDIS_REPLY_ERROR?reply->str:"other"));
                            goto error;
                        }
        
                        freeReplyObject(reply);
                        reply = NULL;
                    }
        
                    if(node->slaves){            
                        it_node = listGetIterator(node->slaves, AL_START_HEAD);
                        while((ln = listNext(it_node)) != NULL){
                            node_slave = listNodeValue(ln);
                            if(node_slave != master){
                                reply = redisCommand(con_s, "cluster meet %s %d", 
                                    node_slave->host, node_slave->port);
                                if(reply == NULL || reply->type != REDIS_REPLY_STATUS || 
                                    strcmp(reply->str, "OK") != 0){
                                    log_stdout("Command \"cluster meet\" reply error: %s.",
                                        reply==NULL?"NULL":(reply->type == REDIS_REPLY_ERROR?reply->str:"other"));
                                    goto error;
                                }
        
                                freeReplyObject(reply);
                                reply = NULL;
                            }
                        }
                        
                        listReleaseIterator(it_node);
                        it_node = NULL;
                    }
                }
                
            }
            
            listReleaseIterator(it);
            it = NULL;
        }
    }

    log_stdout_without_newline("Waiting for the nodes to join.");
    sleep(1);

    ctx->private_data = nodes;
    sdsfree(command_addslot);

    if (ctx->address) {
        sdsfree(ctx->address);
        ctx->address = NULL;
    }

    ctx->address = sdsnew(master->addr);
    
    cluster_async_call(ctx, "cluster nodes", NULL, 
        ctx->redis_role, async_reply_cluster_create);
    
    return;
    
error:

    if(master_slaves != NULL){
        sdsfreesplitres(master_slaves, master_slaves_count);
    }

    if(slaves_str != NULL){
        sdsfreesplitres(slaves_str, slaves_str_count);
    }

    if(reply != NULL){
        freeReplyObject(reply);
    }

    if(it){
        listReleaseIterator(it);
    }

    if(it_node){
        listReleaseIterator(it_node);
    }

    if(nodes != NULL){
        while(hiarray_n(nodes) > 0){
            master = hiarray_pop(nodes);
            redis_instance_deinit(master);
        }
        
        hiarray_destroy(nodes);
    }

    if(command_addslot != NULL){
        sdsfree(command_addslot);
    }

}

void cluster_destroy(rctContext *ctx , int type)
{
    ctx->redis_role = RCT_REDIS_ROLE_ALL;
    cluster_async_call(ctx, "ping", NULL, 
        ctx->redis_role, async_reply_destroy_cluster);
}

void cluster_delete_all_slaves(rctContext *ctx , int type)
{
    ctx->redis_role = RCT_REDIS_ROLE_ALL;
    cluster_async_call(ctx, "ping", NULL, 
        ctx->redis_role, async_reply_delete_all_slaves);
}

void cluster_check(rctContext *ctx , int type)
{    
    cluster_async_call(ctx, "cluster nodes", NULL, 
        ctx->redis_role, async_reply_check_cluster);
}

void cluster_keys_num(rctContext *ctx , int type)
{
    sds *str;
    
    if(hiarray_n(&ctx->args) != 0){
        log_error("Error: there can not have args for command %s", ctx->cmd);
    }

    str = hiarray_push(&ctx->args);
    *str = sdsnew("db0");
    
    cluster_async_call(ctx, "info", NULL, 
        ctx->redis_role, async_reply_info_keynum);
}

void cluster_used_memory(rctContext *ctx , int type)
{
    sds *str;
    
    if(hiarray_n(&ctx->args) != 0){
        log_error("Error: there can not have args for command %s", ctx->cmd);
    }

    str = hiarray_push(&ctx->args);
    *str = sdsnew("used_memory");
    
    cluster_async_call(ctx, "info", NULL, 
        ctx->redis_role, async_reply_info_memory);
}

void cluster_cluster_state(rctContext *ctx , int type)
{
    cluster_async_call(ctx, "cluster info", NULL, 
        ctx->redis_role, async_reply_info_display);
}

void cluster_node_state(rctContext *ctx , int type)
{
    cluster_async_call(ctx, "info", NULL, 
        ctx->redis_role, async_reply_info_display);
}

void cluster_flushall(rctContext *ctx , int type)
{
    ctx->redis_role = RCT_REDIS_ROLE_MASTER;
    cluster_async_call(ctx, "flushall", NULL, 
        ctx->redis_role, async_reply_status);
}

void cluster_config_get(rctContext *ctx , int type)
{
    sds cmd, config;

    config = *(sds *)hiarray_get(&ctx->args, 0);

    cmd = sdsnew("config get ");
    cmd = sdscatsds(cmd, config);

    if(strcmp("maxmemory", config) == 0){
        cluster_async_call(ctx, cmd, NULL, 
            ctx->redis_role, async_reply_maxmemory);
    }else{
        cluster_async_call(ctx, cmd, NULL, 
            ctx->redis_role, async_reply_display_check);
    }
    
    sdsfree(cmd);
}

void cluster_config_set(rctContext *ctx , int type)
{
    int i;
    struct hiarray *parameters;
    sds *str;

    parameters = hiarray_create(hiarray_n(&ctx->args), sizeof(sds));
    str = hiarray_push(parameters);
    *str = sdsnew("set");
    
    for(i = 0; i < hiarray_n(&ctx->args); i ++){
        str = hiarray_push(parameters);
        *str = sdsdup(*(sds*)hiarray_get(&ctx->args, i));
    }
    
    cluster_async_call(ctx, "config", parameters, 
        ctx->redis_role, async_reply_status);
}

void cluster_config_rewrite(rctContext *ctx , int type)
{
    cluster_async_call(ctx, "config rewrite", NULL, 
        ctx->redis_role, async_reply_status);
}

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


