#include "rct_core.h"

#include<async.h>
#include<adapters/ae.h>
#include<hiutil.h>
#include<time.h>
#include <unistd.h>

#define RCT_AE_CONTINUE     0
#define RCT_AE_STOP         1

unsigned int dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

dictType commandTableDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

rctContext *
create_context(struct instance *nci)
{
    int ret;
    int j;
    rctContext *rct_ctx;
    dict *commands;
    sds *cmd_parts = NULL;
    int cmd_parts_count = 0;
    sds *arg_addr;

    if(nci == NULL)
    {
        return NULL;
    }

    rct_ctx = rct_alloc(sizeof(rctContext));
    if(rct_ctx == NULL)
    {
        return NULL;
    }

    rct_ctx->cc = NULL;
    rct_ctx->address = NULL;
    rct_ctx->cf = NULL;

    commands = dictCreate(&commandTableDictType,NULL);
    if(commands == NULL)
    {
        rct_free(rct_ctx);
        return NULL;
    }

    populateCommandTable(commands);
    rct_ctx->commands = commands;

    if (nci->addr)
        rct_ctx->address = sdsnew(nci->addr);

    cmd_parts = sdssplitargs(nci->command, &cmd_parts_count);
    if(cmd_parts == NULL || cmd_parts_count <= 0)
    {
        rct_free(rct_ctx);
        dictRelease(commands);
        return NULL;
    }
    
    rct_ctx->cmd = cmd_parts[0];

    ret = hiarray_init(&rct_ctx->args, 1, sizeof(sds));
    if(ret != RCT_OK)
    {
        sdsfreesplitres(cmd_parts, cmd_parts_count);
        rct_free(rct_ctx);
        dictRelease(commands);
        return NULL;
    }
    
    for(j = 1; j < cmd_parts_count; j++)
    {
        arg_addr = hiarray_push(&rct_ctx->args);
        *arg_addr = cmd_parts[j];
    }

    free(cmd_parts);

    if(strcmp(nci->role, RCT_OPTION_REDIS_ROLE_ALL) == 0)
    {
        rct_ctx->redis_role = RCT_REDIS_ROLE_ALL;
    }
    else if(strcmp(nci->role, RCT_OPTION_REDIS_ROLE_MASTER) == 0)
    {
        rct_ctx->redis_role = RCT_REDIS_ROLE_MASTER;
    }
    else if(strcmp(nci->role, RCT_OPTION_REDIS_ROLE_SLAVE) == 0)
    {
        rct_ctx->redis_role = RCT_REDIS_ROLE_SLAVE;
    }
    else
    {
        rct_ctx->redis_role = RCT_REDIS_ROLE_NULL;
    }

    if(nci->simple)
    {
        rct_ctx->simple = 1;
    }
    else
    {
        rct_ctx->simple = 0;
    }

    rct_ctx->buffer_size = nci->buffer_size;
    rct_ctx->thread_count = nci->thread_count;
    rct_ctx->commands_limit_per_second = nci->commands_limit_per_second;

    if (nci->conf_filename) {
        rct_ctx->cf = rct_conf_create_from_file(nci->conf_filename);
        if (rct_ctx->cf == NULL) {
            destroy_context(rct_ctx);
            return NULL;
        }

        rct_conf_debug_show(rct_ctx->cf);

        if (rct_ctx->address == NULL) {
            for (j = 0; j < hiarray_n(rct_ctx->cf->nodes); j ++) {
                redis_instance *master = hiarray_get(rct_ctx->cf->nodes, j);
                if (rct_ctx->address == NULL) {
                    rct_ctx->address = sdsnew(master->addr);
                } else {
                    rct_ctx->address = sdscat(rct_ctx->address, ",");
                    rct_ctx->address = sdscatsds(rct_ctx->address, master->addr);
                }
            }
        }
    }
    
    rct_ctx->acmd = NULL;
    rct_ctx->private_data = NULL;
    
    return rct_ctx;
}

void destroy_context(rctContext *rct_ctx)
{
    while (hiarray_n(&rct_ctx->args) > 0) {
        sds *arg = hiarray_pop(&rct_ctx->args);
        sdsfree(*arg);
    }
    hiarray_deinit(&rct_ctx->args);
    
    
    sdsfree(rct_ctx->cmd);
    dictRelease(rct_ctx->commands);
    
    if (rct_ctx->acmd != NULL){
        async_command_deinit(rct_ctx->acmd);
        rct_free(rct_ctx->acmd);
    }

    if (rct_ctx->address) {
        sdsfree(rct_ctx->address);
    }

    if (rct_ctx->cf) {
        rct_conf_destroy(rct_ctx->cf);
    }
    
    rct_free(rct_ctx);
}

typedef struct redis_node{
    sds name;
    sds addr;
    char *role_name;
    int slot_num_now;
    int slot_num_move;
    int slot_region_num_now;
    long long key_num;
    long long used_memory;
    sds cluster_state;
    sds config_value;
}redis_node;

static int
reshard_node_move_num_cmp(const void *t1, const void *t2)
{
    const redis_instance *s1 = (const redis_instance *)t1, *s2 = (const redis_instance *)t2;

    return s1->slots_to_import > s2->slots_to_import?-1:1;
}

static int
redis_node_config_value_cmp(const void *t1, const void *t2)
{
    const redis_node *s1 = t1, *s2 = t2;

    return sdscmp(s1->config_value, s2->config_value);
}

static int
elem_sds_cmp(const void *t1, const void *t2)
{
    const sds *s1 = t1, *s2 = t2;

    return sdscmp(*s1, *s2);
}

static char *node_role_name(cluster_node *node)
{
    if(node == NULL)
    {
        return NULL;
    }

    if(node->role == REDIS_ROLE_MASTER)
    {
        return RCT_REDIS_ROLE_NAME_MASTER;
    }
    else if(node->role == REDIS_ROLE_SLAVE)
    {
        return RCT_REDIS_ROLE_NAME_SLAVE;
    }

    return RCT_REDIS_ROLE_NAME_NODE;
}

void slots_state(rctContext *ctx, int type)
{
    redisClusterContext *cc = ctx->cc;
    uint32_t i, num = 0;
    
    if(cc == NULL)
    {
        return;
    }

    for(i = 0; i < hiarray_n(cc->slots); i ++, num ++)
    {
        struct cluster_slot **slot = (struct cluster_slot **)(hiarray_get(cc->slots, i));
        log_stdout("start : %d", (*slot)->start);
        log_stdout("end : %d", (*slot)->end);
        log_stdout("name : %s", (*slot)->node->name);
        log_stdout("host : %s", (*slot)->node->host);
        log_stdout("port : %d", (*slot)->node->port);
        log_stdout("addr : %s", (*slot)->node->addr);
        log_stdout("node role : %s", node_role_name((*slot)->node));
        log_stdout("context : %d", (*slot)->node->con?1:0);
        log_stdout("asyncContext : %d\n", (*slot)->node->acon?1:0);
    }   

    log_stdout("total solts region num : %d", num);
}

int node_hold_slot_num(struct cluster_node *node, redis_node *r_node, int isprint)
{
    hilist *slots;
    listIter *iter;
    listNode *list_node;
    struct cluster_slot *slot;
    int slot_count = 0;
    int slots_count = 0;
    if(node == NULL)
    {
        return -1;
    }
    slots = node->slots;
    if(slots == NULL)
    {
        if(isprint)
        {
            log_stdout("node[%s] holds %d slots_region and %d slots", node->addr, 0, 0);
        }
        return 0;
    }

    iter = listGetIterator(slots, AL_START_HEAD);
    
    list_node = listNext(iter);
    
    while(list_node != NULL)
    {
        slot = list_node->value;

        slot_count += slot->end - slot->start + 1;
        slots_count ++;

        list_node = listNext(iter);
    }

    listReleaseIterator(iter);
    
    if(r_node != NULL)
    {
        r_node->slot_region_num_now = slots_count;
        r_node->slot_num_now = slot_count;
    }
    
    if(isprint)
    {
        log_stdout("node[%s] holds %d slots_region and %d slots", node->addr, slots_count, slot_count);
    }

    return slot_count;
}

void cluster_rebalance1(rctContext *ctx, int type)
{
    dict *nodes = ctx->cc->nodes;
    dictIterator *di;
    dictEntry *de;
    struct cluster_node *node;
    int total_slot_num = 0, node_slot_num = 0;
    int final_slot_num_per_node = 0, slot_num_to_move = 0;
    int nodes_count = 0;
    struct hiarray *reshard_nodes;
    redis_node *node_to_reshard, *node_reshard_from, *node_reshard_to;
    int i, array_len = 0;
    int nodes_num_need_import = 0, begin_index = 0;

    if(nodes == NULL)
    {   
        return;
    }

    nodes_count = dictSize(nodes);
    if(nodes_count <= 0)
    {
        return;
    }
    
    reshard_nodes = hiarray_create(nodes_count, sizeof(*node_to_reshard));
    if(reshard_nodes == NULL)
    {
        return;
    }
    
    di = dictGetIterator(nodes);

    while((de = dictNext(di)) != NULL) {
        node = dictGetEntryVal(de);

        node_slot_num = node_hold_slot_num(node, NULL, 0);
        
        node_to_reshard = hiarray_push(reshard_nodes);
        
        node_to_reshard->name = node->name;
        node_to_reshard->slot_num_now = node_slot_num;
        node_to_reshard->slot_num_move = 0;
    
        total_slot_num += node_slot_num;
    }
    
    dictReleaseIterator(di);
    
    array_len = (int)hiarray_n(reshard_nodes);
    if(array_len != nodes_count)
    {
        reshard_nodes->nelem = 0;
        hiarray_destroy(reshard_nodes);
        return;
    }

    final_slot_num_per_node = total_slot_num/nodes_count;
    
    if(final_slot_num_per_node <= 0)
    {
        return;
    }
    
    for(i = 0; i < array_len; i ++)
    {
        node_to_reshard = hiarray_get(reshard_nodes, i);
        node_to_reshard->slot_num_move = final_slot_num_per_node - node_to_reshard->slot_num_now;
        if(node_to_reshard->slot_num_move > 0)
        {
            nodes_num_need_import ++;
        }
    }
    
    hiarray_sort(reshard_nodes, reshard_node_move_num_cmp);
    
    for(i = 0; i < array_len; i ++)
    {
        node_to_reshard = hiarray_get(reshard_nodes, i);
    }
    
    begin_index = array_len - nodes_num_need_import;
    
    for(i = 0; i < array_len; i ++)
    {
        node_reshard_from = hiarray_get(reshard_nodes, i);
        if(node_reshard_from->slot_num_move >= 0)
        {
            break;
        }
        
        if(begin_index >= array_len)
        {
            for(i = 0; i < array_len; i ++)
            {
                node_to_reshard = hiarray_get(reshard_nodes, i);
            }
            break;
        }
        
        node_reshard_to = hiarray_get(reshard_nodes, begin_index);
        
        slot_num_to_move = node_reshard_from->slot_num_move + node_reshard_to->slot_num_move;
        if(slot_num_to_move > 0)
        {
            slot_num_to_move = 0 - node_reshard_from->slot_num_move;
            node_reshard_from->slot_num_now -= slot_num_to_move;
            node_reshard_from->slot_num_move += slot_num_to_move;
            
            node_reshard_to->slot_num_now += slot_num_to_move;
            node_reshard_to->slot_num_move -= slot_num_to_move;
            
        }
        else if(slot_num_to_move == 0)
        {
            slot_num_to_move = node_reshard_to->slot_num_move;
            node_reshard_from->slot_num_now -= slot_num_to_move;
            node_reshard_from->slot_num_move += slot_num_to_move;
            
            node_reshard_to->slot_num_now += slot_num_to_move;
            node_reshard_to->slot_num_move -= slot_num_to_move;
            begin_index ++;
        }
        else
        {
            slot_num_to_move = node_reshard_to->slot_num_move;
            node_reshard_from->slot_num_now -= slot_num_to_move;
            node_reshard_from->slot_num_move += slot_num_to_move;
            
            node_reshard_to->slot_num_now += slot_num_to_move;
            node_reshard_to->slot_num_move -= slot_num_to_move;
            i --;
            begin_index ++;
        }
        log_stdout("--from %s --to %s --slots %d", node_reshard_from->name, node_reshard_to->name, slot_num_to_move);
    }

    reshard_nodes->nelem = 0;
    hiarray_destroy(reshard_nodes);
}

void cluster_rebalance(rctContext *ctx, int type)
{
    int i;
    dict *cluster_nodes = NULL;
    dictIterator *di;
    dictEntry *de;
    struct cluster_node *cluster_node;
    int total_slot_num = 0, node_slot_num = 0;
    int final_slot_num_per_node = 0, slot_num_to_move = 0;
    int cluster_nodes_count = 0;
    struct hiarray *reshard_nodes = NULL;
    redis_instance *reshard_node, *reshard_node_from, *reshard_node_to;
    int reshard_nodes_count = 0;
    int nodes_num_need_import_slots = 0, node_move_index = 0;
    sds arg;
    size_t args_valid_count = 0;
    int use_redistrib = 1;

    for (i = 0; i < hiarray_n(&ctx->args); i ++) {
        char * arg_in;
        size_t arg_len_in;
        arg = *(sds *)hiarray_get(&ctx->args, i);

        arg_in = "use-redis-trib";arg_len_in = strlen(arg_in);
        if (sdslen(arg) == arg_len_in && memcmp(arg, arg_in, arg_len_in) == 0) {
            use_redistrib = 1;
            args_valid_count ++;
            continue;
        }
    }

    if (args_valid_count != hiarray_n(&ctx->args)) {
        log_stderr("ERROR: some arguments is wrong for 'rebalance' command.");
        return;
    }

    cluster_nodes = ctx->cc->nodes;
    if (cluster_nodes == NULL) {   
        goto end;
    }

    cluster_nodes_count = dictSize(cluster_nodes);
    if (cluster_nodes_count <= 0) {
        goto end;
    }

    if (ctx->cf != NULL) {
        reshard_nodes = ctx->cf->nodes;
    } else {
        reshard_nodes = redis_instance_array_create_from_cluster(cluster_nodes);
        if (reshard_nodes == NULL) {   
            goto end;
        }
        redis_instance_array_assign_master_slots(reshard_nodes);
    }
    if (reshard_nodes == NULL) {   
        goto end;
    }

    reshard_nodes_count = hiarray_n(reshard_nodes);
    if (reshard_nodes_count <= 0) {
        goto end;
    }
    
    if (cluster_nodes_count != reshard_nodes_count) {
        log_stderr("ERROR: nodes count in the conf file is not equal to the cluster.");
        goto end;
    }

    for (i = 0; i < reshard_nodes_count; i ++) {
        reshard_node = hiarray_get(reshard_nodes, i);

        de = dictFind(cluster_nodes, reshard_node->addr);
        if (de == NULL) {
            log_stderr("ERROR: node(%s) in the conf file can't find in the cluster.", reshard_node->addr);
            goto end;
        }
        cluster_node = dictGetEntryVal(de);

        reshard_node->name = sdsdup(cluster_node->name);
        reshard_node->slots_to_import = reshard_node->slots_count - node_hold_slot_num(cluster_node, NULL, 0);
        if (reshard_node->slots_to_import > 0) {
            nodes_num_need_import_slots ++;
        }
    }

    di = dictGetIterator(cluster_nodes);
    while((de = dictNext(di)) != NULL) {
        cluster_node = dictGetEntryVal(de);

        log_debug(LOG_NOTICE, "master: %s, slots_count: %d", cluster_node->addr, node_hold_slot_num(cluster_node, NULL, 0));

    }
    log_debug(LOG_NOTICE, "");
    dictReleaseIterator(di);

    hiarray_sort(reshard_nodes, reshard_node_move_num_cmp);
    rct_redis_instance_array_debug_show(reshard_nodes);
    
    node_move_index = reshard_nodes_count - 1;
    
    for (i = 0; i < reshard_nodes_count; i ++) {
        reshard_node_to = hiarray_get(reshard_nodes, i);
        if (reshard_node_to->slots_to_import <= 0) {
            break;
        }
        
        if (node_move_index < nodes_num_need_import_slots) {
            break;
        }
        
        reshard_node_from = hiarray_get(reshard_nodes, node_move_index);
        
        slot_num_to_move = reshard_node_from->slots_to_import + reshard_node_to->slots_to_import;
        if (slot_num_to_move > 0) {
            slot_num_to_move = 0 - reshard_node_from->slots_to_import;
            
            reshard_node_from->slots_to_import += slot_num_to_move;
            reshard_node_to->slots_to_import -= slot_num_to_move;
            
            i --;
            node_move_index --;
        } else if(slot_num_to_move == 0) {
            slot_num_to_move = reshard_node_to->slots_to_import;
            
            reshard_node_from->slots_to_import += slot_num_to_move;
            
            reshard_node_to->slots_to_import -= slot_num_to_move;
            node_move_index --;
        } else {
            slot_num_to_move = reshard_node_to->slots_to_import;
            
            reshard_node_from->slots_to_import += slot_num_to_move;
            reshard_node_to->slots_to_import -= slot_num_to_move;
        }
        log_stdout("--from %s --to %s --slots %d", reshard_node_from->name, reshard_node_to->name, slot_num_to_move);
    }

    rct_redis_instance_array_debug_show(reshard_nodes);

    for (i = 0; i < reshard_nodes_count; i ++) {
        reshard_node = hiarray_get(reshard_nodes, i);
        RCT_ASSERT(reshard_node->slots_to_import == 0);
    }

end:

    if (ctx->cf == NULL && reshard_nodes != NULL) {
        reshard_nodes->nelem = 0;
        hiarray_destroy(reshard_nodes);
    }
}

void show_new_nodes_name(rctContext *ctx, int type)
{
    dict *nodes = ctx->cc->nodes;
    dictIterator *di;
    dictEntry *de;
    struct cluster_node *node;
    
    if(nodes == NULL)
    {
        return;
    }

    di = dictGetIterator(nodes);
    
    while((de = dictNext(di)) != NULL) {
        node = dictGetEntryVal(de);

        if(node->slots == NULL || listLength(node->slots) == 0)
        {
            log_stdout("%s", node->name==NULL?"NULL":node->name);
        }
    }

    dictReleaseIterator(di);
}

void show_nodes_list(rctContext *ctx, int type)
{
    dict *nodes = ctx->cc->nodes;
    hilist *slaves;
    dictIterator *di;
    dictEntry *de;
    listIter *it;
    listNode *ln;
    struct cluster_node *node, *slave;
    int start_flag = 1;
    
    if(nodes == NULL)
    {
        return;
    }

    di = dictGetIterator(nodes);
    
    while((de = dictNext(di)) != NULL) {
        node = dictGetEntryVal(de);

        if(start_flag)
        {
            start_flag = 0;
        }
        else if(ctx->redis_role == RCT_REDIS_ROLE_ALL && 
            (node->role == REDIS_ROLE_MASTER))
        {
            log_stdout("");
        }

        if(ctx->redis_role == RCT_REDIS_ROLE_ALL 
            || ctx->redis_role == RCT_REDIS_ROLE_MASTER)
        {
            log_stdout("master[%s]", node->addr);
        }

        if(ctx->redis_role == RCT_REDIS_ROLE_ALL
            || ctx->redis_role == RCT_REDIS_ROLE_SLAVE)
        {
            slaves = node->slaves;
            if(slaves == NULL)
            {
                continue;
            }
                
            it = listGetIterator(slaves, AL_START_HEAD);
            while((ln = listNext(it)) != NULL)
            {
                slave = listNodeValue(ln);
                if(ctx->redis_role == RCT_REDIS_ROLE_SLAVE)
                {
                    log_stdout("slave[%s]", slave->addr);
                }
                else
                {
                    log_stdout(" slave[%s]", slave->addr);
                }
            }

            listReleaseIterator(it);            
        }
        
    }

    dictReleaseIterator(di);
}

void show_nodes_hold_slot_num(rctContext *ctx, int type)
{
    int total_slot_num;

    total_slot_num = nodes_hold_slot_num(ctx->cc->nodes, ctx->simple?0:1);

    log_stdout("cluster holds %d slots", total_slot_num);
}


int nodes_hold_slot_num(dict *nodes, int isprint)
{
    dictIterator *di;
    dictEntry *de;
    cluster_node *node;
    struct hiarray *statistics_nodes = NULL;
    redis_node *statistics_node;
    int total_slot_num = 0, node_slot_num = 0;
    int nodes_count = 0;
    int i;

    if(nodes == NULL)
    {
        return -1;
    }

    if(isprint)
    {
        nodes_count = dictSize(nodes);
        if(nodes_count <= 0)
        {
            return -1;
        }
        
        statistics_nodes = hiarray_create(nodes_count, sizeof(*statistics_node));
        if(statistics_nodes == NULL)
        {
            return -1;
        }
    }
    
    di = dictGetIterator(nodes);
    
    while((de = dictNext(di)) != NULL) {
        node = dictGetEntryVal(de);
        
        if(isprint)
        {
            statistics_node = hiarray_push(statistics_nodes);
            
            statistics_node->name = node->name;
            statistics_node->addr = node->addr;
            node_slot_num = node_hold_slot_num(node, statistics_node, 0);
        }
        else
        {
            node_slot_num = node_hold_slot_num(node, NULL, 0);
        }
        total_slot_num += node_slot_num;
    }
    
    dictReleaseIterator(di);
    
    
    if(isprint)
    {
        for(i = 0; i < hiarray_n(statistics_nodes) && total_slot_num > 0; i ++)
        {
            statistics_node = hiarray_get(statistics_nodes, i);
            log_stdout("node[%s] holds %d slots_region and %d slots\t%.2f%%", statistics_node->addr, 
                statistics_node->slot_region_num_now, statistics_node->slot_num_now, 
                ((float)statistics_node->slot_num_now/(float)total_slot_num)*100);
        }
        
        log_stdout("");
        statistics_nodes->nelem = 0;
        hiarray_destroy(statistics_nodes);
    }
    
    return total_slot_num;
}

void nodes_keys_num(redisClusterContext *cc)
{
    dictIterator *di;
    dictEntry *de;
    
    dict *nodes;
    cluster_node *node;
    redisContext *c = NULL;
    redisReply *reply = NULL;
    int node_keys_num = 0, nodes_keys_num = 0;
    sds *line = NULL, *part = NULL, *partchild = NULL;
    int line_len = 0, part_len = 0, partchild_len = 0;
    
    if(cc == NULL)
    {
        return;
    }
    
    nodes = cc->nodes;
    if(nodes == NULL)
    {
        return;
    }
    
    //iter = listGetIterator(nodes, AL_START_HEAD);
    di = dictGetIterator(nodes);
    
    while((de = dictNext(di)) != NULL) {
        node = dictGetEntryVal(de);

        if(listLength(node->slots))
        {
            c = ctx_get_by_node(cc, node);
            if(c == NULL)
            {   
                log_stdout("node[%s] get connect failed", node->addr);
                continue;
            }
            
            reply = redisCommand(c, "info Keyspace");
            if(reply == NULL)
            {
                log_stdout("node[%s] get reply null()", node->addr, cc->errstr);
                continue;
            }
            
            if(reply->type != REDIS_REPLY_STRING)
            {
                log_stdout("error: reply type error!");
                goto done;
            }
            
            line = sdssplitlen(reply->str, reply->len, "\r\n", 2, &line_len);
            if(line == NULL)
            {
                log_stdout("error: line split error(null)!");
                goto done;
            }
            else if(line_len == 2)
            {
                log_stdout("node[%s] has %d keys", node->addr, 0);
                sdsfreesplitres(line, line_len);
                line = NULL;
                continue;
            }
            else if(line_len != 3)
            {
                log_stdout("error: line split error(line_len != 3)!");
                goto done;
            }
            
            part = sdssplitlen(line[1], sdslen(line[1]), ",", 1, &part_len);
            if(line == NULL || line_len != 3)
            {
                log_stdout("error: part split error!");
                goto done;
            }
            
            partchild = sdssplitlen(part[0], sdslen(part[0]), "=", 1, &partchild_len);
            if(partchild == NULL || partchild_len != 2)
            {
                log_stdout("error: partchild split error!");
                goto done;
            }
            
            node_keys_num = rct_atoi(partchild[1]);
            
            if(node_keys_num < 0)
            {
                goto done;
            }
            
            log_stdout("node[%s] has %d keys", node->addr, node_keys_num);
            
            nodes_keys_num += node_keys_num;
            
            sdsfreesplitres(line, line_len);
            line = NULL;
            sdsfreesplitres(part, part_len);
            part = NULL;
            sdsfreesplitres(partchild, partchild_len);
            partchild = NULL;
            freeReplyObject(reply);
            reply = NULL;
        }
        
    }
    
    
    log_stdout("cluster has %d keys", nodes_keys_num);
    
done:   
    
    if(line)
    {
        sdsfreesplitres(line, line_len);
    }
    
    if(part)
    {
        sdsfreesplitres(part, part_len);
    }
            
    if(partchild)
    {
        sdsfreesplitres(partchild, partchild_len);
    }
    
    if(reply != NULL)
    {
        freeReplyObject(reply);
    }
    
    dictReleaseIterator(di);
}

long long node_key_num(redisClusterContext *cc, cluster_node *node, int isprint)
{
    long long key_num = 0;
    sds *line = NULL, *part = NULL, *partchild = NULL;
    int line_len = 0, part_len = 0, partchild_len = 0;
    redisContext *c = NULL;
    redisReply *reply = NULL;
    
    if(node == NULL)
    {
        return -1;
    }
    

    c = ctx_get_by_node(cc, node);
    if(c == NULL)
    {   
        if(isprint)
        {
            log_stdout("node[%s] get connect failed", node->addr);
        }
        key_num = -1;
        goto done;
    }
    
    reply = redisCommand(c, "info Keyspace");
    if(reply == NULL)
    {
        if(isprint)
        {
            log_stdout("node[%s] get reply null(%s)", node->addr, c->errstr);
        }
        key_num = -1;
        goto done;
    }
    
    if(reply->type != REDIS_REPLY_STRING)
    {
        if(isprint)
        {
            log_stdout("error: reply type error!");
        }
        key_num = -1;
        goto done;
    }
    
    line = sdssplitlen(reply->str, reply->len, "\r\n", 2, &line_len);
    if(line == NULL)
    {
        if(isprint)
        {
            log_stdout("error: line split error(null)!");
        }
        key_num = -1;
        goto done;
    }
    else if(line_len == 2)
    {
        key_num = 0;
        if(isprint)
        {
            log_stdout("node[%s] has %d keys", node->addr, key_num);
        }
        goto done;
    }
    else if(line_len != 3)
    {
        if(isprint)
        {
            log_stdout("error: line split error(line_len != 3)!");
        }
        goto done;
    }
    
    part = sdssplitlen(line[1], sdslen(line[1]), ",", 1, &part_len);
    if(line == NULL || line_len != 3)
    {
        if(isprint)
        {
            log_stdout("error: part split error!");
        }
        goto done;
    }
    
    partchild = sdssplitlen(part[0], sdslen(part[0]), "=", 1, &partchild_len);
    if(partchild == NULL || partchild_len != 2)
    {
        if(isprint)
        {
            log_stdout("error: partchild split error!");
        }
        goto done;
    }
    
    key_num = rct_atoll(partchild[1], sdslen(partchild[1]));
    
    if(key_num < 0)
    {
        goto done;
    }
    
    if(isprint)
    {
        log_stdout("node[%s] has %d keys", node->addr, key_num);
    }
    
done:
    if(line)
    {
        sdsfreesplitres(line, line_len);
    }
    
    if(part)
    {
        sdsfreesplitres(part, part_len);
    }
            
    if(partchild)
    {
        sdsfreesplitres(partchild, partchild_len);
    }
    
    if(reply != NULL)
    {
        freeReplyObject(reply);
        reply == NULL;
    }
    
    return key_num;
}

long long node_memory_size(redisClusterContext *cc, cluster_node *node, int isprint)
{
    long long memory_size = 0;
    sds *line = NULL, *key_value = NULL;
    int line_len = 0, key_value_len = 0;
    redisContext *c = NULL;
    redisReply *reply = NULL;
    int i;
    
    if(node == NULL)
    {
        return -1;
    }
    
    c = ctx_get_by_node(cc, node);
    if(c == NULL)
    {   
        if(isprint)
        {
            log_stdout("node[%s] get connect failed", node->addr);
        }
        memory_size = -1;
        goto done;
    }
    
    reply = redisCommand(c, "info Memory");
    if(reply == NULL)
    {
        if(isprint)
        {
            log_stdout("node[%s] get reply null(%s)", node->addr, c->errstr);
        }
        memory_size = -1;
        goto done;
    }
    
    if(reply->type != REDIS_REPLY_STRING)
    {
        if(isprint)
        {
            log_stdout("error: reply type error!");
        }
        memory_size = -1;
        goto done;
    }
    
    line = sdssplitlen(reply->str, reply->len, "\r\n", 2, &line_len);
    if(line == NULL || line_len <= 0)
    {
        if(isprint)
        {
            log_stdout("error: line split error(null)!");
        }
        memory_size = -1;
        goto done;
    }
    
    for(i = 0; i < line_len; i ++)
    {
        key_value = sdssplitlen(line[i], sdslen(line[i]), ":", 1, &key_value_len);
        if(key_value == NULL || key_value_len != 2)
        {
            continue;
        }
        
        if(strcmp(key_value[0], "used_memory") == 0)
        {
            memory_size = rct_atoll(key_value[1], sdslen(key_value[1]));
            goto done;
        }
        
        sdsfreesplitres(key_value, key_value_len);
        key_value = NULL;
    }
    
    if(isprint)
    {
        log_stdout("error: key_value split error or used_memory not found!");
    }
    memory_size = -1;
    
done:
    if(isprint)
    {
        log_stdout("node[%s] used %lld M", node->addr, memory_size);
    }

    if(line)
    {
        sdsfreesplitres(line, line_len);
    }
            
    if(key_value)
    {
        sdsfreesplitres(key_value, key_value_len);
    }
    
    if(reply != NULL)
    {
        freeReplyObject(reply);
        reply == NULL;
    }
    
    return memory_size;
}

typedef struct state_data{
    long long num_sum;
    int all_is_ok;
    int first_flag;
}state_data;

sds node_cluster_state(rctContext *ctx, cluster_node *node, void *data, int isprint)
{
    redisClusterContext *cc = ctx->cc;
    sds cluster_state = NULL;
    sds *line = NULL, *key_value = NULL;
    int line_len = 0, key_value_len = 0;
    redisContext *c = NULL;
    redisReply *reply = NULL;
    int i;
    char format_space[2] = {'\0'};
    state_data *sdata = data;

    if(node->role == REDIS_ROLE_SLAVE)
    {
        format_space[0] = ' ';
    }
    
    if(node == NULL)
    {
        return NULL;
    }

    if(sdata->first_flag)
    {
        sdata->first_flag = 0;
    }
    else if(ctx->redis_role == RCT_REDIS_ROLE_ALL && 
            (node->role == REDIS_ROLE_MASTER) && 
            ctx->simple == 0)
    {
        log_stdout("");
    }

    c = ctx_get_by_node(cc, node);
    if(c == NULL)
    {   
        if(isprint)
        {
            log_stdout("error: node[%s] get connect failed", node->addr);
        }
        goto done;
    }
    
    reply = redisCommand(c, "cluster info");
    if(reply == NULL)
    {
        if(isprint)
        {
            log_stdout("error: node[%s] get reply null()", node->addr, c->errstr);
        }
        goto done;
    }
    
    if(reply->type != REDIS_REPLY_STRING)
    {
        if(isprint)
        {
            log_stdout("error: reply type error!");
        }
        goto done;
    }
    
    line = sdssplitlen(reply->str, reply->len, "\r\n", 2, &line_len);
    if(line == NULL || line_len <= 0)
    {
        if(isprint)
        {
            log_stdout("error: line split error(null)!");
        }
        goto done;
    }
    
    for(i = 0; i < line_len; i ++)
    {
        key_value = sdssplitlen(line[i], sdslen(line[i]), ":", 1, &key_value_len);
        if(key_value == NULL || key_value_len != 2)
        {
            continue;
        }
        
        if(strcmp(key_value[0], "cluster_state") == 0)
        {
            cluster_state = sdsdup(key_value[1]);
            goto done;
        }
        
        sdsfreesplitres(key_value, key_value_len);
        key_value = NULL;
    }
    
    if(isprint)
    {
        log_stdout("error: key_value split error or cluster_state not found!");
    }
    
done:
    if(isprint && cluster_state != NULL)
    {
        log_stdout("%s%s[%s] cluster_state is %s", format_space, 
            node_role_name(node), node->addr, 
            cluster_state?cluster_state:"NULL");
    }

    if(line)
    {
        sdsfreesplitres(line, line_len);
    }
            
    if(key_value)
    {
        sdsfreesplitres(key_value, key_value_len);
    }
    
    if(reply != NULL)
    {
        freeReplyObject(reply);
        reply == NULL;
    }
    
    return cluster_state;
}

static int node_get_state(rctContext *ctx, cluster_node *node, 
    node_state_type_t state_type, struct hiarray *statistics_nodes, void *data)
{
    redisClusterContext *cc = ctx->cc;
    redis_node *statistics_node = NULL;
    state_data *sdata = data;
    sds cluster_state = NULL;
    long long value;
    
    switch(state_type)
    {
    case REDIS_KEY_NUM:

        value = node_key_num(cc, node, 0);

        if(statistics_nodes != NULL)
        {
            statistics_node = hiarray_push(statistics_nodes);
    
            statistics_node->addr = node->addr;
            statistics_node->role_name = node_role_name(node);
                
            statistics_node->key_num = value;
        }
        
        if(value < 0)
        {
            goto error;
        }
        
        sdata->num_sum += value;
        
        break;
    case REDIS_MEMORY:

        value = node_memory_size(cc, node, 0)/1048576;

        if(statistics_nodes != NULL)
        {

            statistics_node = hiarray_push(statistics_nodes);
        
            statistics_node->addr = node->addr;
            statistics_node->role_name = node_role_name(node);
            
            statistics_node->used_memory = value;
        }

        if(value < 0)
        {
            goto error;
        }
        
        sdata->num_sum += value;
        
        break;
    case NODES_CLUSTER_STATE:
        
        cluster_state = node_cluster_state(ctx, node, sdata, ctx->simple?0:1);
        if(cluster_state == NULL)
        {
            sdata->all_is_ok = 0;
            goto error;
        }
        
        if(strcmp(cluster_state, "ok") != 0)
        {
            sdata->all_is_ok = 0;
            sdsfree(cluster_state);
            goto error;
        }
        
        sdsfree(cluster_state);
        break;
    default:
        
        break;
    }

    return RCT_OK;

error:

    return RCT_ERROR;
}

void nodes_get_state(rctContext *ctx, int type)
{
    int ret;
    redisClusterContext *cc = ctx->cc;
    node_state_type_t state_type = type;
    dictIterator *di;
    dictEntry *de;
    dict *nodes;
    listIter *it;
    listNode *ln;
    hilist *slaves;
    cluster_node *master, *slave;
    redisContext *c = NULL;
    redisReply *reply = NULL;
    int i, nodes_count = 0;
    struct hiarray *statistics_nodes = NULL;
    redis_node *statistics_node = NULL;
    state_data sdata;
    char format_space[2] = {'\0'};
    int start_flag = 1;

    sdata.all_is_ok = 1;
    sdata.num_sum = 0;
    sdata.first_flag = 1;
    
    if(cc == NULL)
    {
        return;
    }
    
    nodes = cc->nodes;
    if(nodes == NULL)
    {
        return;
    }
    
    nodes_count = dictSize(nodes);
    if(nodes_count <= 0)
    {
        return;
    }

    if(ctx->simple == 0)
    {
        statistics_nodes = hiarray_create(nodes_count, sizeof(*statistics_node));
        if(statistics_nodes == NULL)
        {
            return;
        }
    }
    
    di = dictGetIterator(nodes);
    
    while((de = dictNext(di)) != NULL) {
        master = dictGetEntryVal(de);
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_MASTER)
        {
            ret = node_get_state(ctx, master, state_type, statistics_nodes, &sdata);
            if(ret != RCT_OK)
            {
                continue;
            }
        }

        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE)
        {
            slaves = master->slaves;
            if(slaves == NULL)
            {
                continue;
            }
            
            it = listGetIterator(slaves, AL_START_HEAD);
            while((ln = listNext(it)) != NULL)
            {
                slave = listNodeValue(ln);
                
                ret = node_get_state(ctx, slave, state_type, statistics_nodes, &sdata);
                if(ret != RCT_OK)
                {
                    continue;
                }
            }

            listReleaseIterator(it);
        }
        
    }

    for(i = 0; ctx->simple == 0 && i < hiarray_n(statistics_nodes); i ++)
    {
        statistics_node = hiarray_get(statistics_nodes, i);

        if(start_flag)
        {
            start_flag = 0;
        }
        else if(ctx->redis_role == RCT_REDIS_ROLE_ALL && 
            (strcmp(statistics_node->role_name, 
            RCT_REDIS_ROLE_NAME_MASTER) == 0))
        {
            log_stdout("");
        }
        
        switch(state_type)
        {
        case REDIS_KEY_NUM:
            if(sdata.num_sum <= 0)
            {
                goto done;
            }

            if(strcmp(statistics_node->role_name, 
                RCT_REDIS_ROLE_NAME_SLAVE) == 0)
            {
                format_space[0] = ' ';
            }
            else
            {
                format_space[0] = '\0';
            }
            
            log_stdout("%s%s[%s] has %d keys\t%d%s", format_space, 
                statistics_node->role_name, statistics_node->addr, 
                statistics_node->key_num, 
                (statistics_node->key_num*100)/sdata.num_sum, "%");
            
            break;
        case REDIS_MEMORY:

            if(sdata.num_sum <= 0)
            {
                goto done;
            }
            
            if(strcmp(statistics_node->role_name, 
                RCT_REDIS_ROLE_NAME_SLAVE) == 0)
            {
                format_space[0] = ' ';
            }
            else
            {
                format_space[0] = '\0';
            }
            
            log_stdout("%s%s[%s] used %lld M\t%d%s", format_space, 
                statistics_node->role_name, statistics_node->addr, 
                statistics_node->used_memory, 
                (statistics_node->used_memory*100)/sdata.num_sum,
                "%");
            
            break;
        case NODES_CLUSTER_STATE:
            
            break;
        default:
            
            break;
        }
    }
    
done:
    if(ctx->simple == 0)
    {
        log_stdout("");
    }
    
    switch(state_type)
    {
    case REDIS_KEY_NUM:
        log_stdout("cluster has %lld keys", sdata.num_sum);
        break;
    case REDIS_MEMORY:
        log_stdout("cluster used %lld M", sdata.num_sum);
        break;
    case NODES_CLUSTER_STATE:
        if(sdata.all_is_ok)
        {
            log_stdout("all nodes cluster_state is ok");
        }
        break;
    default:
        
        break;
    }
    
    dictReleaseIterator(di);
    
    if(statistics_nodes != NULL)
    {
        statistics_nodes->nelem = 0;
        hiarray_destroy(statistics_nodes);
    }
}

typedef struct config_get_data
{
    int all_is_consistent;
    sds compare_value;
    long long sum;
    int first_flag;
}config_get_data;

static int do_command_with_node(rctContext *ctx, cluster_node *node, 
    sds command, redis_command_type_t cmd_type, void *data)
{
    redisClusterContext *cc = ctx->cc;
    redisContext *c = NULL;
    redisReply *reply = NULL, *sub_reply;
    config_get_data *cgdata = data;
    char format_space[2] = {'\0'};

    if(node->role == REDIS_ROLE_SLAVE)
    {
        format_space[0] = ' ';
    }

    if(cgdata->first_flag)
    {
        cgdata->first_flag = 0;
    }
    else if(ctx->redis_role == RCT_REDIS_ROLE_ALL && 
        node->role == REDIS_ROLE_MASTER
        && ctx->simple == 0)
    {
        log_stdout("");
    }
    
    c = ctx_get_by_node(cc, node);
    if(c == NULL)
    {   
        log_stdout("%s[%s] get connect failed", 
            node_role_name(node), node->addr);
        return RCT_ERROR;
    }

    reply = redisCommand(c, command);

    if(reply == NULL)
    {
        log_stdout("%s[%s] %s failed(reply is NULL)!", 
            node_role_name(node), node->addr, command);
        goto error;
    }
    else if(reply->type == REDIS_REPLY_ERROR)
    {
        log_stdout("%s[%s] %s failed(%s)!", node_role_name(node),
            node->addr, command, reply->str);
        goto error;
    }
    else
    {
        switch(cmd_type)
        {
        case REDIS_COMMAND_FLUSHALL:
        case REDIS_COMMAND_CONFIG_SET:
        case REDIS_COMMAND_CONFIG_REWRITE:
            if(ctx->simple == 0)
            {
                log_stdout("%s%s[%s] %s OK",format_space, 
                    node_role_name(node), node->addr, command);
            }
            break;
        case REDIS_COMMAND_CONFIG_GET:

            if(cgdata == NULL)
            {
                goto error;
            }
            
            if(reply->type != REDIS_REPLY_ARRAY)
            {
                log_stdout("ERR: command [%s] reply type error(want 2, but %d) for node %s.", 
                    command, reply->type, node->addr);
                goto error;
            }

            if(reply->elements == 0)
            {
                log_stdout("ERR: node %s do not support this config [%s].", 
                    node->addr, *(sds*)hiarray_get(&ctx->args, 0));
                goto error;
            }
            else if(reply->elements != 2)
            {
                log_stdout("ERR: command [%s] reply array len error(want 2, but %d) for node %s.", 
                    command, reply->elements, node->addr);
                goto error;
            }

            sub_reply = reply->element[0];
            if(sub_reply == NULL || sub_reply->type != REDIS_REPLY_STRING)
            {
                log_stdout("ERR: command [%s] reply(config name) type error(want 1, but %d) for node %s.", 
                    command, sub_reply->type, node->addr);
                goto error;
            }

            if(strcmp(sub_reply->str, *(sds*)hiarray_get(&ctx->args, 0)))
            {
                log_stdout("ERR: command [%s] reply config name is not %s for node %s.", 
                    command, *(sds*)hiarray_get(&ctx->args, 0), node->addr);
                goto error;
            }

            sub_reply = reply->element[1];
            if(sub_reply == NULL || sub_reply->type != REDIS_REPLY_STRING)
            {
                log_stdout("ERR: command [%s] reply(config value) type error(want 1, but %d) for node %s.", 
                    command, sub_reply->type, node->addr);
                goto error;
            }

            if(strcmp("maxmemory", *(sds*)hiarray_get(&ctx->args, 0)) == 0)
            {
                long long memory_num = rct_atoll(reply->element[1]->str, reply->element[1]->len);
                if(ctx->simple == 0)
                {
                    log_stdout("%s%s[%s] config %s is %s (%lldMB)", format_space, node_role_name(node), 
                        node->addr, reply->element[0]->str, reply->element[1]->str, 
                        memory_num/(1024*1024));
                }
                cgdata->sum += memory_num;
            }
            else if(ctx->simple == 0)
            {
                log_stdout("%s%s[%s] config %s is %s", format_space, node_role_name(node), 
                    node->addr, reply->element[0]->str, reply->element[1]->str);
            }
            
            if(cgdata->all_is_consistent == 0)
            {
                if(cgdata->compare_value != NULL)
                {
                    sdsfree(cgdata->compare_value);
                    cgdata->compare_value = NULL;
                }
            }
            else
            {
                if(cgdata->compare_value == NULL)
                {
                    cgdata->compare_value = sdsnewlen(sub_reply->str, sub_reply->len);
                }
                else
                {
                    if(strcmp(cgdata->compare_value, sub_reply->str))
                    {
                        cgdata->all_is_consistent = 0;
                    }
                    else
                    {
                        cgdata->compare_value = sdscpylen(cgdata->compare_value, 
                            sub_reply->str, sub_reply->len);
                    }
                }
            }
            
            break;
        
        default:

            break;
        }
    }


    if(reply != NULL)
    {
        freeReplyObject(reply);
    }

    return RCT_OK;
    
error:

    if(reply != NULL)
    {
        freeReplyObject(reply);
    }

    return RCT_ERROR;
}

void do_command_node_by_node(rctContext *ctx, int type)
{
    int ret;
    redisClusterContext *cc = ctx->cc;
    redis_command_type_t cmd_type = type;
    sds command = NULL;
    dictIterator *di = NULL;
    dictEntry *de;
    listIter *it;
    listNode *ln;
    dict *nodes;
    hilist *slaves;
    int nodes_count;
    struct cluster_node *master, *slave;
    redisContext *c = NULL;
    redisReply *reply = NULL, *sub_reply;
    int all_is_ok = 1;
    config_get_data data;
    int avoid_slave = 0;
    
    if(cc == NULL)
    {
        return;
    }
    
    nodes = cc->nodes;
    if(nodes == NULL)
    {
        return;
    }

    nodes_count = dictSize(nodes);
    if(nodes_count <= 0)
    {
        return;
    }

    data.first_flag = 1;

    switch(cmd_type)
    {
    case REDIS_COMMAND_FLUSHALL:
        avoid_slave = 1;
        command = sdsnew("flushall");
        if(command == NULL)
        {
            log_stdout("ERR: out of memory.");
            goto done;
        }
        break;
    case REDIS_COMMAND_CONFIG_GET:
        command = sdsnew("config get ");
        if(command == NULL)
        {
            log_stdout("ERR: out of memory.");
            goto done;
        }

        command = sdscatsds(command, *(sds*)hiarray_get(&ctx->args, 0));
        if(command == NULL)
        {
            log_stdout("ERR: out of memory.");
            goto done;
        }

        data.all_is_consistent = 1;
        data.compare_value = NULL;
        data.sum = 0;
        
        break;
    case REDIS_COMMAND_CONFIG_SET:
        command = sdsnew("config set ");
        if(command == NULL)
        {
            log_stdout("ERR: out of memory.");
            goto done;
        }

        command = sdscatsds(command, *(sds*)hiarray_get(&ctx->args, 0));
        if(command == NULL)
        {
            log_stdout("ERR: out of memory.");
            goto done;
        }

        command = sdscat(command, " ");
        if(command == NULL)
        {
            log_stdout("ERR: out of memory.");
            goto done;
        }

        command = sdscatsds(command, *(sds*)hiarray_get(&ctx->args, 1));
        if(command == NULL)
        {
            log_stdout("ERR: out of memory.");
            goto done;
        }
        
        break;
    case REDIS_COMMAND_CONFIG_REWRITE:
        command = sdsnew("config rewrite");
        if(command == NULL)
        {
            log_stdout("ERR: out of memory.");
            goto done;
        }
        
        break;
    default:
        
        break;
    }
    
    di = dictGetIterator(nodes);
    
    while((de = dictNext(di)) != NULL) {
        master = dictGetEntryVal(de);

        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_MASTER)
        {
            ret = do_command_with_node(ctx, master, command, cmd_type, &data);
            if(ret != RCT_OK)
            {
                all_is_ok = 0;
            }
        }

        if(!avoid_slave && (ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE))
        {
            slaves = master->slaves;
            if(slaves == NULL)
            {
                continue;
            }
            
            it = listGetIterator(slaves, AL_START_HEAD);
            while((ln = listNext(it)) != NULL)
            {
                slave = listNodeValue(ln);
                
                ret = do_command_with_node(ctx, slave, command, cmd_type, &data);
                if(ret != RCT_OK)
                {
                    all_is_ok = 0;
                }
            }

            listReleaseIterator(it);
        }
    }
    
    switch(cmd_type)
    {
    case REDIS_COMMAND_FLUSHALL:
    case REDIS_COMMAND_CONFIG_SET:
    case REDIS_COMMAND_CONFIG_REWRITE:
        if(all_is_ok)
        {
            log_stdout("\nOK");
        }
        else
        {
            log_stdout("\nOthers is OK");
        }
        
        break;
    case REDIS_COMMAND_CONFIG_GET:

        if(ctx->simple == 0)
        {
            log_stdout("");
        }
        
        if(all_is_ok)
        {
            if(data.all_is_consistent)
            {
                if(strcmp("maxmemory", *(sds*)hiarray_get(&ctx->args, 0)) == 0)
                {
                    log_stdout("All nodes config %s are Consistent: %s (%lldMB)",
                        *(sds*)hiarray_get(&ctx->args, 0), data.compare_value,
                        rct_atoll(data.compare_value, sdslen(data.compare_value))/(1024*1024));
                }
                else
                {
                    log_stdout("All nodes config %s are Consistent: %s",
                        *(sds*)hiarray_get(&ctx->args, 0), data.compare_value);
                }
            }
            else
            {
                log_stdout("Nodes config are Inconsistent");
            }
        }
        else
        {
            if(data.all_is_consistent)
            {
                if(strcmp("maxmemory", *(sds*)hiarray_get(&ctx->args, 0)) == 0)
                {
                    log_stdout("Other nodes config %s are Consistent: %s (%lldMB)",
                        *(sds*)hiarray_get(&ctx->args, 0), data.compare_value,
                        rct_atoll(data.compare_value, sdslen(data.compare_value))/(1024*1024));
                }
                else
                {
                    log_stdout("Other nodes config %s are Consistent: %s",
                        *(sds*)hiarray_get(&ctx->args, 0), data.compare_value);
                }
            }
            else
            {
                log_stdout("Other nodes config are Inconsistent");
            }
        }
        
        if(strcmp("maxmemory", *(sds*)hiarray_get(&ctx->args, 0)) == 0)
        {
            log_stdout("cluster total maxmemory: %lld (%lldMB)", data.sum, data.sum/(1024*1024));
        }
        break;
    
    default:

        break;
    }
    
done:
    
    if(reply != NULL)
    {
        freeReplyObject(reply);
    }

    if(command != NULL)
    {
        sdsfree(command);
    }

    if(di != NULL)
    {
        dictReleaseIterator(di);
    }

    if(cmd_type == REDIS_COMMAND_CONFIG_GET 
        && data.compare_value != NULL)
    {
        sdsfree(data.compare_value);
    }
}

void do_command(rctContext *ctx, int type)
{
    redisClusterContext *cc = ctx->cc;
    int command_type = type;
    int start_key = 0;
    int end_key = 0;
    redisReply *reply = NULL;
    int i;
    int null_count=0,ok_count=0,equal_count=0,error_count=0,reply_null_count=0;
    
    if(cc == NULL)
    {
        return;
    }
    
    for(i = start_key; i < end_key; i ++)
    {
        switch(command_type)
        {
        case REDIS_COMMAND_GET:
            reply = redisClusterCommand(cc, "get %d", i);
            break;
        case REDIS_COMMAND_SET:
            reply = redisClusterCommand(cc, "set %d %d", i, i);
            break;
        default:
            goto done;
            break;
        }

        if(reply == NULL)
        {
            log_stdout("reply is null[%s]", cc->errstr);
            reply_null_count ++;
            continue;
        }
        
        switch(reply->type)
        {
        case REDIS_REPLY_STRING:
            
            if(rct_atoi(reply->str) == i)
            {
                equal_count ++;
            }
            break;
        case REDIS_REPLY_ARRAY:
        
            break;
        case REDIS_REPLY_INTEGER:
        
            break;
        case REDIS_REPLY_NIL:
            null_count ++;
            
            break;
        case REDIS_REPLY_STATUS:
        
            if(strcmp(reply->str, "OK") == 0)
            {
                ok_count ++;
            }
            break;
        case REDIS_REPLY_ERROR:
            error_count ++;
            log_stdout("%s", reply->str);
            break;
        default:
            break;
        }
        freeReplyObject(reply);
        reply = NULL;
    }
    
    log_stdout("null_count: %d", null_count);
    log_stdout("ok_count: %d", ok_count);
    log_stdout("equal_count: %d", equal_count);
    log_stdout("error_count: %d", error_count);
    log_stdout("reply_null_count: %d", reply_null_count);

done:
    
    if(reply != NULL)
    {
        freeReplyObject(reply);
        reply = NULL;   
    }
}

int async_command_init(async_command *acmd, rctContext *ctx, char *addrs, int flags)
{
    int ret;

    if(acmd == NULL){
        return RCT_ERROR;
    }

    acmd->ctx = NULL;
    acmd->loop = NULL;
    acmd->acc = NULL;
    acmd->nodes = NULL;
    acmd->command = NULL;
    acmd->parameters = NULL;
    acmd->callback = NULL;
    acmd->role = RCT_REDIS_ROLE_NULL;
    acmd->nodes_count = 0;
    acmd->finished_count = 0;
    hiarray_null(&acmd->results);
    acmd->step = 0;
    acmd->black_nodes = NULL;

    acmd->ctx = ctx;
    
    acmd->acc = redisClusterAsyncConnect(addrs, HIRCLUSTER_FLAG_ADD_SLAVE);
    if(acmd->acc == NULL){
        log_error("Connect to %s failed.", addrs);
        goto error;
    }

    acmd->nodes = acmd->acc->cc->nodes;

    acmd->loop = aeCreateEventLoop(1000);
    if(acmd->loop == NULL){
        log_error("Create ae event loop failed.");
        goto error;
    }

    redisClusterAeAttach(acmd->loop, acmd->acc);

    ret = hiarray_init(&acmd->results, dictSize(acmd->nodes), sizeof(struct cluster_node *));
    if(ret != HI_OK){
        log_error("Init result array failed.");
        goto error;
    }

    acmd->black_nodes = listCreate();
    if(acmd->black_nodes == NULL){
        log_error("Create black_nodes list failed: out of memory.");
        goto error;
    }

    return RCT_OK;

error:

    async_command_deinit(acmd);
    return RCT_ERROR;
}

void async_command_deinit(async_command *acmd)
{
    struct cluster_node **node;
    redisReply *reply;

    if(acmd == NULL){
        return;
    }

    if(acmd->results.nelem > 0){
        while(hiarray_n(&acmd->results) > 0){
            node = hiarray_pop(&acmd->results);
            
            reply = (*node)->data;
            (*node)->data = NULL;
            if(reply != NULL){
                freeReplyObject(reply);
            }
        }

        hiarray_deinit(&acmd->results);
    }

    if(acmd->acc != NULL){
        redisClusterAsyncFree(acmd->acc);
        acmd->acc = NULL;
    }

    acmd->ctx = NULL;
    acmd->nodes = NULL;

    if(acmd->command != NULL){
        sdsfree(acmd->command);
        acmd->command = NULL;
    }

    if(acmd->parameters != NULL){
        sds *str;
        while(hiarray_n(acmd->parameters) > 0){
            str = hiarray_pop(acmd->parameters);
            sdsfree(*str);
        }

        hiarray_destroy(acmd->parameters);
        acmd->parameters = NULL;
    }

    if(acmd->loop != NULL){
        aeDeleteEventLoop(acmd->loop);
        acmd->loop = NULL;
    }

    if(acmd->black_nodes != NULL){
        listRelease(acmd->black_nodes);
        acmd->black_nodes = NULL;
    }
    
    acmd->callback = NULL;
    acmd->role = RCT_REDIS_ROLE_NULL;
    acmd->nodes_count = 0;
    acmd->finished_count = 0;
    acmd->step = 0;
}

void async_command_reset(async_command *acmd)
{
    struct cluster_node **node;
    redisReply *reply;

    if(acmd == NULL){
        return;
    }

    if(acmd->results.nelem > 0){
        while(hiarray_n(&acmd->results) > 0){
            node = hiarray_pop(&acmd->results);
            
            reply = (*node)->data;
            if(reply != NULL){
                freeReplyObject(reply);
                (*node)->data = NULL;
            }
        }
    }

    if(acmd->command != NULL){
        sdsfree(acmd->command);
        acmd->command = NULL;
    }

    if(acmd->parameters != NULL){
        sds *str;
        while(hiarray_n(acmd->parameters) > 0){
            str = hiarray_pop(acmd->parameters);
            sdsfree(*str);
        }

        hiarray_destroy(acmd->parameters);
        acmd->parameters = NULL;
    }

    if(acmd->callback != NULL){
        acmd->callback = NULL;
        acmd->nodes_count = 0;
        acmd->finished_count = 0;
    }

    acmd->role = RCT_REDIS_ROLE_NULL;
}

static void async_callback_func(redisAsyncContext *c, void *r, void *privdata) {
    int ret;
    redisReply *reply = r;
    async_callback_data *data = privdata;
    async_command *acmd = data->acmd;
    struct cluster_node *node = data->node, **result;
    rctContext *ctx = acmd->ctx;

    acmd->finished_count ++;
    
    if (reply == NULL) node->acon = NULL;

    if(acmd->callback != NULL){
        if(reply == NULL){
            node->data = NULL;
        }else{
            node->data = redis_reply_clone(reply);
            if(node->data == NULL){
                log_error("Redis reply clone failed.");
            }
        }
        
        if(acmd->role == RCT_REDIS_ROLE_ALL || 
            acmd->role == RCT_REDIS_ROLE_MASTER){
            if(node->role == REDIS_ROLE_SLAVE){
                goto done;
            }
        }else if(acmd->role == RCT_REDIS_ROLE_SLAVE){
            if(node->role != REDIS_ROLE_SLAVE){
                goto done;
            }
        }else{
            log_error("Node %s role is error.", node->addr);
            goto done;
        }
        
        result = hiarray_push(&acmd->results);
        *result = node;
    }

done:

    if(acmd->finished_count >= acmd->nodes_count){
        acmd->step ++;
        
        if(acmd->callback != NULL){
            ret = acmd->callback(acmd);
            if(ret == RCT_AE_STOP){
                aeStop(acmd->loop);
            }
        }else{
            aeStop(acmd->loop);
        }
    }

    rct_free(data);
}

static int do_command_one_node_async(async_command *acmd, struct cluster_node *node)
{
    async_callback_data *data;
    redisAsyncContext *ac;
    int ret, i, argc;
    char **argv;
    size_t *argvlen;
    sds *str;
        
    if (acmd == NULL || node == NULL) {
        return RCT_ERROR;
    }

    data = rct_alloc(sizeof(*data));
    if (data == NULL) {
        return RCT_ENOMEM;
    }

    data->acmd = acmd;
    data->node = node;
    ac = actx_get_by_node(acmd->acc, node);
    if (acmd->parameters == NULL) {
        ret = redisAsyncCommand(ac, async_callback_func, data, acmd->command);
    } else {
        argc = 1 + hiarray_n(acmd->parameters);
        argv = rct_alloc(argc * sizeof(*argv));
        argvlen = rct_alloc(argc * sizeof(*argvlen));

        argv[0] = acmd->command;
        argvlen[0] = sdslen(acmd->command);
        for (i = 1; i < argc; i ++) {
            str = hiarray_get(acmd->parameters, i - 1);
            argv[i] = *str;
            argvlen[i] = sdslen(*str);
        }
        
        ret = redisAsyncCommandArgv(ac, async_callback_func, data, argc, (const char **)argv, argvlen);

        rct_free(argv);
        rct_free(argvlen);
    }

    if (ret != REDIS_OK) {
        if(ac->err) log_error("err: %s", ac->errstr);
        return RCT_ERROR;
    }
    
    acmd->nodes_count ++;
    
    return RCT_OK;
}

static int do_command_all_nodes_async(async_command *acmd)
{
    int ret;
    dictIterator *di = NULL;
    dictEntry *de;
    listIter *it = NULL;
    listNode *ln;
    dict *nodes;
    hilist *slaves;
    struct cluster_node *master, *slave;

    if(acmd == NULL)
    {
        return RCT_ERROR;
    }
    
    nodes = acmd->nodes;
    if(nodes == NULL)
    {
        return RCT_ERROR;
    }

    di = dictGetIterator(nodes);
    
    while((de = dictNext(di)) != NULL) {
        master = dictGetEntryVal(de);

        if(acmd->role != RCT_REDIS_ROLE_SLAVE){
            ret = do_command_one_node_async(acmd, master);
            if(ret != RCT_OK){
                
                goto error;
            }
        }

        if(acmd->role != RCT_REDIS_ROLE_MASTER){
            slaves = master->slaves;
            if(slaves == NULL)
            {
                continue;
            }
            
            it = listGetIterator(slaves, AL_START_HEAD);
            while((ln = listNext(it)) != NULL)
            {
                slave = listNodeValue(ln);
                ret = do_command_one_node_async(acmd, slave);
                if(ret != RCT_OK){
                    goto error;
                }
            }

            listReleaseIterator(it);
        }
    }

    dictReleaseIterator(di);
    
    return RCT_OK;

error:

    if(di != NULL){
        dictReleaseIterator(di);
    }

    if(it != NULL){
        listReleaseIterator(it);
    }

    return RCT_ERROR;
}

int cluster_async_call(rctContext *ctx, 
    char *command, struct hiarray *parameters, 
    int role, async_callback_reply *callback)
{
    int ret;
    async_command *acmd;

    if(role != RCT_REDIS_ROLE_ALL &&
        role != RCT_REDIS_ROLE_MASTER &&
        role != RCT_REDIS_ROLE_SLAVE){
        log_error("Call %s for target redis role %s is error.",
            role);
        return RCT_ERROR;
    }

    acmd = ctx->acmd;
    if(acmd == NULL){
        acmd = rct_alloc(sizeof(*acmd));
        if(acmd == NULL){
            log_error("Out of memory.");
            return RCT_ENOMEM;
        }

        ret = async_command_init(acmd, ctx, ctx->address, 0);
        if(ret != RCT_OK){
            log_error("Init async_command error.");
            goto error;
        }

        ctx->acmd = acmd;
    }else{
        acmd->callback = callback;
        async_command_reset(acmd);
    }

    acmd->role = role;

    acmd->command = sdsnew(command);
    if(parameters != NULL){
        acmd->parameters = parameters;
    }
    
    acmd->callback = callback;

    ret = do_command_all_nodes_async(acmd);
    if(ret != RCT_OK){
        log_error("Call command \"%s\" error.", acmd->command);
        goto error;
    }

    if(acmd->nodes_count == 0){
        acmd->step ++;

        if(acmd->callback != NULL){
            ret = acmd->callback(acmd);

            if(acmd->step == 1){
                return RCT_OK;
            }else if(ret == RCT_AE_STOP){
                aeStop(acmd->loop);
            }
        }else{
            if(acmd->step == 1){
                return RCT_OK;
            }
            
            aeStop(acmd->loop);
        }
    }

    if(acmd->step == 0){
        aeMain(acmd->loop);
    }
    
    return RCT_OK;

error:
    
    async_command_deinit(acmd);
    rct_free(acmd);
    ctx->acmd = NULL;

    return RCT_ERROR;
}

static int
redis_cluster_addr_cmp(const void *t1, const void *t2)
{
    const cluster_node **s1 = (const cluster_node **)t1;
    const cluster_node **s2 = (const cluster_node **)t2;

    return sdscmp((*s1)->addr, (*s2)->addr);
}

int async_reply_status(async_command *acmd)
{
    int i, all_is_ok;
    redisReply *reply;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    struct cluster_node **node, *slave;
    struct hiarray *results = &acmd->results;
    rctContext *ctx = acmd->ctx;

    hiarray_sort(results, redis_cluster_addr_cmp);

    all_is_ok = 1;
    
    for(i = 0; i < hiarray_n(results); i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;

        if(reply == NULL){
            all_is_ok = 0;
        }else if(reply->type != REDIS_REPLY_STATUS ||
            strcmp(reply->str, "OK") != 0){
            all_is_ok = 0;
        }
        
        if(!ctx->simple){
            log_stdout("%s[%s] %s", 
                (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                (*node)->addr,
                reply?reply->str:"error");
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;
                    if(reply == NULL){
                        all_is_ok = 0;
                    }else if(reply->type != REDIS_REPLY_STATUS ||
                        strcmp(reply->str, "OK") != 0){
                        all_is_ok = 0;
                    }
                    
                    if(!ctx->simple){
                        log_stdout(" slave[%s] %s",
                            slave->addr,
                            reply?reply->str:"error");
                    }
                }

                listReleaseIterator(li);
                li = NULL;
            }

            if(!ctx->simple && ctx->redis_role == RCT_REDIS_ROLE_ALL) log_stdout("");
        }
    }

    log_stdout("");

    if(all_is_ok){
        log_stdout("All nodes \"%s\" are OK", acmd->command);
    }else{
        log_stdout("Some nodes \"%s\" are ERROR", acmd->command);
    }

    return RCT_AE_STOP;
}

int async_reply_string(async_command *acmd)
{
    int i, all_is_ok, all_is_same;
    redisReply *reply;
    char *previous_str;
    int previous_len;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    struct cluster_node **node, *slave;
    struct hiarray *results = &acmd->results;
    rctContext *ctx = acmd->ctx;

    hiarray_sort(results, redis_cluster_addr_cmp);

    all_is_ok = 1;
    all_is_same = 1;
    previous_str = NULL;
    previous_len = 0;
    
    for(i = 0; i < hiarray_n(results); i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;
        log_stdout("reply->type: %d", reply->type);
        if(reply == NULL){
            all_is_ok = 0;
        }else if(reply->type != REDIS_REPLY_STRING){
            all_is_ok = 0;
        }else if(previous_str == NULL){
            previous_str = reply->str;
            previous_len = reply->len;
        }else if(all_is_same){
            if(strncmp(previous_str, reply->str, 
                MIN(previous_len, reply->len)) == 0){
                previous_str = reply->str;
                previous_len = reply->len;
            }else{
                all_is_same = 0;
            }
        }

        if(!ctx->simple){
            if(reply == NULL){
                 log_stdout("%s[%s] is error", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr);
            }else{
                log_stdout("%s[%s] %s", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr,
                    reply?reply->str:"NULL");
            }
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;
                    
                    if(reply == NULL){
                        all_is_ok = 0;
                    }else if(reply->type != REDIS_REPLY_STRING){
                        all_is_ok = 0;
                    }else if(previous_str == NULL){
                        previous_str = reply->str;
                        previous_len = reply->len;
                    }else if(all_is_same){
                        if(strncmp(previous_str, reply->str, 
                            MIN(previous_len, reply->len)) == 0){
                            previous_str = reply->str;
                            previous_len = reply->len;
                        }else{
                            all_is_same = 0;
                        }
                    }

                    if(!ctx->simple){
                        if(reply == NULL){
                            log_stdout(" slave[%s] is error",
                                slave->addr);
                        }else{
                            log_stdout(" slave[%s] %s",
                                slave->addr,
                                reply?reply->str:"error");
                        }
                    }
                }

                listReleaseIterator(li);
                li = NULL;
            }
            
            if(!ctx->simple && ctx->redis_role == RCT_REDIS_ROLE_ALL) log_stdout("");
        }
    }

    log_stdout("");

    if(all_is_ok && all_is_same){
        log_stdout("All nodes \"%s\" are SAME", acmd->command);
    }else if(all_is_ok && !all_is_same){
        log_stdout("Some nodes \"%s\" are DIFFERENT", acmd->command);
    }else if(!all_is_ok && all_is_same){
        log_stdout("Some nodes are error, others \"%s\" are SAME", acmd->command);
    }else{
        log_stdout("Some nodes are error, others \"%s\" are DIFFERENT", acmd->command);
    }

    return RCT_AE_STOP;
}

static sds redis_reply_to_string(redisReply *reply)
{
    int i;
    sds str, substr;

    if(reply == NULL){
        return NULL;
    }

    switch (reply->type)
    {
    case REDIS_REPLY_STRING:
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_ERROR:
        str = sdsnewlen(reply->str, reply->len);
        break;
    case REDIS_REPLY_INTEGER:
        str = sdsfromlonglong(reply->integer);
        break;
    case REDIS_REPLY_NIL:
        str = NULL;
        break;
    case REDIS_REPLY_ARRAY:
        str = sdsempty();
        for(i = 0; i < reply->elements; i ++){
            substr = redis_reply_to_string(reply->element[i]);
            if(substr == NULL){
                continue;
            }
            str = sdscatsds(str, substr);
            if(i < reply->elements - 1){
                str = sdscat(str, " ");
            }
            sdsfree(substr);
        }

        if(sdslen(str) == 0){
            sdsfree(str);
            str = NULL;
        }
        break;
    default:
        str = NULL;
        RCT_NOT_REACHED();
        break;
    }

    return str;
}

int async_reply_display(async_command *acmd)
{
    int i;
    redisReply *reply;
    sds str;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    struct cluster_node **node, *slave;
    struct hiarray *results = &acmd->results;
    rctContext *ctx = acmd->ctx;

    hiarray_sort(results, redis_cluster_addr_cmp);
    
    str = NULL;
    
    for(i = 0; i < hiarray_n(results); i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;

        if(str != NULL){
            sdsfree(str);
        }
        str = redis_reply_to_string(reply);

        if(!ctx->simple){
            if(reply == NULL){
                log_stdout("%s[%s] is error", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr);
            }else{
                log_stdout("%s[%s] %s", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr,
                    str?str:"NULL");
            }
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;

                    if(str != NULL){
                        sdsfree(str);
                    }
                    str = redis_reply_to_string(reply);

                    if(!ctx->simple){
                        if(reply == NULL){
                            log_stdout(" slave[%s] is error",
                                slave->addr);
                        }else{
                            log_stdout(" slave[%s] %s",
                                slave->addr,
                                str?str:"NULL");
                        }
                    }
                }

                listReleaseIterator(li);
                li = NULL;
            }
            
            if(!ctx->simple && ctx->redis_role == RCT_REDIS_ROLE_ALL) log_stdout("");
        }
    }

    if(str != NULL){
        sdsfree(str);
    }

    return RCT_AE_STOP;
}

int async_reply_display_check(async_command *acmd)
{
    int i, all_is_ok, all_is_same;
    redisReply *reply;
    sds pre_str, str;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    struct cluster_node **node, *slave;
    struct hiarray *results = &acmd->results;
    rctContext *ctx = acmd->ctx;

    hiarray_sort(results, redis_cluster_addr_cmp);

    all_is_ok = 1;
    all_is_same = 1;
    pre_str = NULL;
    str = NULL;
    
    for(i = 0; i < hiarray_n(results); i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;

        if(str != NULL){
            sdsfree(str);
        }
        
        str = redis_reply_to_string(reply);

        if(!ctx->simple){
            if(reply == NULL){
                log_stdout("%s[%s] is error", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr);
            }else{
                log_stdout("%s[%s] %s", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr,
                    str?str:"NULL");
            }
        }
        
        if(str == NULL){
            all_is_ok = 0;
        }else{
            if(pre_str != NULL){
                if(sdscmp(pre_str, str) != 0){
                    all_is_same = 0;
                }
                sdsfree(pre_str);
            }
            pre_str = str;
            str = NULL;
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;

                    if(str != NULL){
                        sdsfree(str);
                    }
                    
                    str = redis_reply_to_string(reply);

                    if(!ctx->simple){
                        if(reply == NULL){
                            log_stdout(" slave[%s] is error",
                                slave->addr);
                        }else{
                            log_stdout(" slave[%s] %s",
                            slave->addr,
                            str?str:"NULL");
                        }
                    }
                    
                    if(str == NULL){
                        all_is_ok = 0;
                    }else{
                        if(pre_str != NULL){
                            if(sdscmp(pre_str, str) != 0){
                                all_is_same = 0;
                            }
                            sdsfree(pre_str);
                        }
                        pre_str = str;
                        str = NULL;
                    }
                }

                listReleaseIterator(li);
                li = NULL;
            }
            
            if(!ctx->simple && ctx->redis_role == RCT_REDIS_ROLE_ALL) log_stdout("");
        }
    }

    log_stdout("");

    if(all_is_ok && all_is_same){
        log_stdout("All nodes \"%s\" are SAME: %s", 
            acmd->command, pre_str?pre_str:"NULL");
    }else if(all_is_ok && !all_is_same){
        log_stdout("Some nodes \"%s\" are DIFFERENT", acmd->command);
    }else if(!all_is_ok && all_is_same){
        log_stdout("Some nodes are error, others \"%s\" are SAME: %s", 
            acmd->command, pre_str?pre_str:"NULL");
    }else{
        log_stdout("Some nodes are error, others \"%s\" are DIFFERENT", acmd->command);
    }

    if(pre_str != NULL){
        sdsfree(pre_str);
    }

    if(str != NULL){
        sdsfree(str);
    }

    return RCT_AE_STOP;
}

int async_reply_maxmemory(async_command *acmd)
{
    int i, all_is_ok, all_is_same;
    char *pre_str, *str;
    redisReply *reply, *subreply;
    long long total_memory, memory;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    struct cluster_node **node, *slave;
    struct hiarray *results = &acmd->results;
    rctContext *ctx = acmd->ctx;

    hiarray_sort(results, redis_cluster_addr_cmp);

    all_is_ok = 1;
    all_is_same = 1;
    total_memory = 0;
    pre_str = NULL;
    str = NULL;
    
    for(i = 0; i < hiarray_n(results); i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;
        
        if(reply == NULL || reply->type != REDIS_REPLY_ARRAY 
            || reply->elements != 2){
            str = NULL;
        }else{
            str = reply->element[1]->str;
            if(str != NULL){
                memory = rct_atoll(str, strlen(str));
                total_memory += memory;
            }
        }

        if(!ctx->simple){
            
            if(reply == NULL){
                log_stdout("%s[%s] is error",
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr);
            }else{
                log_stdout("%s[%s] maxmemory: %s (%lld MB)", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr,
                    str?str:"NULL", str?memory/(1024*1024):0);
            }
        }
        
        if(str == NULL){
            all_is_ok = 0;
        }else{
            if(pre_str != NULL){
                if(strcmp(pre_str, str) != 0){
                    all_is_same = 0;
                }
            }
            pre_str = str;
            str = NULL;
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;

                    if(reply == NULL || reply->type != REDIS_REPLY_ARRAY || 
                        reply->elements != 2){
                        str = NULL;
                    }else{
                        str = reply->element[1]->str;
                        if(str != NULL){
                            memory = rct_atoll(str, strlen(str));
                            total_memory += memory;
                        }
                    }

                    if(!ctx->simple){
                        if(reply == NULL){
                            log_stdout(" slave[%s] is error",
                                slave->addr);
                        }else{
                            log_stdout(" slave[%s] maxmemory: %s (%lld MB)",
                                slave->addr,
                                str?str:"NULL", str?memory/(1024*1024):0);
                        }
                    }
                    
                    if(str == NULL){
                        all_is_ok = 0;
                    }else{
                        if(pre_str != NULL){
                            if(strcmp(pre_str, str) != 0){
                                all_is_same = 0;
                            }
                        }
                        pre_str = str;
                        str = NULL;
                    }
                }

                listReleaseIterator(li);
                li = NULL;
            }
            
            if(!ctx->simple && ctx->redis_role == RCT_REDIS_ROLE_ALL) log_stdout("");
        }
    }

    log_stdout("");

    if(all_is_ok && all_is_same){
        log_stdout("All nodes \"maxmemory\" are SAME: %s (%lld MB)", 
            pre_str?pre_str:"NULL", 
            pre_str?rct_atoll(pre_str, strlen(pre_str))/(1024*1024):0);
    }else if(all_is_ok && !all_is_same){
        log_stdout("Some nodes \"maxmemory\" are DIFFERENT");
    }else if(!all_is_ok && all_is_same){
        log_stdout("Some nodes are error, others \"maxmemory\" are SAME: %s (%lld MB)", 
            pre_str?pre_str:"NULL", 
            pre_str?rct_atoll(pre_str, strlen(pre_str))/(1024*1024):0);
    }else{
        log_stdout("Some nodes are error, others \"maxmemory\" are DIFFERENT");
    }

    log_stdout("Cluster total maxmemory: %lld (%lld MB)",
        total_memory, total_memory/(1024*1024));

    return RCT_AE_STOP;
}

static sds redis_reply_info_get_value(char *str, int len, sds key)
{
    sds value;
    int value_len;

    if(str == NULL || len <= 0 ||
        key == NULL){
        return NULL;
    }

    value = NULL;
    
    while(len > sdslen(key)){
        //log_stdout("str: %s", str);
        if(strncmp(str, key, sdslen(key))){
            while(*str != '\n'){
                str ++;
                len --;
            }

            if(len > 0){
                str ++;
                len --;
            }
        }else{
            if(*(str+sdslen(key)) != ':'){
                log_error("Error: reply format for info is error: "
                    "there must follow ':' for %s", key);
                value = NULL;
                break;
            }

            str += sdslen(key) + 1;
            value_len = 0;
            while(*(str+value_len) != '\r'){
                value_len ++;
            }

            if(value_len == 0){
                log_error("Error: reply format for info is error: "
                    "value length can not be zero for %s",key);
                break;
            }

            value = sdsnewlen(str, value_len);
            break;
        }
    }

    return value;
}

int async_reply_info_memory(async_command *acmd)
{
    int i, all_is_ok;
    sds key, value;
    redisReply *reply, *subreply;
    long long total_memory, memory;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    struct cluster_node **node, *slave;
    struct hiarray *results = &acmd->results;
    rctContext *ctx = acmd->ctx;

    if(hiarray_n(&ctx->args) != 1){
        log_error("Error: args for % is not 1", acmd->command);
        return RCT_AE_STOP;
    }

    key = *(sds*)hiarray_get(&ctx->args, 0);

    hiarray_sort(results, redis_cluster_addr_cmp);

    all_is_ok = 1;
    total_memory = 0;
    value = NULL;
    
    for(i = 0; i < hiarray_n(results); i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;

        if(value != NULL){
            sdsfree(value);
            value = NULL;
        }
        
        if(reply == NULL || reply->type != REDIS_REPLY_STRING){
            value = NULL;
        }else{
            value = redis_reply_info_get_value(reply->str, reply->len, key);
            if(value != NULL){
                memory = rct_atoll(value, sdslen(value));
                total_memory += memory;
            }
        }

        if(value == NULL){
            all_is_ok = 0;
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;

                    if(value != NULL){
                        sdsfree(value);
                        value = NULL;
                    }
                    
                    if(reply == NULL || reply->type != REDIS_REPLY_STRING){
                        value = NULL;
                    }else{
                        value = redis_reply_info_get_value(reply->str, reply->len, key);
                        if(value != NULL){
                            memory = rct_atoll(value, sdslen(value));
                            total_memory += memory;
                        }
                    }

                    if(value == NULL){
                        all_is_ok = 0;
                    }
                }

                listReleaseIterator(li);
                li = NULL;
            }
        }
    }

    for(i = 0; i < hiarray_n(results) && total_memory > 0; i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;

        if(value != NULL){
            sdsfree(value);
            value = NULL;
        }
        
        if(reply == NULL || reply->type != REDIS_REPLY_STRING){
            value = NULL;
        }else{
            value = redis_reply_info_get_value(reply->str, reply->len, key);
            if(value != NULL){
                memory = rct_atoll(value, sdslen(value));
            }
        }
        
        if(!ctx->simple){
            if(reply == NULL){
                log_stdout("%s[%s] is error", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr);
            }else{
                log_stdout("%s[%s] %s: %s (%lld MB %.2f%%)", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr, key,
                    value?value:"NULL", value?memory/(1024*1024):0,
                    ((float)memory/(float)total_memory)*100);
            }
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;

                    if(value != NULL){
                        sdsfree(value);
                        value = NULL;
                    }
                    
                    if(reply == NULL || reply->type != REDIS_REPLY_STRING){
                        value = NULL;
                    }else{
                        value = redis_reply_info_get_value(reply->str, reply->len, key);
                        if(value != NULL){
                            memory = rct_atoll(value, sdslen(value));
                        }
                    }

                    if(!ctx->simple){
                        if(reply == NULL){
                            log_stdout(" slave[%s] is error", 
                                (*node)->addr);
                        }else{
                            log_stdout(" slave[%s] %s: %s (%lld MB %.2f%%)",
                                slave->addr,key,
                                value?value:"NULL", value?memory/(1024*1024):0,
                                ((float)memory/(float)total_memory)*100);
                        }
                    }
                    
                }

                listReleaseIterator(li);
                li = NULL;
            }
            
            if(!ctx->simple && ctx->redis_role == RCT_REDIS_ROLE_ALL) log_stdout("");
        }
    }

    log_stdout("");

    if(all_is_ok){
        log_stdout("Cluster total \"%s\" : %lld (%lld MB)", 
            key, total_memory, total_memory/(1024*1024));
    }else{
        log_stdout("Some nodes are error, other nodes total \"%s\" : %lld (%lld MB)", 
            key, total_memory, total_memory/(1024*1024));
    }

    return RCT_AE_STOP;
}

int async_reply_info_keynum(async_command *acmd)
{
    int i, all_is_ok;
    sds key, value;
    char *str_begin, *str_end;
    redisReply *reply, *subreply;
    long long total_keynum, keynum;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    struct cluster_node **node, *slave;
    struct hiarray *results = &acmd->results;
    rctContext *ctx = acmd->ctx;

    if(hiarray_n(&ctx->args) != 1){
        log_error("Error: args for % is not 1", acmd->command);
        return RCT_AE_STOP;
    }

    key = *(sds*)hiarray_get(&ctx->args, 0);

    hiarray_sort(results, redis_cluster_addr_cmp);

    all_is_ok = 1;
    total_keynum = 0;
    value = NULL;
    
    for(i = 0; i < hiarray_n(results); i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;

        if(value != NULL){
            sdsfree(value);
            value = NULL;
        }
        
        if(reply == NULL || reply->type != REDIS_REPLY_STRING){
            all_is_ok = 0;
        }else{
            value = redis_reply_info_get_value(reply->str, reply->len, key);
            if(value != NULL){
                str_begin = strchr(value, '=');
                str_end = strchr(value, ',');
                if(str_begin == NULL || str_end == NULL){
                    all_is_ok = 0;
                }else{
                    str_begin ++;
                    str_end--;
                    if(str_end < str_begin){
                        all_is_ok = 0;
                    }else{
                        keynum = rct_atoll(str_begin, (int)(str_end-str_begin) + 1);
                        total_keynum += keynum;
                    }
                }
            }
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;

                    if(value != NULL){
                        sdsfree(value);
                        value = NULL;
                    }
                    
                    if(reply == NULL || reply->type != REDIS_REPLY_STRING){
                        all_is_ok = 0;
                    }else{
                        value = redis_reply_info_get_value(reply->str, reply->len, key);
                        if(value != NULL){
                            str_begin = strchr(value, '=');
                            str_end = strchr(value, ',');
                            if(str_begin == NULL || str_end == NULL){
                                all_is_ok = 0;
                            }else{
                                str_begin ++;
                                str_end--;
                                if(str_end < str_begin){
                                    all_is_ok = 0;
                                }else{
                                    keynum = rct_atoll(str_begin, (int)(str_end-str_begin) + 1);
                                    total_keynum += keynum;
                                }
                            }
                        }
                    }
                }

                listReleaseIterator(li);
                li = NULL;
            }
        }
    }

    for(i = 0; i < hiarray_n(results) && total_keynum > 0; i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;

        if(value != NULL){
            sdsfree(value);
            value = NULL;
        }

        keynum = 0;
        
        if(reply == NULL || reply->type != REDIS_REPLY_STRING){
            keynum = -1;
        }else{
            value = redis_reply_info_get_value(reply->str, reply->len, key);
            if(value != NULL){
                str_begin = strchr(value, '=');
                str_end = strchr(value, ',');
                if(str_begin == NULL || str_end == NULL){
                    keynum = -1;
                }else{
                    str_begin ++;
                    str_end--;
                    if(str_end < str_begin){
                        keynum = -1;
                    }else{
                        keynum = rct_atoll(str_begin, (int)(str_end-str_begin) + 1);
                    }
                }
            }
        }
        
        if(!ctx->simple){
            if(keynum == -1){
                log_stdout("%s[%s] is error", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr);
            }else{
                log_stdout("%s[%s] has %lld keys %.2f%%", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr, keynum,
                    ((float)keynum/(float)total_keynum)*100);
            }
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;

                    if(value != NULL){
                        sdsfree(value);
                        value = NULL;
                    }

                    keynum = 0;
                    
                    if(reply == NULL || reply->type != REDIS_REPLY_STRING){
                        keynum = -1;
                    }else{
                        value = redis_reply_info_get_value(reply->str, reply->len, key);
                        if(value != NULL){
                            str_begin = strchr(value, '=');
                            str_end = strchr(value, ',');
                            if(str_begin == NULL || str_end == NULL){
                                keynum = -1;
                            }else{
                                str_begin ++;
                                str_end--;
                                if(str_end < str_begin){
                                    keynum = -1;
                                }else{
                                    keynum = rct_atoll(str_begin, (int)(str_end-str_begin) + 1);
                                }
                            }
                        }
                    }

                    if(!ctx->simple){
                        if(keynum == -1){
                            log_stdout(" slave[%s] is error",
                                slave->addr);
                        }else{
                            log_stdout(" slave[%s] has %lld keys %.2f%%",
                                slave->addr,keynum,
                                ((float)keynum/(float)total_keynum)*100);
                        }
                    }
                    
                }

                listReleaseIterator(li);
                li = NULL;
            }
            
            if(!ctx->simple && ctx->redis_role == RCT_REDIS_ROLE_ALL) log_stdout("");
        }
    }

    log_stdout("");

    if(all_is_ok){
        log_stdout("Cluster has %lld keys", 
            total_keynum);
    }else{
        log_stdout("Some nodes are error, other nodes has %lld keys", 
            total_keynum);
    }

    return RCT_AE_STOP;
}

int async_reply_info_display(async_command *acmd)
{
    int i;
    redisReply *reply;
    char *str;
    int len;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    struct cluster_node **node, *slave;
    struct hiarray *results = &acmd->results;
    rctContext *ctx = acmd->ctx;
    sds key, value;

    if(hiarray_n(&ctx->args) != 1){
        log_error("Error: args for % is not 1", acmd->command);
        return RCT_AE_STOP;
    }

    key = *(sds*)hiarray_get(&ctx->args, 0);

    hiarray_sort(results, redis_cluster_addr_cmp);

    value = NULL;
    
    for(i = 0; i < hiarray_n(results); i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;

        if(reply == NULL || reply->type != REDIS_REPLY_STRING){
            str = NULL;
            len = 0;
        }else{
            str = reply->str;
            len = reply->len;
        }

        if(value != NULL){
            sdsfree(value);
            value = NULL;
        }

        value = redis_reply_info_get_value(str, len, key);
        
        if(!ctx->simple){
            if(reply == NULL){
                log_stdout("%s[%s] is error", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr);
            }else{
                log_stdout("%s[%s] %s: %s", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr,
                    key,
                    value?value:"NULL");
            }
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;

                    if(reply == NULL || reply->type != REDIS_REPLY_STRING){
                        str = NULL;
                        len = 0;
                    }else{
                        str = reply->str;
                        len = reply->len;
                    }

                    if(value != NULL){
                        sdsfree(value);
                        value = NULL;
                    }

                    value = redis_reply_info_get_value(str, len, key);
                    
                    if(!ctx->simple){
                        if(reply == NULL){
                            log_stdout(" slave[%s] is error",
                                slave->addr);
                        }else{
                            log_stdout(" slave[%s] %s: %s",
                                slave->addr,key,
                                value?value:"NULL");
                        }
                    }
                }

                listReleaseIterator(li);
                li = NULL;
            }

            if(!ctx->simple && ctx->redis_role == RCT_REDIS_ROLE_ALL) log_stdout("");
        }
    }

    log_stdout("");

    if(value != NULL){
        sdsfree(value);
    }

    return RCT_AE_STOP;
}

int async_reply_info_display_check(async_command *acmd)
{
    int i, all_is_ok, all_is_same;
    redisReply *reply;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    struct cluster_node **node, *slave;
    struct hiarray *results = &acmd->results;
    rctContext *ctx = acmd->ctx;
    sds key, value, pre_value;

    if(hiarray_n(&ctx->args) != 1){
        log_error("Error: args for % is not 1", acmd->command);
        return RCT_AE_STOP;
    }

    key = *(sds*)hiarray_get(&ctx->args, 0);

    hiarray_sort(results, redis_cluster_addr_cmp);
    
    all_is_ok = 1;
    all_is_same = 1;
    pre_value = NULL;
    value = NULL;
    
    for(i = 0; i < hiarray_n(results); i ++){
        node = hiarray_get(results, i);
        reply = (*node)->data;

        if(value != NULL){
            sdsfree(value);
            value = NULL;
        }

        if(reply == NULL || reply->type != REDIS_REPLY_STRING){
            value = NULL;
        }else{
            value = redis_reply_info_get_value(reply->str, reply->len, key);
        }
    
        if(!ctx->simple){
             if(reply == NULL){
                log_stdout("%s[%s] is error", 
                    (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                    (*node)->addr);
            }else{
                log_stdout("%s[%s] %s: %s", 
                (*node)->role == REDIS_ROLE_MASTER?"master":"slave",
                (*node)->addr,
                key,
                value?value:"NULL");
            }
        }

        if(value == NULL){
            all_is_ok = 0;
        }else{
            if(pre_value != NULL){
                if(all_is_same && sdscmp(pre_value, value)){
                    all_is_same = 0;
                }

                sdsfree(pre_value);
            }

            pre_value = value;
            value = NULL;
        }
        
        if(ctx->redis_role == RCT_REDIS_ROLE_ALL || 
            ctx->redis_role == RCT_REDIS_ROLE_SLAVE){
            slaves = (*node)->slaves;
            if((*node)->role == REDIS_ROLE_MASTER &&
                slaves != NULL){
                li = listGetIterator(slaves, AL_START_HEAD);
                while(ln = listNext(li)){
                    slave = listNodeValue(ln);
                    reply = slave->data;

                    if(value != NULL){
                        sdsfree(value);
                        value = NULL;
                    }

                    if(reply == NULL || reply->type != REDIS_REPLY_STRING){
                        value = NULL;
                    }else{
                        value = redis_reply_info_get_value(reply->str, reply->len, key);
                    }
                    
                    if(!ctx->simple){
                        if(reply == NULL){
                            log_stdout(" slave[%s] is error",
                                slave->addr);
                        }else{
                            log_stdout(" slave[%s] %s: %s",
                                slave->addr,key,
                                value?value:"NULL");
                        }
                    }

                    if(value == NULL){
                        all_is_ok = 0;
                    }else{
                        if(pre_value != NULL){
                            if(all_is_same && sdscmp(pre_value, value)){
                                all_is_same = 0;
                            }

                            sdsfree(pre_value);
                        }

                        pre_value = value;
                        value = NULL;
                    }
                }

                listReleaseIterator(li);
                li = NULL;
            }

            if(!ctx->simple && ctx->redis_role == RCT_REDIS_ROLE_ALL) log_stdout("");
        }
    }

    log_stdout("");

    if(all_is_ok && all_is_same){
        log_stdout("All nodes \"%s\" are SAME: %s", 
            key, pre_value?pre_value:"NULL");
    }else if(all_is_ok && !all_is_same){
        log_stdout("Some nodes \"%s\" are DIFFERENT", key);
    }else if(!all_is_ok && all_is_same){
        log_stdout("Some nodes are error, others \"%s\" are SAME: %s", 
            key, pre_value?pre_value:"NULL");
    }else{
        log_stdout("Some nodes are error, others \"%s\" are DIFFERENT", key);
    }

    if(value != NULL){
        sdsfree(value);
    }

    if(pre_value != NULL){
        sdsfree(pre_value);
    }

    return RCT_AE_STOP;
}

static sds get_cluster_config_signature(dict *nodes)
{
    int ret;
    sds csign, csigns;
    dictIterator *di = NULL;
    dictEntry *de;
    listIter *lit = NULL;
    listNode *ln;
    cluster_node *master;
    cluster_slot *slot;
    uint32_t total_len;
    struct hiarray signatures;
    sds *elem;
    
    if(nodes == NULL){
        return NULL;
    }

    total_len = 0;
    csign = NULL;
    ret = hiarray_init(&signatures, dictSize(nodes), sizeof(sds));
    if(ret != HI_OK){
        log_stdout("Out of memory");
        return NULL;
    }
    
    di = dictGetIterator(nodes);
    
    while((de = dictNext(di))) {
        master = dictGetEntryVal(de);
        csign = sdsdup(master->name);
        if(csign == NULL){
            log_stdout("Out of memory.");
            goto error;
        }
        
        csign = sdscatsds(csign, master->addr);
        if(csign == NULL){
            log_stdout("Out of memory.");
            goto error;
        }
        
        lit = listGetIterator(master->slots, AL_START_HEAD);
        if(lit == NULL){
            log_stdout("Out of memory.");
            goto error;
        }
        
        while((ln = listNext(lit))){
            slot = listNodeValue(ln);
            csign = sdscatfmt(csign, "%u-%u,", 
                slot->start, slot->end);
            if(csign == NULL){
                log_stdout("Out of memory.");
                goto error;
            }
        }
        
        listReleaseIterator(lit);
        lit = NULL;

        total_len += sdslen(csign);
        elem = hiarray_push(&signatures);
        *elem = csign;
    }

    dictReleaseIterator(di);

    hiarray_sort(&signatures, elem_sds_cmp);

    csigns = sdsempty();
    csigns = sdsMakeRoomFor(csigns, total_len + 2);
    
    while(hiarray_n(&signatures)){
        elem = hiarray_pop(&signatures);
        csigns = sdscatsds(csigns, *elem);
        sdsfree(*elem);
    }

    hiarray_deinit(&signatures);

    return csigns;

error:

    sdsfree(csign);

    if(lit){
        listReleaseIterator(lit);
    }

    if(di){
        dictReleaseIterator(di);
    }
    
    return NULL;
}

static int check_cluster_config_signature(dict *nodes_infos, int show)
{
    dict *nodes;
    sds csign_pre = NULL, csign = NULL;
    dictIterator *di_nodes = NULL;
    dictEntry *de_nodes;
    sds addr_pre;

    if(nodes_infos == NULL){
        return RCT_ERROR;
    }

    csign_pre = NULL;
    addr_pre = NULL;
    
    di_nodes = dictGetIterator(nodes_infos);
    while((de_nodes = dictNext(di_nodes))){
        nodes = dictGetEntryVal(de_nodes);

        csign = get_cluster_config_signature(nodes);
        if(csign == NULL){
            goto error;
        }

        if(csign_pre == NULL){
            addr_pre = dictGetEntryKey(de_nodes);
            csign_pre = csign;
            csign = NULL;
        }else if(sdscmp(csign_pre, csign) == 0){
            addr_pre = dictGetEntryKey(de_nodes);
            sdsfree(csign_pre);
            csign_pre = csign;
            csign = NULL;
        }else{
            if(show){
                log_stdout("node[%s] and node[%s] cluster conf are different.", 
                    addr_pre, dictGetEntryKey(de_nodes));
                log_stdout("node[%s]: %s", addr_pre, csign_pre);
                log_stdout("node[%s]: %s", dictGetEntryKey(de_nodes), csign);
            }
            goto error;
        }
    }

    dictReleaseIterator(di_nodes);
    di_nodes = NULL;

    if(csign_pre != NULL){
        sdsfree(csign_pre);
    }

    if(csign != NULL){
        sdsfree(csign);
    }

    return RCT_OK;

error:

    if(di_nodes != NULL){
        dictReleaseIterator(di_nodes);
    }

    if(csign_pre != NULL){
        sdsfree(csign_pre);
    }

    if(csign != NULL){
        sdsfree(csign);
    }

    return RCT_ERROR;
}

static int check_cluster_slot_coverage(dict *nodes)
{
    dictIterator *di = NULL;
    dictEntry *de;
    listIter *lit = NULL;
    listNode *ln;
    cluster_node *master;
    cluster_slot *slot;
    unsigned char slots[REDIS_CLUSTER_SLOTS];
    unsigned char wished[REDIS_CLUSTER_SLOTS];

    if(nodes == NULL){
        return RCT_ERROR;
    }
    
    memset(slots, '0', REDIS_CLUSTER_SLOTS);
    memset(wished, '1', REDIS_CLUSTER_SLOTS);

    di = dictGetIterator(nodes);
    
    while((de = dictNext(di))) {
        master = dictGetEntryVal(de);

        lit = listGetIterator(master->slots, AL_START_HEAD);
        if(lit == NULL){
            log_stdout("Out of memory.");
            goto error;
        }
        
        while((ln = listNext(lit))){
            slot = listNodeValue(ln);
            memset(slots+slot->start, '1', slot->end-slot->start+1);
        }
        
        listReleaseIterator(lit);
        lit = NULL;
    }

    dictReleaseIterator(di);

    if(memcmp(slots, wished, REDIS_CLUSTER_SLOTS)){
        return RCT_ERROR;
    }

    return RCT_OK;

error:

    if(lit){
        listReleaseIterator(lit);
    }

    if(di){
        dictReleaseIterator(di);
    }
    
    return RCT_ERROR;
}

typedef struct open_slot_data{
    uint32_t slot_num;  /* slot number */
    struct hiarray *source; /* this slot import from what nodes */
    struct hiarray *target; /* this slot migrate to what nodes */
}open_slot_data;

static int open_slot_data_init(open_slot_data *osdata)
{
    if(osdata == NULL){
        return RCT_ERROR;
    }
    
    osdata->slot_num = 0;
    osdata->source = NULL;
    osdata->target = NULL;

    return RCT_OK;
}

static open_slot_data *open_slot_data_create(void)
{
    open_slot_data *osdata;

    osdata = rct_alloc(sizeof(*osdata));
    if(osdata == NULL){
        return NULL;
    }

    open_slot_data_init(osdata);

    return osdata;
}

static void open_slot_data_destroy(open_slot_data *osdata)
{
    if(osdata == NULL){
        return;
    }

    if(osdata->source){
        osdata->source->nelem = 0;
        hiarray_destroy(osdata->source);
    }

    if(osdata->target){
        osdata->target->nelem = 0;
        hiarray_destroy(osdata->target);
    }

    rct_free(osdata);
}

static struct hiarray *get_cluster_open_slot(dict *nodes_infos)
{
    uint32_t i, idx;
    dict *nodes;
    dictIterator *di_nodes = NULL;
    dictEntry *de_nodes, *de_node;
    sds addr;
    cluster_node *master, **node_elem;
    copen_slot **oslot;
    struct hiarray *osdatas;
    open_slot_data *osdata, **osdata_elem;
    open_slot_data *slots[REDIS_CLUSTER_SLOTS];

    if(nodes_infos == NULL){
        return NULL;
    }

    osdatas = hiarray_create(1, sizeof(open_slot_data *));
    if(osdatas == NULL){
        return NULL;
    }

    memset(slots, 0, REDIS_CLUSTER_SLOTS*sizeof(open_slot_data *));
    
    di_nodes = dictGetIterator(nodes_infos);
    while((de_nodes = dictNext(di_nodes))){
        addr = dictGetEntryKey(de_nodes);
        nodes = dictGetEntryVal(de_nodes);

         de_node = dictFind(nodes, addr);
         if(de_node == NULL){
            continue;
         }

         master = dictGetEntryVal(de_node);
         if(master->migrating){
            for(i = 0; i < hiarray_n(master->migrating); i ++){
                oslot = hiarray_get(master->migrating, i);
                idx = (*oslot)->slot_num;
                if(slots[idx] == NULL){
                    osdata_elem = hiarray_push(osdatas);
                    if(osdata_elem == NULL){
                        log_stdout("Out of memory");
                        goto error;
                    }
                    
                    osdata = open_slot_data_create();
                    if(osdata == NULL){
                        log_stdout("Out of memory");
                        goto error;
                    }

                    osdata->slot_num = idx;
                    *osdata_elem = osdata;
                    slots[idx] = osdata;
                }

                osdata = slots[idx];
                RCT_ASSERT(osdata->slot_num == idx);
                if(osdata->source == NULL){
                    osdata->source = hiarray_create(1, sizeof(cluster_node *));
                    if(osdata->source == NULL){
                        log_stdout("Out of memory");
                        goto error;
                    }
                }

                node_elem = hiarray_push(osdata->source);
                *node_elem = (*oslot)->node;
            }
         }

         if(master->importing){
            for(i = 0; i < hiarray_n(master->importing); i ++){
                oslot = hiarray_get(master->importing, i);
                idx = (*oslot)->slot_num;
                if(slots[idx] == NULL){
                    osdata_elem = hiarray_push(osdatas);
                    if(osdata_elem == NULL){
                        log_stdout("Out of memory");
                        goto error;
                    }

                    osdata = open_slot_data_create();
                    if(osdata == NULL){
                        log_stdout("Out of memory");
                        goto error;
                    }

                    osdata->slot_num = idx;
                    *osdata_elem = osdata;
                    slots[idx] = osdata;
                }

                osdata = slots[idx];
                RCT_ASSERT(osdata->slot_num == idx);
                if(osdata->target == NULL){
                    osdata->target = hiarray_create(1, sizeof(cluster_node *));
                    if(osdata->target == NULL){
                        log_stdout("Out of memory");
                        goto error;
                    }
                }

                node_elem = hiarray_push(osdata->target);
                *node_elem = (*oslot)->node;
            }
         }
    }

    dictReleaseIterator(di_nodes);
    di_nodes = NULL;

    return osdatas;

error:

    if(di_nodes != NULL){
        dictReleaseIterator(di_nodes);
    }

    if(osdatas){
        while(hiarray_n(osdatas)){           
            osdata_elem = hiarray_pop(osdatas);
            open_slot_data_destroy(*osdata_elem);
        }
        hiarray_destroy(osdatas);
    }

    return NULL;
}

void dictDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    dictRelease(val);
}

dictType nodesConfDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    dictDestructor              /* val destructor */
};

static dict *get_nodes_info_with_async_reply(struct hiarray *results)
{
    uint32_t i;
    listIter *li;
    listNode *ln;
    redisReply *reply;
    dict *nodes_infos;
    dict *nodes;
    hilist *slaves;
    struct cluster_node **elem_node, *node, *slave;
    
    nodes_infos = dictCreate(&nodesConfDictType, NULL);
    if(nodes_infos == NULL){
        log_error("Out of memory");
        return NULL;
    }

    for(i = 0; i < hiarray_n(results); i ++){
        elem_node = hiarray_get(results, i);
        reply = (*elem_node)->data;

        if(reply == NULL || reply->type != REDIS_REPLY_STRING){
            log_warn("Warn: %s[%s] is error: %s.", 
                (*elem_node)->role == REDIS_ROLE_MASTER?"master":"slave",
                (*elem_node)->addr, reply==NULL?"NULL":reply->str);
            continue;
        }

        nodes = parse_cluster_nodes(NULL, reply->str, reply->len, 
            HIRCLUSTER_FLAG_ADD_SLAVE|HIRCLUSTER_FLAG_ADD_OPENSLOT);
        if(nodes == NULL){
            log_warn("Warn: %s[%s] \"cluster nodes\" reply parse error", 
                (*elem_node)->role == REDIS_ROLE_MASTER?"master":"slave",
                (*elem_node)->addr);
            continue;
        }

        dictAdd(nodes_infos, (*elem_node)->addr, nodes);
        
        slaves = (*elem_node)->slaves;
        if((*elem_node)->role == REDIS_ROLE_MASTER &&
            slaves != NULL){
            li = listGetIterator(slaves, AL_START_HEAD);
            while(ln = listNext(li)){
                slave = listNodeValue(ln);
                reply = slave->data;

                if(reply == NULL || reply->type != REDIS_REPLY_STRING){
                    log_warn("Warn: slave[%s] is error: %s.", 
                        slave->addr, reply==NULL?"NULL":reply->str);
                    continue;
                }

                nodes = parse_cluster_nodes(NULL, reply->str, reply->len, 
                    HIRCLUSTER_FLAG_ADD_SLAVE|HIRCLUSTER_FLAG_ADD_OPENSLOT);
                if(nodes == NULL){
                    log_warn("Warn: slave[%s] \"cluster nodes\" reply parse error", 
                        (*elem_node)->addr);
                    continue;
                }

                dictAdd(nodes_infos, slave->addr, nodes);
            }

            listReleaseIterator(li);
        }
    }

    return nodes_infos;
}

int async_reply_cluster_create(async_command *acmd)
{
    int ret, stop;
    int i;
    struct hiarray *results = &acmd->results;
    rctContext *ctx = acmd->ctx;
    dict *nodes_infos;
    struct hiarray *rinsts;
    redis_instance *master, *slave;
    redisContext *con;
    redisReply *reply = NULL;
    listIter *it = NULL;
    listNode *ln;
    sds master_name;
    dictEntry *den;
    cluster_node *node;

    stop = 1;
    
    nodes_infos = get_nodes_info_with_async_reply(results);
    if(nodes_infos == NULL){
        log_stdout("Get nodes config info error.");
        return RCT_AE_STOP;
    }

    ret = check_cluster_config_signature(nodes_infos, 0);
    if(ret != RCT_OK){
        log_stdout_without_newline(".");
        sleep(1);
        stop = 0;
        cluster_async_call(ctx, "cluster nodes", NULL, 
            ctx->redis_role, async_reply_cluster_create);
        goto done;
    }

    log_stdout("\nAll nodes joined!");

    async_command_reset(acmd);
    cluster_update_route(ctx->acmd->acc->cc);
    acmd->nodes = acmd->acc->cc->nodes;

    rinsts = ctx->private_data;

    for(i = 0; i < hiarray_n(rinsts); i++){
        master = hiarray_get(rinsts, i);

        den = dictFind(acmd->nodes, master->addr);
        if(den == NULL){
            log_stdout("Error: node %s can not find in the cluster.",
                master->addr);
            goto done;
        }
        
        node = dictGetEntryVal(den);
        master_name = node->name;
        
        if(master->slaves){            
            it = listGetIterator(master->slaves, AL_START_HEAD);
            while((ln = listNext(it)) != NULL){
                slave = listNodeValue(ln);
                con = cxt_get_by_redis_instance(slave);
                if(con == NULL || con->err){
                    log_stdout("Connect to %s failed: %s", 
                        slave->addr, con==NULL?"NULL":con->errstr);
                    goto done;
                }
                
                reply = redisCommand(con, "cluster replicate %s", master_name);
                if(reply == NULL || reply->type != REDIS_REPLY_STATUS || 
                    strcmp(reply->str, "OK") != 0){
                    log_stdout("Command \"cluster replicate\" reply error: %s.",
                        reply==NULL?"NULL":(reply->type == REDIS_REPLY_ERROR?reply->str:"other"));
                    goto done;
                }

                freeReplyObject(reply);
                reply = NULL;
            }
            
            listReleaseIterator(it);
            it = NULL;
        }
    }

    log_stdout("Cluster created success!");

done:

    if(nodes_infos != NULL){
        dictRelease(nodes_infos);
    }

    if(reply != NULL){
        freeReplyObject(reply);
    }

    if(it != NULL){
        listReleaseIterator(it);
    }

    if(!stop) return RCT_AE_CONTINUE;
    
    return RCT_AE_STOP;
}

int async_reply_check_cluster(async_command *acmd)
{
    int ret;
    uint32_t i, k;
    struct cluster_node **elem_node;
    struct hiarray *results = &acmd->results;
    dict *nodes;
    copen_slot **oslot;
    dict *nodes_infos;
    dictIterator *di_nodes;
    dictEntry *de_nodes;
    struct hiarray *osdatas = NULL;
    open_slot_data **osdata;

    log_stdout("Checking cluster ...");

    nodes_infos = get_nodes_info_with_async_reply(results);
    if(nodes_infos == NULL){
        log_stdout("Get nodes config info error.");
        return RCT_AE_STOP;
    }

    ret = check_cluster_config_signature(nodes_infos, 1);
    if(ret == RCT_OK){
        log_stdout("All nodes agree about slots configuration.");

        di_nodes = dictGetIterator(nodes_infos);
        de_nodes = dictNext(di_nodes);
        nodes = dictGetEntryVal(de_nodes);
        dictReleaseIterator(di_nodes);
        ret = check_cluster_slot_coverage(nodes);
        if(ret == RCT_OK){
            log_stdout("All 16384 slots covered.");
        }else{
            log_stdout("Not all 16384 slots covered.");
        }
    }
    
    osdatas = get_cluster_open_slot(nodes_infos);
    if(osdatas == NULL){
        log_stdout("Get cluster open slot failed.");
        goto done;
    }

    if(hiarray_n(osdatas) == 0){
        log_stdout("No open slots.");
    }else{
        log_stdout("");
        for(i = 0; i < hiarray_n(osdatas); i ++){
            osdata = hiarray_get(osdatas, i);
            if((*osdata)->source){
                for(k = 0; k < hiarray_n((*osdata)->source); k++){
                    elem_node = hiarray_get((*osdata)->source, k);
                    log_stdout("node[%s] migrating %5u slot", 
                        (*elem_node)->addr, (*osdata)->slot_num);
                }
            }
            if((*osdata)->target){
                for(k = 0; k < hiarray_n((*osdata)->target); k++){
                    elem_node = hiarray_get((*osdata)->target, k);
                    log_stdout("node[%s] importing %5u slot", 
                        (*elem_node)->addr, (*osdata)->slot_num);
                }
            }
        }
    }
    
    log_stdout("");

done:

    if(nodes_infos != NULL){
        dictRelease(nodes_infos);
    }

    if(osdatas){
        while(hiarray_n(osdatas)){           
            osdata = hiarray_pop(osdatas);
            open_slot_data_destroy(*osdata);   
        }
        hiarray_destroy(osdatas);
    }

    return RCT_AE_STOP;
}

int async_reply_destroy_cluster(async_command *acmd)
{
    int ret;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    dictIterator *di = NULL;
    dictEntry *de;
    dict *nodes;
    rctContext *ctx = acmd->ctx;
    struct cluster_node *master, *slave;
    sds command = NULL;

    if(acmd == NULL)
    {
        return RCT_AE_STOP;
    }

    async_reply_display(acmd);
    
    nodes = acmd->nodes;
    if(nodes == NULL)
    {
        return RCT_AE_STOP;
    }

    command = sdsnew("CLUSTER FORGET ");

    /* step 1: cluster forget all nodes */
    di = dictGetIterator(nodes);
    while((de = dictNext(di)) != NULL) {
        master = dictGetEntryVal(de);

        sdsrange(command, 0, 14);

        command = sdscatsds(command, master->name);
        
        cluster_async_call(ctx, command, NULL, 
            ctx->redis_role, NULL);
        
        slaves = master->slaves;
        if(slaves == NULL)
        {
            continue;
        }
        
        li = listGetIterator(slaves, AL_START_HEAD);
        while((ln = listNext(li)) != NULL)
        {
            slave = listNodeValue(ln);
            
            sdsrange(command, 0, 14);
            command = sdscatsds(command, slave->name);
            cluster_async_call(ctx, command, NULL, 
                ctx->redis_role, NULL);
        }

        listReleaseIterator(li);
   
    }
    dictReleaseIterator(di);
    di = NULL;

    sleep(1);

    /* step 2: delete slaves for masters by manual failover */
    cluster_async_call(ctx, "CLUSTER FAILOVER TAKEOVER", NULL, 
        ctx->redis_role, NULL);

    /* step 3: forget master for slaves */
    di = dictGetIterator(nodes);
    while((de = dictNext(di)) != NULL) {
        master = dictGetEntryVal(de);

        sdsrange(command, 0, 14);
        command = sdscatsds(command, master->name);
        
        cluster_async_call(ctx, command, NULL, 
            ctx->redis_role, NULL);   
    }
    dictReleaseIterator(di);
    di = NULL;

    sdsfree(command);

    /* step 4: delete slots for original slaves after manual failover */
    cluster_async_call(ctx, "CLUSTER FLUSHSLOTS", NULL, 
        ctx->redis_role, NULL);

    /* step 5: flushall the nodes */
    cluster_async_call(ctx, "FLUSHALL", NULL, 
        ctx->redis_role, NULL);

    /* step 6: cluster reset */
    cluster_async_call(ctx, "CLUSTER RESET HARD", NULL, 
        ctx->redis_role, NULL);

    log_stdout("Cluster destroy finished.");
    log_stdout("");
    
    return RCT_AE_CONTINUE;

error:

    if(di != NULL){
        dictReleaseIterator(di);
    }

    if(li != NULL){
        listReleaseIterator(li);
    }

    if(command != NULL){
        sdsfree(command);
    }

    return RCT_AE_STOP;
}

int async_reply_delete_all_slaves(async_command *acmd)
{
    int ret;
    hilist *slaves;
    listIter *li = NULL;
    listNode *ln;
    dictIterator *di = NULL;
    dictEntry *de;
    dict *nodes;
    rctContext *ctx = acmd->ctx;
    struct cluster_node *master, *slave;
    sds command = NULL;

    if(acmd == NULL)
    {
        return RCT_AE_STOP;
    }

    async_reply_display(acmd);
    
    nodes = acmd->nodes;
    if(nodes == NULL)
    {
        return RCT_AE_STOP;
    }

    command = sdsnew("CLUSTER FORGET ");

    /* step 1: cluster forget all slaves */
    di = dictGetIterator(nodes);
    while((de = dictNext(di)) != NULL) {
        master = dictGetEntryVal(de);
        
        slaves = master->slaves;
        if(slaves == NULL)
        {
            continue;
        }
        
        li = listGetIterator(slaves, AL_START_HEAD);
        while((ln = listNext(li)) != NULL)
        {
            slave = listNodeValue(ln);
            
            sdsrange(command, 0, 14);
            command = sdscatsds(command, slave->name);
            cluster_async_call(ctx, command, NULL, 
                ctx->redis_role, NULL);
        }

        listReleaseIterator(li);
   
    }
    dictReleaseIterator(di);
    di = NULL;
    sdsfree(command);
    command = NULL;

    sleep(1);

    /* step 2: flushall the slaves */
    cluster_async_call(ctx, "FLUSHALL", NULL, 
        RCT_REDIS_ROLE_SLAVE, NULL);

    /* step 3: cluster reset the slaves */
    cluster_async_call(ctx, "CLUSTER RESET HARD", NULL, 
        RCT_REDIS_ROLE_SLAVE, NULL);

    log_stdout("All slaves are deleted.");
    log_stdout("");
    
    return RCT_AE_CONTINUE;

error:

    if(di != NULL){
        dictReleaseIterator(di);
    }

    if(li != NULL){
        listReleaseIterator(li);
    }

    if(command != NULL){
        sdsfree(command);
    }

    return RCT_AE_STOP;
}

int async_reply_dump_conf_file(async_command *acmd)
{
    int ret;
    dict *cluster_nodes;
    rctContext *ctx = acmd->ctx;
    struct hiarray *nodes = NULL;
    struct redis_instance *master;
    sds command = NULL;
    sds conf_info_string = NULL;
    sds filename = NULL;

    if(acmd == NULL)
    {
        return RCT_AE_STOP;
    }

    async_reply_display(acmd);
    
    cluster_nodes = acmd->nodes;
    if(cluster_nodes == NULL)
    {
        return RCT_AE_STOP;
    }

    command = sdsnew("");

    nodes = redis_instance_array_create_from_cluster(cluster_nodes);
    if (nodes == NULL) {
        goto error;
    }

    conf_info_string = generate_conf_info_string(nodes);
    if (hiarray_n(&ctx->args) > 0) {
        filename = *(sds *)hiarray_get(&ctx->args, 0);
    } else if (ctx->cf != NULL) {
        filename = ctx->cf->fname;
    }
    dump_conf_file(filename, conf_info_string);

    sdsfree(command);
    sdsfree(conf_info_string);

    while (hiarray_n(nodes) > 0) {
        redis_instance *master = hiarray_pop(nodes);
        redis_instance_deinit(master);
    }
    hiarray_destroy(nodes);

    log_stdout("");
    log_stdout("Cluster conf file dumped complete.");
    
    return RCT_AE_STOP;

error:

    if(command != NULL){
        sdsfree(command);
    }

    if(conf_info_string != NULL){
        sdsfree(conf_info_string);
    }

    if (nodes != NULL) {
        while (hiarray_n(nodes) > 0) {
            redis_instance *master = hiarray_pop(nodes);
            redis_instance_deinit(master);
        }
        hiarray_destroy(nodes);
    }

    return RCT_AE_STOP;
}

redisReply *redis_reply_clone(redisReply *r)
{
    int j;
    redisReply *reply = NULL;

    if(r == NULL){
        return NULL;
    }

    reply = rct_alloc(sizeof(*reply));
    if(reply == NULL){
        goto enomem;
    }

    reply->type = r->type;
    reply->integer = r->integer;
    reply->len = r->len;
    reply->str = NULL;
    reply->elements = r->elements;
    reply->element = NULL;

    switch(r->type) {
    case REDIS_REPLY_INTEGER:
        break; /* Nothing to free */
    case REDIS_REPLY_ARRAY:
        if (r->element != NULL && r->elements > 0) {
            reply->element = rct_alloc(r->elements * sizeof(redisReply *));
            if(reply->element == NULL){
                goto enomem;
            }
            
            for (j = 0; j < r->elements; j++){
                if (r->element[j] != NULL){
                    reply->element[j] = 
                        redis_reply_clone(r->element[j]);
                    if(reply->element[j] == NULL){
                        goto enomem;
                    }
                }else{
                    reply->element[j] = NULL;
                }
            }
        }
        
        break;
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_STRING:
        if (r->str != NULL){
            reply->str = rct_strndup(r->str, r->len);
            if(reply->str == NULL){
                goto enomem;
            }
        }
        
        break;
    }
    
    return reply;

enomem:

    log_error("Out of memory");

    if(reply != NULL){
        freeReplyObject(reply);
    }

    return NULL;
}

static void redis_instance_list_free(void *ptr)
{
    redis_instance *node = ptr;
    redis_instance_destroy(node);
}

int redis_instance_init(redis_instance *node, const char *addr, int role)
{
    sds *ip_port = NULL;
    int ip_port_count = 0;

    if(node == NULL || addr == NULL){
        return RCT_ERROR;
    }

    node->name = NULL;
    node->addr = NULL;
    node->host = NULL;
    node->port = 0;
    node->con = NULL;
    node->role = RCT_REDIS_ROLE_NULL;
    node->slaves = NULL;
    node->slots = NULL;
    node->slots_count = 0;
    node->slots_weight = 1;
    node->slots_to_import = 0;

    node->addr = sdsnew(addr);
    
    ip_port = sdssplitlen(addr, strlen(addr), ":", 1, &ip_port_count);
    if(ip_port == NULL || ip_port_count != 2 || 
        sdslen(ip_port[0]) <= 0 || sdslen(ip_port[1]) <= 0)
    {
        log_stdout("Redis address %s is error.", addr);
        goto error;
    }

    node->host = ip_port[0];
    ip_port[0] = NULL;
    node->port = rct_atoi(ip_port[1]);

    sdsfreesplitres(ip_port, ip_port_count);
    ip_port = NULL;

    if(role == RCT_REDIS_ROLE_MASTER){
        node->role = RCT_REDIS_ROLE_MASTER;
    }else if(role == RCT_REDIS_ROLE_SLAVE){
        node->role = RCT_REDIS_ROLE_SLAVE;
    }else{
        log_stdout("Redis role error.");
        goto error;
    }

    node->slots = hiarray_create(1, sizeof(slots_region));
    if (node->slots == NULL) {
        log_stdout("Out of memory");
        goto error;
    }
    
    node->slaves = listCreate();
    if(node->slaves == NULL){
        log_stdout("Out of memory");
        goto error;
    }

    node->slaves->free = redis_instance_list_free;
    
    return RCT_OK;

error:
    
    if(ip_port != NULL)
    {
        sdsfreesplitres(ip_port, ip_port_count);
    }

    redis_instance_deinit(node);
    
    return RCT_ERROR;
}

redis_instance *redis_instance_create(const char *addr, int role)
{
    int ret;
    redis_instance *node;

    node = rct_alloc(sizeof(*node));
    if(node == NULL){
        return NULL;
    }

    ret = redis_instance_init(node, addr, role);
    if(ret != RCT_OK){
        redis_instance_destroy(node);
        return NULL;
    }

    return node;
}

void redis_instance_deinit(redis_instance *node)
{
    listIter *it;
    listNode *ln;
    redis_instance *slave;
    
    if(node == NULL){
        return;
    }

    if(node->name){
        sdsfree(node->name);
        node->name = NULL;
    }

    if(node->addr){
        sdsfree(node->addr);
        node->addr = NULL;
    }

    if(node->host){
        sdsfree(node->host);
        node->host = NULL;
    }

    if(node->con){
        redisFree(node->con);
        node->con = NULL;
    }

    if (node->slaves) {        
        listRelease(node->slaves);
        node->slaves = NULL;
    }

    if (node->slots) {
        hiarray_destroy(node->slots);
        node->slots = NULL;
    }

    node->slots_count = 0;
    node->slots_to_import = 0;
}

void redis_instance_destroy(redis_instance *node)
{
    if(node == NULL){
        return;
    }
    
    redis_instance_deinit(node);

    rct_free(node);
}

void rct_redis_instance_array_debug_show(struct hiarray *nodes)
{
#ifdef RCT_DEBUG_LOG

    int i, j;
    for (i = 0; i < hiarray_n(nodes); i++) {
        redis_instance *master;
        sds slots_region_str = sdsempty();

        master = hiarray_get(nodes, i);
        if (master->slots_count > 0) {
            slots_region_str = sdscat(slots_region_str, "{");
            for (j = 0; j < hiarray_n(master->slots); j ++) {
                slots_region *sr = hiarray_get(master->slots, j);
                slots_region_str = sdscatfmt(slots_region_str, "[%u, %u],", sr->start, sr->end);
            }
            sdsrange(slots_region_str, 0, sdslen(slots_region_str)-2);
            slots_region_str = sdscat(slots_region_str, "}");
        }

        
        log_stdout("master: %s:%d, slots_weight: %d, slots_count: %d, slots_region: %s, slots_to_import: %d", 
            master->host, master->port, master->slots_weight, master->slots_count, 
            sdslen(slots_region_str) == 0?"NULL":slots_region_str, master->slots_to_import);

        if (master->slaves) {
            listIter *it = listGetIterator(master->slaves, AL_START_HEAD);
            listNode *ln;
            redis_instance *slave;
            while((ln = listNext(it)) != NULL){
                slave = listNodeValue(ln);
                log_stdout(" slave: %s:%d", slave->host, slave->port);
            }
            
            listReleaseIterator(it);
        }
    }
    log_stdout("");

#endif
}

redisContext *cxt_get_by_redis_instance(redis_instance *node)
{
    if(node == NULL){
        return NULL;
    }

    if(node->con){
        if(node->con->err){
            redisReconnect(node->con);
        }

        return node->con;
    }

    if(node->host == NULL || !rct_valid_port(node->port)){
        return NULL;
    }

    return redisConnect(node->host, node->port);
}

int redis_instance_array_assign_master_slots(struct hiarray *nodes)
{
    int i;
    float node_slots_proportion;
    int slots_begin, slots_step, slots_remainder = REDIS_CLUSTER_SLOTS;
    int total_slots_weight = 0;
    int master_count;
    redis_instance *master;

    master_count = hiarray_n(nodes);
    for (i = 0; i < master_count; i++) {
        master = hiarray_get(nodes, i);
        total_slots_weight += master->slots_weight;
    }

    for (i = 0; i < master_count; i ++) {
        master = hiarray_get(nodes, i);
        if (master->slots_weight <= 0) continue;
        
        node_slots_proportion = (float)master->slots_weight/(float)total_slots_weight;
        master->slots_count = floor(node_slots_proportion*REDIS_CLUSTER_SLOTS);
        if (master->slots_count <= 0) master->slots_count = 1;
        slots_remainder -= master->slots_count;
    }

    RCT_ASSERT (slots_remainder >= 0);

    i = -1;
    while (slots_remainder > 0) {
        if (++i >= master_count) i = 0;
        master = hiarray_get(nodes, i);
        if (master->slots_weight <= 0) {
            continue;
        }

        master->slots_count ++;
        slots_remainder--;
    }

    slots_begin = 0;
    for (i = 0; i < master_count; i ++) {
        slots_region *sr;
        
        master = hiarray_get(nodes, i);
        if (master->slots_weight <= 0) continue;

        sr = hiarray_push(master->slots);
        sr->start = slots_begin;
        sr->end = sr->start + master->slots_count - 1;
        
        slots_begin += master->slots_count;
    }
    
    return RCT_OK;
}

struct hiarray *redis_instance_array_create_from_cluster(dict *cluster_nodes)
{
    int ret;
    listIter *li = NULL;
    listNode *ln;
    dictIterator *di = NULL;
    dictEntry *de;
    hilist *cluster_slaves;
    struct cluster_node *cluster_master, *cluster_slave;
    struct hiarray *nodes = NULL;
    struct redis_instance *master, *slave;
    
    nodes = hiarray_create(dictSize(cluster_nodes), sizeof(struct redis_instance));
    if (nodes == NULL) {
        log_stderr("ERROR: out of memory.");
        goto error;
    }

    di = dictGetIterator(cluster_nodes);
    while((de = dictNext(di)) != NULL) {
        cluster_master = dictGetEntryVal(de);

        master = hiarray_push(nodes);
        if (master == NULL) {
            log_stderr("ERROR: out of memory.");
            goto error;
        }
        ret = redis_instance_init(master, cluster_master->addr, RCT_REDIS_ROLE_MASTER);
        if (ret != RCT_OK) {
            goto error;
        }
        master->slots_count = node_hold_slot_num(cluster_master, NULL, 0);
        
        cluster_slaves = cluster_master->slaves;
        if (cluster_slaves == NULL) {
            continue;
        }
        
        li = listGetIterator(cluster_slaves, AL_START_HEAD);
        while ((ln = listNext(li)) != NULL) {
            cluster_slave = listNodeValue(ln);
            slave = redis_instance_create(cluster_slave->addr, RCT_REDIS_ROLE_SLAVE);
            if (slave == NULL) {
                goto error;
            }
            listAddNodeTail(master->slaves, slave);
        }

        listReleaseIterator(li);
    }
    dictReleaseIterator(di);
    di = NULL;

    return nodes;

error:

    if(di != NULL){
        dictReleaseIterator(di);
    }

    if(li != NULL){
        listReleaseIterator(li);
    }

    if (nodes != NULL) {
        while (hiarray_n(nodes) > 0) {
            redis_instance *master = hiarray_pop(nodes);
            redis_instance_deinit(master);
        }
        hiarray_destroy(nodes);
    }

    return NULL;
}


int redis_instance_array_addr_cmp(const void *t1, const void *t2)
{
    const redis_instance *s1 = (const redis_instance *)t1;
    const redis_instance *s2 = (const redis_instance *)t2;

    return sdscmp(s1->addr, s2->addr);
}

static void *event_run(void *args)
{
    aeMain(args);
    return 0;
}

struct scan_keys_data;
struct del_keys_data;
static void delete_keys_job(aeEventLoop *el, int fd, void *privdata, int mask);
static void scan_node_finish(aeEventLoop *el, int fd, void *privdata, int mask);

typedef struct del_keys_node{
    rctContext *ctx;
    cluster_node *node;
    struct scan_keys_data *scan_data;
    struct del_keys_data *del_data;
    redisAsyncContext *scan_ac;
    redisAsyncContext *del_ac;
    mttlist *keys;
    long long cursor;
    long long scan_keys_num;
    long long delete_keys_num;
    long long deleted_keys_num;
    int sd_notice;  //used to notice the delete thread to delete keys
    int scan_node_finish;
    long long cmds_sent_at_this_period;
}del_keys_node;

//for the scan thread
typedef struct scan_keys_data{
    pthread_t thread_id;
    aeEventLoop *loop;
    hilist *nodes_data;   //type : del_keys_node
    int nodes_count;    //this loop thread is responsible for 
    int finish_scan_nodes;
}scan_keys_data;

//for delete keys in aeEventLoop
typedef struct del_keys_data{
    pthread_t thread_id;
    aeEventLoop *loop;
    int nodes_count;
    int finish_del_nodes;
}del_keys_data;

void connectCallback(const redisAsyncContext *c, int status) {
    if (status != RCT_OK) {
        log_stdout("Error: %s", c->errstr);
        return;
    }

    log_debug(LOG_DEBUG, "Connected...");
}

void disconnectCallback(const redisAsyncContext *c, int status) {
    if (status != RCT_OK) {
        log_stdout("Error: %s", c->errstr);
        return;
    }

    log_debug(LOG_DEBUG, "Disconnected...");
}

static int del_keys_node_init(del_keys_node *node_data, 
    rctContext *ctx, cluster_node *node)
{
    int ret;

    if(node_data == NULL || node == NULL)
    {
        return RCT_ERROR;
    }

    node_data->ctx = ctx;
    node_data->node = node;
    node_data->cursor = 0;
    node_data->scan_node_finish = 0;
    node_data->delete_keys_num = 0;
    node_data->scan_keys_num = 0;
    node_data->deleted_keys_num = 0;
    node_data->sd_notice = 0;
    node_data->scan_ac = NULL;
    node_data->del_ac = NULL;
    node_data->keys = NULL;
    node_data->scan_data = NULL;
    node_data->del_data = NULL;
    node_data->cmds_sent_at_this_period = 0;

    node_data->sd_notice = socket(AF_INET, SOCK_STREAM, 0);
    if (node_data->sd_notice < 0) 
    {   
        log_stdout("error: get sd_notice failed");
        return RCT_EAGAIN;
    }
    
    node_data->scan_ac = redisAsyncConnect(node->host, node->port);
    if(node_data->scan_ac == NULL)
    {
        log_stdout("error: %s[%s] get scan_ac failed", 
            node_role_name(node), node->addr);
        return RCT_EAGAIN;
    }

    node_data->del_ac = redisAsyncConnect(node->host, node->port);
    if(node_data->del_ac == NULL)
    {
        log_stdout("error: %s[%s] get del_ac failed", 
            node_role_name(node), node->addr);
        return RCT_EAGAIN;
    }

    redisAsyncSetConnectCallback(node_data->scan_ac,connectCallback);
    redisAsyncSetDisconnectCallback(node_data->scan_ac,disconnectCallback);
    redisAsyncSetConnectCallback(node_data->del_ac,connectCallback);
    redisAsyncSetDisconnectCallback(node_data->del_ac,disconnectCallback);
    
    node_data->keys = mttlist_create();
    if(node_data->keys == NULL)
    {
        log_stdout("error: out of memory");
        return RCT_ENOMEM;
    }

    ret = mttlist_init_with_locklist(node_data->keys);
    if(ret != RCT_OK)
    {
        log_stdout("error: out of memory");
        return RCT_ENOMEM;
    }
    
    return RCT_OK;
}

static void del_keys_node_deinit(del_keys_node *node_data)
{
    if(node_data == NULL)
    {
        return;
    }

    if(node_data->sd_notice > 0)
    {
        close(node_data->sd_notice);
        node_data->sd_notice = 0;
    }

    if(node_data->scan_ac != NULL)
    {
        redisAsyncDisconnect(node_data->scan_ac);
        node_data->scan_ac = NULL;
    }

    if(node_data->del_ac != NULL)
    {
        redisAsyncDisconnect(node_data->del_ac);
        node_data->del_ac = NULL;
    }

    if(node_data->keys != NULL)
    {
        mttlist_destroy(node_data->keys);
        node_data->keys = NULL;
    }
}

static int scan_keys_data_init(scan_keys_data *sdata)
{
	if(sdata == NULL)
	{
		return RCT_ERROR;
	}

	sdata->thread_id = 0;
    sdata->finish_scan_nodes = 0;
    sdata->nodes_count = 0;
    sdata->loop = NULL;
    sdata->nodes_data = NULL;

    sdata->loop = aeCreateEventLoop(1000);
    if(sdata->loop == NULL)
    {
    	log_stdout("error: create event loop failed");
        return RCT_ERROR;
    }
    
	sdata->nodes_data = listCreate();
	if(sdata->nodes_data == NULL)
	{
		log_stdout("error: out of memory");
		return RCT_ENOMEM;
	}
	
	return RCT_OK;
}

static void scan_keys_data_deinit(scan_keys_data *sdata)
{
	if(sdata == NULL)
	{
		return;
	}

    if(sdata->loop != NULL)
	{
		aeDeleteEventLoop(sdata->loop);
		sdata->loop = NULL;
	}

	if(sdata->nodes_data != NULL)
	{
		listRelease(sdata->nodes_data);
		sdata->nodes_data = NULL;
	}
}

static int del_keys_data_init(del_keys_data *ddata)
{
	if(ddata == NULL)
	{
		return RCT_ERROR;
	}

	ddata->thread_id = 0;
    ddata->finish_del_nodes = 0;
    ddata->nodes_count = 0;
	ddata->loop = NULL;

	ddata->loop = aeCreateEventLoop(1000);
    if(ddata->loop == NULL)
    {
    	log_stdout("error: create event loop failed");
        return RCT_ERROR;
    }
	
	return RCT_OK;
}

static void del_keys_data_deinit(del_keys_data *ddata)
{
	if(ddata == NULL)
	{
		return;
	}

	if(ddata->loop != NULL)
	{
		aeDeleteEventLoop(ddata->loop);
		ddata->loop = NULL;
	}
}

static void print_del_keys_node(del_keys_node *node_data, int log_level)
{
    if(node_data == NULL)
    {
        return;
    }

    if(log_level < RCT_LOG_MIN || log_level > RCT_LOG_MAX)
    {
        log_level = LOG_DEBUG;
    }

    log_debug(log_level, "del_keys_node info :");
    log_debug(log_level, "address: %s", node_data->node->addr);
    log_debug(log_level, "cursor: %d", node_data->cursor);
    log_debug(log_level, "delete_keys_num: %lld", node_data->delete_keys_num);
    log_debug(log_level, "scan_keys_num: %lld", node_data->scan_keys_num);
    log_debug(log_level, "sd_notice: %d", node_data->sd_notice);
    log_debug(log_level, "scan_node_finish: %d", node_data->scan_node_finish);
    log_debug(log_level, "scan_data loop: %d", node_data->scan_data->loop);
    log_debug(log_level, "del_data loop: %d", node_data->del_data->loop);
    log_debug(log_level, "");
}

static void print_scan_keys_data(scan_keys_data *sdata, int log_level)
{
    listIter *it;
    listNode *ln;
    del_keys_node *node_data;

	if(sdata == NULL)
	{
		return;
	}

    if(log_level < RCT_LOG_MIN || log_level > RCT_LOG_MAX)
    {
        log_level = LOG_DEBUG;
    }

    log_debug(log_level, "scan_keys_data info :");
    log_debug(log_level, "thread_id: %ld", sdata->thread_id);
    log_debug(log_level, "loop: %d", sdata->loop);
    log_debug(log_level, "nodes_count: %d", sdata->nodes_count);
    log_debug(log_level, "finish_scan_nodes: %d", sdata->finish_scan_nodes);
    log_debug(log_level, "");
    if(sdata->nodes_data == NULL)
    {
        log_stdout("nodes_data: %s", "NULL");
    }
    else
    {
        it = listGetIterator(sdata->nodes_data, AL_START_HEAD);
        while((ln = listNext(it)) != NULL)
        {
            node_data = listNodeValue(ln);
            
            print_del_keys_node(node_data, log_level);
        }

        listReleaseIterator(it);
    }
    
    log_debug(log_level, "");
}

static void print_del_keys_data(del_keys_data *ddata, int log_level)
{
	if(ddata == NULL)
	{
		return;
	}

    if(log_level < RCT_LOG_MIN || log_level > RCT_LOG_MAX)
    {
        log_level = LOG_DEBUG;
    }

	log_debug(log_level, "del_keys_data info :");
    log_debug(log_level, "thread_id: %ld", ddata->thread_id);
    log_debug(log_level, "loop: %d", ddata->loop);
    log_debug(log_level, "nodes_count: %d", ddata->nodes_count);
    log_debug(log_level, "finish_del_nodes: %d", ddata->finish_del_nodes);
    log_debug(log_level, "");
}

static int scan_job_finished(scan_keys_data *scan_data)
{
    if(scan_data == NULL)
    {
        return 0;
    }

    log_debug(LOG_DEBUG, "scan_job_finished() %d, %d", 
        scan_data->finish_scan_nodes, scan_data->nodes_count);
    
    if(scan_data->finish_scan_nodes >= scan_data->nodes_count)
    {
        return 1;
    }

    return 0;
}

static int del_job_finished(del_keys_data *del_data)
{
    if(del_data == NULL)
    {
        return 0;
    }

    log_debug(LOG_DEBUG, "del_job_finished() %d, %d", 
        del_data->finish_del_nodes, del_data->nodes_count);

    if(del_data->finish_del_nodes >= del_data->nodes_count)
    {
        return 1;
    }

    return 0;
}

static void scan_keys_callback(redisAsyncContext *ac, void *r, void *privdata)
{   
    int ret;
    int i;
    long long cursor;
    del_keys_node *node_data = privdata;
    rctContext *ctx = node_data->ctx;
    scan_keys_data *scan_data = node_data->scan_data;
    del_keys_data *del_data = node_data->del_data;
    cluster_node *node = node_data->node;    
    mttlist *keys = node_data->keys;
    int sd_notice = node_data->sd_notice;
    redisReply *reply = r, *sub_reply;
    int sd_notice_scan_finish;
    
    log_debug(LOG_VERB, "scan_keys_callback() node:%s", node->addr);

    //step 1: get the cursor and keys from the scan reply.
    if(reply == NULL)
    {
        log_stdout("%s[%s] %s failed(reply is NULL)!", 
            node_role_name(node), node->addr, "scan");
        goto done;
    }
    else if(reply->type == REDIS_REPLY_ERROR)
    {
        log_stdout("error: scan reply error(%s)", reply->str);
        goto done;
    }
    
    if(reply->type != REDIS_REPLY_ARRAY || 
        reply->elements != 2)
    {
        log_stdout("error: scan reply format is wrong");
        goto done;
    }

    sub_reply = reply->element[0];
    if(sub_reply->type != REDIS_REPLY_STRING)
    {
        log_stdout("error: scan reply array first element is not integer");
        goto done;
    }

    cursor = rct_atoll(sub_reply->str, sub_reply->len);
    node_data->cursor = cursor;

    log_debug(LOG_VERB, "cursor: %lld", cursor);

    sub_reply = reply->element[1];
    if(sub_reply->type != REDIS_REPLY_ARRAY)
    {
        log_stdout("error: scan reply array second element is not array");
        goto done;
    }

    for(i = 0; i < sub_reply->elements && del_data != NULL; i ++)
    {
        log_debug(LOG_VERB, "key : %s", sub_reply->element[i]->str);
        
        mttlist_push(keys, sdsnewlen(sub_reply->element[i]->str,sub_reply->element[i]->len));
        aeCreateFileEvent(del_data->loop, sd_notice, 
            AE_WRITABLE, delete_keys_job, node_data);
        node_data->cmds_sent_at_this_period ++;
    } 


    if (ctx->commands_limit_per_second > 0 &&
        node_data->cmds_sent_at_this_period >= 
        ctx->commands_limit_per_second) {
        sleep(1);
        node_data->cmds_sent_at_this_period = 0;
    }

    //step 2: Continue to get the keys.
    if(cursor > 0)
    {
        redisAsyncCommand(ac, scan_keys_callback, node_data, 
            "scan %lld MATCH %s COUNT %d", cursor, 
            *(sds*)hiarray_get(&ctx->args, 0), 1000);
        node_data->cmds_sent_at_this_period ++;
        return;
    }

done:

    //step 3: end up this node scan keys.    
    scan_data->finish_scan_nodes ++;
    if(scan_job_finished(scan_data))
    {
        aeStop(scan_data->loop);
        log_debug(LOG_NOTICE, "scan thread(%ld) stop. node[%s]", 
            scan_data->thread_id, node->addr);
    }

    if(del_data != NULL)
    {
        sd_notice_scan_finish = socket(AF_INET, SOCK_STREAM, 0);
        if (sd_notice_scan_finish < 0) 
        {   
            log_stdout("error: get sd_notice_finish failed");
            return;
        }

        aeCreateFileEvent(del_data->loop, sd_notice_scan_finish,
            AE_WRITABLE, scan_node_finish, node_data);
    }
}

static void delete_keys_callback(redisAsyncContext *ac, void *r, void *privdata)
{
    del_keys_node *node_data = privdata;
    del_keys_data *del_data = node_data->del_data;
    redisReply *reply = r;

    log_debug(LOG_VERB, "delete_keys_callback() node:%s", node_data->node->addr);

    node_data->delete_keys_num ++;

    if(reply != NULL && reply->type == REDIS_REPLY_INTEGER
        && reply->integer == 1)
    {
        node_data->deleted_keys_num ++;
    }

    if(node_data->scan_node_finish)
    {
        if((node_data->delete_keys_num >= 
            node_data->scan_keys_num) && 
            mttlist_empty(node_data->keys))
        {
            del_data->finish_del_nodes ++;
            if(del_job_finished(del_data))
            {
                aeStop(del_data->loop);
                log_debug(LOG_NOTICE, "delete thread(%ld) stop. node[%s]", 
                    del_data->thread_id, node_data->node->addr);
            }
        }
    }
}

static void delete_keys_job(aeEventLoop *el, int fd, void *privdata, int mask)
{
    del_keys_node *node_data = privdata;
    del_keys_data *del_data = node_data->del_data;
    redisAsyncContext *ac = node_data->del_ac;
    aeEventLoop *loop = del_data->loop;
    void *keys = node_data->keys;
    void *key;

    log_debug(LOG_VERB, "delete_keys_job() node:%s", node_data->node->addr);

    while((key = mttlist_pop(keys)) != NULL)
    {
        log_debug(LOG_VERB, "key: %s", key);
        node_data->scan_keys_num ++;
        char *argv[2];
        size_t argvlen[2];
        argv[0] = "del";
        argv[1] = key;
        argvlen[0] = 3;
        argvlen[1] = sdslen(key);
        redisAsyncCommandArgv(ac, delete_keys_callback, node_data, 2, argv, argvlen);
        sdsfree(key);
    }
}

static void scan_node_finish(aeEventLoop *el, int fd, void *privdata, int mask)
{
    del_keys_node *node_data = privdata;
    del_keys_data *del_data = node_data->del_data;

    log_debug(LOG_DEBUG, "scan_node_finish() node:%s", node_data->node->addr);

    node_data->scan_node_finish = 1;

    aeDeleteFileEvent(del_data->loop, fd, AE_WRITABLE);

    if((node_data->delete_keys_num >= 
        node_data->scan_keys_num) && 
        mttlist_empty(node_data->keys))
    {
        log_debug(LOG_INFO, "delete keys finish. fd: %d, node[%s]", 
            fd, node_data->node->addr);

        del_data->finish_del_nodes ++;
        if(del_job_finished(del_data))
        {
            aeStop(del_data->loop);
            log_debug(LOG_NOTICE, "delete thread(%ld) stop. node[%s]", 
                del_data->thread_id, node_data->node->addr);
        }
    }

    close(fd);
}

static long long scan_keys_job_one_node(del_keys_node *node_data)
{
    rctContext *ctx = node_data->ctx;
    redisClusterContext *cc = ctx->cc;
    cluster_node *node = node_data->node;
    del_keys_data *del_data = node_data->del_data;
    aeEventLoop *del_loop = del_data->loop;
    redisContext *c = NULL;
    redisReply *reply = NULL, *sub_reply;
    long long cursor = node_data->cursor;
    int i;
    int done = 0;
    mttlist *keys = node_data->keys;
    int sd_notice = node_data->sd_notice;
    int ret;

    log_debug(LOG_VERB, "scan_keys_job() node:%s", node->addr);
    
    c = ctx_get_by_node(cc, node);
    if(c == NULL)
    {   
        log_stdout("%s[%s] get connect failed", 
            node_role_name(node), node->addr);
        return 0;
    } 

    reply = redisCommand(c, "scan %lld MATCH %s COUNT %d", 
        cursor, *(sds*)hiarray_get(&ctx->args, 0), 1000);

    if(reply == NULL)
    {
        log_stdout("%s[%s] %s failed(reply is NULL)!", 
            node_role_name(node), node->addr, "scan");
        goto error;
    }
    else if(reply->type == REDIS_REPLY_ERROR)
    {
        log_stdout("error: scan reply error(%s)", reply->str);
        goto error;
    }
    
    if(reply->type != REDIS_REPLY_ARRAY || 
        reply->elements != 2)
    {
        log_stdout("error: scan reply format is wrong");
        goto error;
    }

    sub_reply = reply->element[0];
    if(sub_reply->type != REDIS_REPLY_STRING)
    {
        log_stdout("error: scan reply array first element is not integer");
        goto error;
    }

    cursor = rct_atoll(sub_reply->str, sub_reply->len);
    node_data->cursor = cursor;

    log_debug(LOG_VERB, "cursor: %lld", cursor);

    sub_reply = reply->element[1];
    if(sub_reply->type != REDIS_REPLY_ARRAY)
    {
        log_stdout("error: scan reply array second element is not array");
        goto error;
    }

    for(i = 0; i < sub_reply->elements; i ++)
    {
        log_debug(LOG_VERB, "key : %s", sub_reply->element[i]->str);
        
        mttlist_push(keys, sub_reply->element[i]->str);
        sub_reply->element[i]->str = NULL;
        aeCreateFileEvent(del_loop, sd_notice, 
            AE_WRITABLE, delete_keys_job, node_data);
    }

    freeReplyObject(reply);
    
    return cursor;

error:

    if(reply != NULL)
    {
        freeReplyObject(reply);
    }

    return 0;
}

void *scan_keys_job(void *args)
{
    hilist *nodes_data = args;  //type : del_keys_node
    del_keys_node *node_data;
    del_keys_data *del_data;
    listNode *lnode, *lnode_next;
    long long cursor;

    while(listLength(nodes_data) > 0)
    {
    	lnode = listFirst(nodes_data);
    	while(lnode != NULL)
    	{
			node_data = listNodeValue(lnode);
            del_data = node_data->del_data;
            
            lnode_next = listNextNode(lnode);
            
			cursor = scan_keys_job_one_node(node_data);
            if(cursor <= 0)
            {
                listDelNode(nodes_data, lnode);
                lnode = lnode_next;

                int sd_notice_finish = socket(AF_INET, SOCK_STREAM, 0);
                if (sd_notice_finish < 0) 
                {   
                    log_stdout("error: get sd_notice_finish failed");
                    continue;
                }

                aeCreateFileEvent(del_data->loop, sd_notice_finish,
                    AE_WRITABLE, scan_node_finish, node_data);
            }
			else
            {         
			    lnode = lnode_next;
            }
        }
	}

}

void *scan_keys_job_run(void *args)
{
    scan_keys_data *scan_data = args;
    hilist *nodes_data = scan_data->nodes_data;  //type : del_keys_node
    del_keys_node *node_data;
    rctContext *ctx;
    redisAsyncContext *ac;
    listNode *lnode;
    listIter *it;

    it = listGetIterator(nodes_data, AL_START_HEAD);
    while((lnode = listNext(it)) != NULL)
    {
    	node_data = listNodeValue(lnode);
	    ctx = node_data->ctx;
        ac = node_data->scan_ac;
        
        redisAsyncCommand(ac, scan_keys_callback, node_data, 
            "scan %lld MATCH %s COUNT %d", node_data->cursor, 
            *(sds*)hiarray_get(&ctx->args, 0), 1000);
        node_data->cmds_sent_at_this_period ++;
	}
    
    listReleaseIterator(it);

    aeMain(scan_data->loop);
}


void cluster_del_keys(rctContext *ctx, int type)
{
    int ret;
	int i, k;
    dictIterator *di = NULL;
    dictEntry *de;
    redisClusterContext *cc;
    dict *nodes;
    cluster_node *master;
    int node_count = 0;
    int scan_threads_hold_node_count = 0;
    int del_threads_hold_node_count = 0;
    int thread_count;
	int modulo, remainder, add_one_count;
	int begin, step;
    int scan_threads_count, delete_threads_count;
    struct hiarray *node_datas = NULL; //type : del_keys_node
    del_keys_node *node_data;
	struct hiarray *scan_datas = NULL; //type : scan_keys_data
    scan_keys_data *scan_data;
    struct hiarray *del_datas = NULL; //type : del_keys_data
    del_keys_data *del_data;
    time_t t_start, t_end;
    long long deleted_keys_num = 0;
    int factor; //used to assign scan thread number and delete thread number
    int remainder_threads;

    if(ctx == NULL || ctx->cc == NULL)
    {
        return;
    }

    cc = ctx->cc;
    nodes = cc->nodes;
    if(nodes == NULL)
    {
        return;
    }
    
    thread_count = ctx->thread_count;
    if(thread_count <= 0)
    {
        log_stdout("error: thread count <= 0");
        return;
    }
	else if(thread_count == 1)
	{
        thread_count ++;
	}

    //init the delete key node data
    node_count = dictSize(nodes);
    if(node_count <= 0)
    {
        log_stdout("error: node count <= 0");
        return;
    }
    
    node_datas = hiarray_create(node_count, sizeof(del_keys_node));
    if(node_datas == NULL)
    {
        log_stdout("error: out of memory");
        goto done;
    }

	di = dictGetIterator(nodes);
    
    while((de = dictNext(di)) != NULL) {
        master = dictGetEntryVal(de);
        if(master->slots == NULL || listLength(master->slots) == 0)
        {
            continue;
        }

        node_data = hiarray_push(node_datas);

        ret = del_keys_node_init(node_data, ctx, master);
        if(ret != RCT_OK)
        {
            dictReleaseIterator(di);
            goto done;
        }
	}

    dictReleaseIterator(di);

    //assign the scan_threads_count and delete_threads_count
    node_count = hiarray_n(node_datas);
    if(node_count <= 0)
    {
        log_stdout("No node needs to delete keys");
        goto done;
    }

    /*avoid scan node job too much faster 
        than delete job and used too much memory,
        we set factor=40 to let scan thread less 
        than delete thread.
       */
    factor = 40;    
    
	scan_threads_count = (thread_count*factor)/100;
    if(scan_threads_count <= 0)
    {
        scan_threads_count = 1;
    }
	else if(scan_threads_count > node_count)
	{
		scan_threads_count = node_count;
	}
	
	delete_threads_count = thread_count - scan_threads_count;
	if(delete_threads_count == 0)
	{
		scan_threads_count --;
		delete_threads_count ++;
	}
	else if(delete_threads_count > node_count)
	{
		delete_threads_count = node_count;
	}

    remainder_threads = thread_count - (scan_threads_count + delete_threads_count);
    while(remainder_threads > 0)
    {
        if(delete_threads_count < node_count)
        {
            scan_threads_count ++;
            remainder_threads --;
        }
        else if(scan_threads_count < node_count)
        {
            scan_threads_count ++;
            remainder_threads --;
        }
        else
        {
            break;
        }
    }

	log_debug(LOG_NOTICE, "node_count : %d", node_count);
	log_debug(LOG_NOTICE, "thread_count : %d", thread_count);
	log_debug(LOG_NOTICE, "scan_threads_count : %d", scan_threads_count);
	log_debug(LOG_NOTICE, "delete_threads_count : %d", delete_threads_count);
    log_debug(LOG_NOTICE, "");

    //init the scan thread data
	scan_datas = hiarray_create(scan_threads_count, 
		sizeof(scan_keys_data));
    if(scan_datas == NULL)
    {
        log_stdout("error: out of memory");
        goto done;
    }

	modulo = node_count/scan_threads_count;
	remainder = node_count%scan_threads_count;
    add_one_count = remainder;
    begin = 0;
	step = modulo;
	if(add_one_count > 0)
	{
		step ++;
		add_one_count --;
	}
	
	for(i = 0; i < scan_threads_count; i ++)
	{
		scan_data = hiarray_push(scan_datas);
		ret = scan_keys_data_init(scan_data);
		if(ret != RCT_OK)
		{
			goto done;
		}

        scan_data->nodes_count = step;
		for(k = begin; k < begin + step; k ++)
		{
			node_data = hiarray_get(node_datas, k);
			listAddNodeTail(scan_data->nodes_data, node_data);
            node_data->scan_data = scan_data;
            redisAeAttach(scan_data->loop, node_data->scan_ac);
		}

		begin += step;
		step = modulo;
		if(add_one_count > 0)
		{
			step ++;
			add_one_count --;
		}
	}

    //init the delete thread data
	del_datas = hiarray_create(delete_threads_count, 
		sizeof(del_keys_data));
    if(del_datas == NULL)
    {
        log_stdout("error: out of memory");
        goto done;
    }

	modulo = node_count/delete_threads_count;
	remainder = node_count%delete_threads_count;
    add_one_count = remainder;
	begin = 0;
	step = modulo;
	if(add_one_count > 0)
	{
		step ++;
		add_one_count --;
	}
    
	for(i = 0; i < delete_threads_count; i ++)
	{
		del_data = hiarray_push(del_datas);
		ret = del_keys_data_init(del_data);
		if(ret != RCT_OK)
		{
			goto done;
		}

        del_data->nodes_count = step;
		for(k = begin; k < begin + step; k ++)
		{
			node_data = hiarray_get(node_datas, k);
            node_data->del_data = del_data;
            redisAeAttach(del_data->loop, node_data->del_ac);
		}

		begin += step;
		step = modulo;
		if(add_one_count > 0)
		{
			step ++;
			add_one_count --;
		}
	}

    //check the delete job
	for(i = 0; i < delete_threads_count; i ++)
	{
		del_data = hiarray_get(del_datas, i);
        del_threads_hold_node_count += del_data->nodes_count;
        print_del_keys_data(del_data, LOG_INFO);
	}

    if(del_threads_hold_node_count != node_count)
    {
        log_stdout("error: delete threads hold node count is wrong");
        goto done;
    }

	//check the scan job
	for(i = 0; i < scan_threads_count; i ++)
	{
		scan_data = hiarray_get(scan_datas, i);
        scan_threads_hold_node_count += scan_data->nodes_count;
        print_scan_keys_data(scan_data, LOG_INFO);
	}

    if(scan_threads_hold_node_count != node_count)
    {
        log_stdout("error: scan threads hold node count is wrong");
        goto done;
    }

    t_start = time(NULL);

    //run the delete job
	for(i = 0; i < delete_threads_count; i ++)
	{
		del_data = hiarray_get(del_datas, i);

		pthread_create(&del_data->thread_id, 
        	NULL, event_run, del_data->loop);
	}

	//run the scan job
	for(i = 0; i < scan_threads_count; i ++)
	{
		scan_data = hiarray_get(scan_datas, i);

		//pthread_create(&scan_data->thread_id, 
        //	NULL, scan_keys_job, scan_data->nodes_data);
        pthread_create(&scan_data->thread_id, 
        	NULL, scan_keys_job_run, scan_data);
	}

    log_stdout("delete keys job is running...");

	//wait for the scan job finish
	for(i = 0; i < scan_threads_count; i ++)
	{
		scan_data = hiarray_get(scan_datas, i);
		pthread_join(scan_data->thread_id, NULL);
	}

	//wait for the delete job finish
	for(i = 0; i < delete_threads_count; i ++)
	{
		del_data = hiarray_get(del_datas, i);
		pthread_join(del_data->thread_id, NULL);
	}

    t_end = time(NULL);

    for(i = 0; i < node_count; i ++)
    {
        node_data = hiarray_get(node_datas, i);
        deleted_keys_num += node_data->deleted_keys_num;
    }

	log_stdout("delete keys job finished, deleted: %lld keys, used: %d s", 
        deleted_keys_num, (int)difftime(t_end,t_start));

done:

    if(node_datas != NULL)
    {
        while(hiarray_n(node_datas) > 0)
        {
            node_data = hiarray_pop(node_datas);
            del_keys_node_deinit(node_data);
        }
        
        hiarray_destroy(node_datas);
    }

	if(scan_datas != NULL)
    {
        while(hiarray_n(scan_datas) > 0)
        {
            scan_data = hiarray_pop(scan_datas);
            scan_keys_data_deinit(scan_data);
        }
        
        hiarray_destroy(scan_datas);
    }

	if(del_datas != NULL)
    {
        while(hiarray_n(del_datas) > 0)
        {
            del_data = hiarray_pop(del_datas);
            del_keys_data_deinit(del_data);
        }
        
        hiarray_destroy(del_datas);
    }
}

int core_core(rctContext *ctx)
{
    redisClusterContext *cc = NULL;
    char * type;
    int start, end;
    int command_type;
    dictEntry *di;
    RCTCommand *command;
    sds arg;
    int args_num;
    int flags;

    di = dictFind(ctx->commands, ctx->cmd);
    if(di == NULL)
    {
        log_stdout("ERR: command [%s] not found, please read the help.", ctx->cmd);
        return RCT_ERROR;
    }

    command = dictGetEntryVal(di);
    if(command == NULL)
    {
        return RCT_ERROR;
    }

    if(command->flag & CMD_FLAG_NEED_CONFIRM)
    {
        log_stdout("Do you really want to execute the \"%s\"?", command->name);
        char confirm_input[5] = {0};
        int confirm_retry = 0;

        while(strcmp("yes", confirm_input))
        {
            if(strcmp("no", confirm_input) == 0)
            {
                return RCT_OK;
            }

            if(confirm_retry > 3)
            {
                log_stdout("ERR: Your input is always error!");
                return RCT_OK;
            }
            
            memset(confirm_input, '\0', 5);
            
            log_stdout("please input \"yes\" or \"no\" :");
            scanf("%s", &confirm_input);
            confirm_retry ++;
        }
    }

    if (ctx->address == NULL && 
        !(command->flag & CMD_FLAG_NOT_NEED_ADDRESS)) {
        log_stdout("Error: address is need.");
        goto done;
    }

    if (command->flag & CMD_FLAG_NEED_SYNCHRONOUS) {
    
        struct timeval timeout = { 3, 5000 };//3.005s

        if(ctx->redis_role == RCT_REDIS_ROLE_ALL 
            || ctx->redis_role == RCT_REDIS_ROLE_SLAVE)
        {
            flags = HIRCLUSTER_FLAG_ADD_SLAVE;
        }

        cc = redisClusterConnect(ctx->address, flags);
        //cc = redisClusterConnectAllWithTimeout(addr, timeout, flags);
        if(cc == NULL || cc->err)
        {
            log_stdout("connect error : %s", cc == NULL ? "NULL" : cc->errstr);
            goto done;
        }

        ctx->cc = cc;

        redisClusterSetMaxRedirect(cc, 1);
    }

    args_num = hiarray_n(&ctx->args);
    if(command->min_arg_count < 0) command->min_arg_count = 0;
    if(command->max_arg_count < 0) command->max_arg_count = 2147483647;
    if(args_num < command->min_arg_count || args_num > command->max_arg_count)
    {
        if(command->max_arg_count == 0)
        {
            log_stdout("ERR: command [%s] can not have argumemts", ctx->cmd);
        }
        else if(command->max_arg_count == command->min_arg_count)
        {
            log_stdout("ERR: command [%s] must have %d argumemts.", 
                ctx->cmd, command->min_arg_count);
        }
        else
        {
            log_stdout("ERR: the argumemts number for command [%s] must between %d and %d.", 
                ctx->cmd, command->min_arg_count, command->max_arg_count);
        }
        return RCT_ERROR;
    }

    command->proc(ctx, command->type);

done:
    
    if(cc != NULL)
    {
        redisClusterFree(cc);
        cc = NULL;
    }
    
    return RCT_OK;
}

