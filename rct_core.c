#include "rct_core.h"

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
    const redis_node *s1 = t1, *s2 = t2;

    return s1->slot_num_move > s2->slot_num_move?1:-1;
}

static int
redis_node_config_value_cmp(const void *t1, const void *t2)
{
    const redis_node *s1 = t1, *s2 = t2;

    return sdscmp(s1->config_value, s2->config_value);
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
	list *slots;
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

void cluster_rebalance(rctContext *ctx, int type)
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
	list *slaves;
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
			log_stdout("node[%s] holds %d slots_region and %d slots\t%d%s", statistics_node->addr, 
				statistics_node->slot_region_num_now, statistics_node->slot_num_now, 
				(statistics_node->slot_num_now*100)/total_slot_num,"%");
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
			c = ctx_get_by_node(node, NULL, cc->flags);
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
	

	c = ctx_get_by_node(node, NULL, cc->flags);
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
	
	c = ctx_get_by_node(node, NULL, cc->flags);
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

	c = ctx_get_by_node(node, NULL, cc->flags);
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
	list *slaves;
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
	
	c = ctx_get_by_node(node, NULL, cc->flags);
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
	list *slaves;
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

void cluster_del_keys(rctContext *ctx, int type)
{
	
	
	
}

int core_core(rctContext *ctx)
{
	redisClusterContext *cc = NULL;
	char * addr;
	char * type;
	int start, end;
	int command_type;
	dictEntry *di;
	RCTCommand *command;
	sds arg;
	int args_num;
	int flags;
	
	addr = ctx->address;
	
	struct timeval timeout = { 3, 5000 };//3.005s

	if(ctx->redis_role == RCT_REDIS_ROLE_ALL 
		|| ctx->redis_role == RCT_REDIS_ROLE_SLAVE)
	{
		flags = HIRCLUSTER_FLAG_ADD_SLAVE;
	}

	cc = redisClusterConnect(addr, flags);
	//cc = redisClusterConnectAllWithTimeout(addr, timeout, flags);
	if(cc == NULL || cc->err)
	{
		log_stdout("connect error : %s", cc == NULL ? "NULL" : cc->errstr);
		goto done;
	}

	ctx->cc = cc;

	redisClusterSetMaxRedirect(cc, 1);


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
		log_stdout("Do you really do the %s?", command->name);
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

	args_num = hiarray_n(&ctx->args);
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

