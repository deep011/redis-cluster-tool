#include "rct_core.h"

typedef struct redis_node{
	sds name;
	sds addr;
	int slot_num_now;
	int slot_num_move;
	int slot_region_num_now;
	int key_num;
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
		printf("start : %d\n", (*slot)->start);
		printf("end : %d\n", (*slot)->end);
		printf("name : %s\n", (*slot)->node->name);
		printf("host : %s\n", (*slot)->node->host);
		printf("port : %d\n", (*slot)->node->port);
		printf("addr : %s\n", (*slot)->node->addr);
		printf("master : %d\n", (*slot)->node->master);
		printf("context : %d\n\n", (*slot)->node->con?1:0);
		printf("asyncContext : %d\n\n", (*slot)->node->acon?1:0);
	}	

	printf("total solts region num : %d\n", num);
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
		printf("node[%s] holds %d slots_region and %d slots\n", node->addr, slots_count, slot_count);
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
		//printf("%s\t%d\t%d\n", node_to_reshard->name, node_to_reshard->slot_num_now, node_to_reshard->slot_num_move);
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
				//printf("%s\t%d\t%d\n", node_to_reshard->name, node_to_reshard->slot_num_now, node_to_reshard->slot_num_move);
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
		printf("--from %s --to %s --slots %d\n", node_reshard_from->name, node_reshard_to->name, slot_num_to_move);
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
			printf("%s\n", node->name==NULL?"NULL":node->name);
		}
	}

	dictReleaseIterator(di);
}

void show_nodes_hold_slot_num(rctContext *ctx, int type)
{
	nodes_hold_slot_num(ctx->cc->nodes, 1);
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
			printf("node[%s] holds %d slots_region and %d slots\t%d\%\n", statistics_node->addr, 
				statistics_node->slot_region_num_now, statistics_node->slot_num_now, 
				(statistics_node->slot_num_now*100)/total_slot_num);
		}
		
		printf("\ncluster holds %d slots\n", total_slot_num);
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
				printf("node[%s] get connect failed\n", node->addr);
				continue;
			}
			
			reply = redisCommand(c, "info Keyspace");
			if(reply == NULL)
			{
				printf("node[%s] get reply null()\n", node->addr, cc->errstr);
				continue;
			}
			
			//printf("reply->type : %d\n", reply->type);
			
			if(reply->type != REDIS_REPLY_STRING)
			{
				printf("error: reply type error!\n");
				goto done;
			}
			
			//printf("reply->str : %s\n", reply->str);
			//print_string_with_length_fix_CRLF(reply->str, reply->len);
			line = sdssplitlen(reply->str, reply->len, "\r\n", 2, &line_len);
			if(line == NULL)
			{
				printf("error: line split error(null)!\n");
				goto done;
			}
			else if(line_len == 2)
			{
				printf("node[%s] has %d keys\n", node->addr, 0);
				sdsfreesplitres(line, line_len);
				line = NULL;
				continue;
			}
			else if(line_len != 3)
			{
				printf("error: line split error(line_len != 3)!\n");
				goto done;
			}
			
			part = sdssplitlen(line[1], sdslen(line[1]), ",", 1, &part_len);
			if(line == NULL || line_len != 3)
			{
				printf("error: part split error!\n");
				goto done;
			}
			
			partchild = sdssplitlen(part[0], sdslen(part[0]), "=", 1, &partchild_len);
			if(partchild == NULL || partchild_len != 2)
			{
				printf("error: partchild split error!\n");
				goto done;
			}
			
			node_keys_num = rct_atoi(partchild[1]);
			
			if(node_keys_num < 0)
			{
				goto done;
			}
			
			printf("node[%s] has %d keys\n", node->addr, node_keys_num);
			
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
    
	
	printf("cluster has %d keys\n", nodes_keys_num);
	
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

int node_key_num(redisClusterContext *cc, cluster_node *node, int isprint)
{
	int key_num = 0;
	sds *line = NULL, *part = NULL, *partchild = NULL;
	int line_len = 0, part_len = 0, partchild_len = 0;
	redisContext *c = NULL;
	redisReply *reply = NULL;
	
	if(node == NULL || node->slots == NULL)
	{
		return -1;
	}
	
	if(listLength(node->slots))
	{
		c = ctx_get_by_node(node, NULL, cc->flags);
		if(c == NULL)
		{	
			if(isprint)
			{
				printf("node[%s] get connect failed\n", node->addr);
			}
			key_num = -1;
			goto done;
		}
		
		reply = redisCommand(c, "info Keyspace");
		if(reply == NULL)
		{
			if(isprint)
			{
				printf("node[%s] get reply null()\n", node->addr, c->errstr);
			}
			key_num = -1;
			goto done;
		}
		
		if(reply->type != REDIS_REPLY_STRING)
		{
			if(isprint)
			{
				printf("error: reply type error!\n");
			}
			key_num = -1;
			goto done;
		}
		
		line = sdssplitlen(reply->str, reply->len, "\r\n", 2, &line_len);
		if(line == NULL)
		{
			if(isprint)
			{
				printf("error: line split error(null)!\n");
			}
			key_num = -1;
			goto done;
		}
		else if(line_len == 2)
		{
			key_num = 0;
			if(isprint)
			{
				printf("node[%s] has %d keys\n", node->addr, key_num);
			}
			goto done;
		}
		else if(line_len != 3)
		{
			if(isprint)
			{
				printf("error: line split error(line_len != 3)!\n");
			}
			goto done;
		}
		
		part = sdssplitlen(line[1], sdslen(line[1]), ",", 1, &part_len);
		if(line == NULL || line_len != 3)
		{
			if(isprint)
			{
				printf("error: part split error!\n");
			}
			goto done;
		}
		
		partchild = sdssplitlen(part[0], sdslen(part[0]), "=", 1, &partchild_len);
		if(partchild == NULL || partchild_len != 2)
		{
			if(isprint)
			{
				printf("error: partchild split error!\n");
			}
			goto done;
		}
		
		key_num = rct_atoi(partchild[1]);
		
		if(key_num < 0)
		{
			goto done;
		}
		
		if(isprint)
		{
			printf("node[%s] has %d keys\n", node->addr, key_num);
		}
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
	
	if(node == NULL || node->slots == NULL)
	{
		return -1;
	}
	
	if(listLength(node->slots))
	{
		c = ctx_get_by_node(node, NULL, cc->flags);
		if(c == NULL)
		{	
			if(isprint)
			{
				printf("node[%s] get connect failed\n", node->addr);
			}
			memory_size = -1;
			goto done;
		}
		
		reply = redisCommand(c, "info Memory");
		if(reply == NULL)
		{
			if(isprint)
			{
				printf("node[%s] get reply null()\n", node->addr, c->errstr);
			}
			memory_size = -1;
			goto done;
		}
		
		//printf("reply->type : %d\n", reply->type);
		
		if(reply->type != REDIS_REPLY_STRING)
		{
			if(isprint)
			{
				printf("error: reply type error!\n");
			}
			memory_size = -1;
			goto done;
		}
		
		//printf("reply->str : %s\n", reply->str);
		//print_string_with_length_fix_CRLF(reply->str, reply->len);
		line = sdssplitlen(reply->str, reply->len, "\r\n", 2, &line_len);
		if(line == NULL || line_len <= 0)
		{
			if(isprint)
			{
				printf("error: line split error(null)!\n");
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
			printf("error: key_value split error or used_memory not found!\n");
		}
		memory_size = -1;
	}
	
done:
	if(isprint)
	{
		printf("node[%s] used %lld M\n", node->addr, memory_size);
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

sds node_cluster_state(redisClusterContext *cc, cluster_node *node, int isprint)
{
	sds cluster_state = NULL;
	sds *line = NULL, *key_value = NULL;
	int line_len = 0, key_value_len = 0;
	redisContext *c = NULL;
	redisReply *reply = NULL;
	int i;
	
	if(node == NULL || node->slots == NULL)
	{
		return NULL;
	}

	c = ctx_get_by_node(node, NULL, cc->flags);
	if(c == NULL)
	{	
		if(isprint)
		{
			printf("error: node[%s] get connect failed\n", node->addr);
		}
		goto done;
	}
	
	reply = redisCommand(c, "cluster info");
	if(reply == NULL)
	{
		if(isprint)
		{
			printf("error: node[%s] get reply null()\n", node->addr, c->errstr);
		}
		goto done;
	}
	
	if(reply->type != REDIS_REPLY_STRING)
	{
		if(isprint)
		{
			printf("error: reply type error!\n");
		}
		goto done;
	}
	
	line = sdssplitlen(reply->str, reply->len, "\r\n", 2, &line_len);
	if(line == NULL || line_len <= 0)
	{
		if(isprint)
		{
			printf("error: line split error(null)!\n");
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
		printf("error: key_value split error or cluster_state not found!\n");
	}
	
done:
	if(isprint && cluster_state != NULL)
	{
		printf("node[%s] cluster_state is %s \n", node->addr, cluster_state?cluster_state:"NULL");
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

void nodes_get_state(rctContext *ctx, int type)
{
	redisClusterContext *cc = ctx->cc;
	node_state_type_t state_type = type;
	dictIterator *di;
    dictEntry *de;
	
	dict *nodes;
	struct cluster_node *node;
	redisContext *c = NULL;
	redisReply *reply = NULL;
	int i, nodes_count = 0;
	struct hiarray *statistics_nodes = NULL;
	redis_node *statistics_node = NULL;
	int node_keys_num = 0, nodes_keys_num = 0;
	long long memory_size = 0, memory_size_all = 0;
	sds cluster_state;
	int all_cluster_state_is_ok = 1;
	
	
	
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
	
	statistics_nodes = hiarray_create(nodes_count, sizeof(*statistics_node));
	if(statistics_nodes == NULL)
	{
		return;
	}
	
	di = dictGetIterator(nodes);
	
	while((de = dictNext(di)) != NULL) {
        node = dictGetEntryVal(de);
		
		switch(state_type)
		{
		case REDIS_KEY_NUM:
			statistics_node = hiarray_push(statistics_nodes);
		
			statistics_node->addr = node->addr;
		
			node_keys_num = node_key_num(cc, node, 0);			
			statistics_node->key_num = node_keys_num;
			
			if(node_keys_num < 0)
			{
				continue;
			}
			
			nodes_keys_num += node_keys_num;
			break;
		case REDIS_MEMORY:
			statistics_node = hiarray_push(statistics_nodes);
		
			statistics_node->addr = node->addr;
			
			memory_size = node_memory_size(cc, node, 0);
			statistics_node->used_memory = memory_size/1048576;
			
			if(statistics_node->used_memory < 0)
			{
				continue;
			}
			
			memory_size_all += statistics_node->used_memory;
			break;
		case NODES_CLUSTER_STATE:
			
			cluster_state = node_cluster_state(cc, node, 1);
			if(cluster_state == NULL)
			{
				all_cluster_state_is_ok = 0;
				continue;
			}
			
			if(strcmp(cluster_state, "ok") != 0)
			{
				all_cluster_state_is_ok = 0;
			}
			sdsfree(cluster_state);
			cluster_state = NULL;
			break;
		default:
			
			break;
        }
		
    }
    
	for(i = 0; i < hiarray_n(statistics_nodes); i ++)
	{
		statistics_node = hiarray_get(statistics_nodes, i);
		
		switch(state_type)
		{
		case REDIS_KEY_NUM:
			if(nodes_keys_num <= 0)
			{
				goto done;
			}
			
			printf("node[%s] has %d keys\t%d\%\n", statistics_node->addr, 
			statistics_node->key_num, (statistics_node->key_num*100)/nodes_keys_num);
			break;
		case REDIS_MEMORY:
			if(memory_size_all <= 0)
			{
				goto done;
			}
			
			printf("node[%s] used %lld M\t%d\%\n", statistics_node->addr, 
			statistics_node->used_memory, (statistics_node->used_memory*100)/memory_size_all);
			break;
		case NODES_CLUSTER_STATE:
			
			break;
		default:
			
			break;
        }
		
	}
	
done:
	
	switch(state_type)
	{
	case REDIS_KEY_NUM:
		printf("cluster has %d keys\n", nodes_keys_num);
		break;
	case REDIS_MEMORY:
		printf("cluster used %lld M\n", memory_size_all);
		break;
	case NODES_CLUSTER_STATE:
		if(all_cluster_state_is_ok)
		{
			printf("all nodes cluster_state is ok\n");
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

void do_command_node_by_node(rctContext *ctx, int type)
{
	redisClusterContext *cc = ctx->cc;
	redis_command_type_t cmd_type = type;
	sds command = NULL;
	dictIterator *di = NULL;
    dictEntry *de;
	dict *nodes;
	int nodes_count;
	struct cluster_node *node;
	redisContext *c = NULL;
	redisReply *reply = NULL, *sub_reply;
	int all_is_ok = 1;
	int all_is_consistent = 1;
	sds compare_value = NULL;
	long long sum = 0;
	
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

	switch(cmd_type)
	{
	case REDIS_COMMAND_FLUSHALL:
		command = sdsnew("flushall");
		if(command == NULL)
		{
			log_stderr("ERR: out of memory.");
			goto done;
		}
		break;
	case REDIS_COMMAND_CONFIG_GET:
		command = sdsnew("config get ");
		if(command == NULL)
		{
			log_stderr("ERR: out of memory.");
			goto done;
		}

		command = sdscatsds(command, *(sds*)hiarray_get(&ctx->args, 0));
		if(command == NULL)
		{
			log_stderr("ERR: out of memory.");
			goto done;
		}
		
		break;
	case REDIS_COMMAND_CONFIG_SET:
		command = sdsnew("config set ");
		if(command == NULL)
		{
			log_stderr("ERR: out of memory.");
			goto done;
		}

		command = sdscatsds(command, *(sds*)hiarray_get(&ctx->args, 0));
		if(command == NULL)
		{
			log_stderr("ERR: out of memory.");
			goto done;
		}

		command = sdscat(command, " ");
		if(command == NULL)
		{
			log_stderr("ERR: out of memory.");
			goto done;
		}

		command = sdscatsds(command, *(sds*)hiarray_get(&ctx->args, 1));
		if(command == NULL)
		{
			log_stderr("ERR: out of memory.");
			goto done;
		}
		
		break;
	case REDIS_COMMAND_CONFIG_REWRITE:
		command = sdsnew("config rewrite");
		if(command == NULL)
		{
			log_stderr("ERR: out of memory.");
			goto done;
		}
		
		break;
	default:
		
		break;
	}
	
	di = dictGetIterator(nodes);
	
	while((de = dictNext(di)) != NULL) {
        node = dictGetEntryVal(de);

		if(listLength(node->slots))
		{
			c = ctx_get_by_node(node, NULL, cc->flags);
			if(c == NULL)
			{	
				printf("node[%s] get connect failed\n", node->addr);
				continue;
			}
			
			reply = redisCommand(c, command);

			if(reply == NULL)
			{
				all_is_ok = 0;
				printf("node[%s] %s failed(reply is NULL)!\n", 
					node->addr, command);
			}
			else if(reply->type == REDIS_REPLY_ERROR)
			{
				all_is_ok = 0;
				printf("node[%s] %s failed(%s)!\n", 
					node->addr, command, reply->str);
			}
			else
			{
				switch(cmd_type)
				{
				case REDIS_COMMAND_FLUSHALL:
				case REDIS_COMMAND_CONFIG_SET:
				case REDIS_COMMAND_CONFIG_REWRITE:
					break;
				case REDIS_COMMAND_CONFIG_GET:
					if(reply->type != REDIS_REPLY_ARRAY)
					{
						all_is_ok = 0;
						log_stderr("ERR: command [%s] reply type error(want 2, but %d) for node %s.", 
							command, reply->type, node->addr);
						goto next;
					}

					if(reply->elements == 0)
					{
						all_is_ok = 0;
						log_stderr("ERR: node %s do not support this config [%s].", 
							node->addr, *(sds*)hiarray_get(&ctx->args, 0));
						goto next;
					}
					else if(reply->elements != 2)
					{
						all_is_ok = 0;
						log_stderr("ERR: command [%s] reply array len error(want 2, but %d) for node %s.", 
							command, reply->elements, node->addr);
						goto next;
					}

					sub_reply = reply->element[0];
					if(sub_reply == NULL || sub_reply->type != REDIS_REPLY_STRING)
					{
						all_is_ok = 0;
						log_stderr("ERR: command [%s] reply(config name) type error(want 1, but %d) for node %s.", 
							command, sub_reply->type, node->addr);
						goto next;
					}

					if(strcmp(sub_reply->str, *(sds*)hiarray_get(&ctx->args, 0)))
					{
						all_is_ok = 0;
						log_stderr("ERR: command [%s] reply config name is not %s for node %s.", 
							command, *(sds*)hiarray_get(&ctx->args, 0), node->addr);
						goto next;
					}

					sub_reply = reply->element[1];
					if(sub_reply == NULL || sub_reply->type != REDIS_REPLY_STRING)
					{
						all_is_ok = 0;
						log_stderr("ERR: command [%s] reply(config value) type error(want 1, but %d) for node %s.", 
							command, sub_reply->type, node->addr);
						goto next;
					}

					if(strcmp("maxmemory", *(sds*)hiarray_get(&ctx->args, 0)) == 0)
					{
						long long memory_num = rct_atoll(reply->element[1]->str, reply->element[1]->len);
						log_stderr("node %s config %s is %s (%lldMB)", node->addr, 
							reply->element[0]->str, reply->element[1]->str, 
							memory_num/(1024*1024));
						sum += memory_num;
					}
					else
					{
						log_stderr("node %s config %s is %s", node->addr, 
							reply->element[0]->str, reply->element[1]->str);
					}
					
					if(all_is_consistent == 0)
					{
						if(compare_value != NULL)
						{
							sdsfree(compare_value);
							compare_value = NULL;
						}
					}
					else
					{
						if(compare_value == NULL)
						{
							compare_value = sdsnewlen(sub_reply->str, sub_reply->len);
						}
						else
						{
							if(strcmp(compare_value, sub_reply->str))
							{
								all_is_consistent = 0;
							}
							else
							{
								compare_value = sdscpylen(compare_value, 
									sub_reply->str, sub_reply->len);
							}
						}
					}
					
					break;
				
				default:

					break;
				}
			}
next:		
			if(reply != NULL)
			{
				freeReplyObject(reply);
				reply = NULL;
			}
		}
		
	}
	
	switch(cmd_type)
	{
	case REDIS_COMMAND_FLUSHALL:
	case REDIS_COMMAND_CONFIG_SET:
	case REDIS_COMMAND_CONFIG_REWRITE:
		if(all_is_ok)
		{
			printf("OK\n");
		}
		else
		{
			printf("Others is OK\n");
		}
		
		break;
	case REDIS_COMMAND_CONFIG_GET:
		log_stderr("");
		if(all_is_ok)
		{
			if(all_is_consistent)
			{
				log_stderr("All nodes config are Consistent");
			}
			else
			{
				log_stderr("Nodes config are Inconsistent");
			}
		}
		else
		{
			if(all_is_consistent)
			{
				log_stderr("Other nodes config are Consistent");
			}
			else
			{
				log_stderr("Other nodes config are Inconsistent");
			}
		}
		
		if(strcmp("maxmemory", *(sds*)hiarray_get(&ctx->args, 0)) == 0)
		{
			log_stderr("cluster total maxmemory: %lld (%lldMB)", sum, sum/(1024*1024));
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

	if(compare_value != NULL)
	{
		sdsfree(compare_value);
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
        	printf("reply is null[%s]\n", cc->errstr);
			reply_null_count ++;
        	continue;
    	}
		
		switch(reply->type)
		{
		case REDIS_REPLY_STRING:
			
			//printf("%s\n", reply->str);
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
			//printf("null\n");
			break;
		case REDIS_REPLY_STATUS:
		
			//printf("%s\n", reply->str);
			if(strcmp(reply->str, "OK") == 0)
			{
				ok_count ++;
			}
			break;
		case REDIS_REPLY_ERROR:
			error_count ++;
			printf("%s\n", reply->str);
			break;
		default:
			break;
		}
		freeReplyObject(reply);
		reply = NULL;
	}
	
	printf("null_count: %d\n", null_count);
	printf("ok_count: %d\n", ok_count);
	printf("equal_count: %d\n", equal_count);
	printf("error_count: %d\n", error_count);
	printf("reply_null_count: %d\n", reply_null_count);

done:
	
	if(reply != NULL)
	{
		freeReplyObject(reply);
		reply = NULL;	
	}
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
	
	addr = ctx->address;
	
	struct timeval timeout = { 0, 5000 };//0.005s

	cc = redisClusterConnect(addr);
	//cc = redisClusterConnectWithTimeout(addr,timeout);
	if(cc == NULL || cc->err)
	{
		printf("connect error : %s\n", cc == NULL ? "NULL" : cc->errstr);
		goto done;
	}

	ctx->cc = cc;

	redisClusterSetMaxRedirect(cc, 1);


	di = dictFind(ctx->commands, ctx->cmd);
	if(di == NULL)
	{
		log_stderr("ERR: command [%s] not found, please read the help.", ctx->cmd);
		return RCT_ERROR;
	}

	command = dictGetEntryVal(di);
	if(command == NULL)
	{
		return RCT_ERROR;
	}

	if(command->flag & CMD_FLAG_NEED_CONFIRM)
	{
		log_stderr("Do you really do the %s?", command->name);
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
				log_stderr("ERR: Your input is always error!");
				return RCT_OK;
			}
			
			memset(confirm_input, '\0', 5);
			
			log_stderr("please input \"yes\" or \"no\" :");
			scanf("%s", &confirm_input);
			confirm_retry ++;
		}
	}

	args_num = hiarray_n(&ctx->args);
	if(args_num < command->min_arg_count || args_num > command->max_arg_count)
	{
		if(command->max_arg_count == 0)
		{
			log_stderr("ERR: command [%s] can not have argumemts", ctx->cmd);
		}
		else if(command->max_arg_count == command->min_arg_count)
		{
			log_stderr("ERR: command [%s] must have %d argumemts.", 
				ctx->cmd, command->min_arg_count);
		}
		else
		{
			log_stderr("ERR: the argumemts number for command [%s] must between %d and %d.", 
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

