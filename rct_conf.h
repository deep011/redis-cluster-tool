#ifndef _RCT_CONF_H_
#define _RCT_CONF_H_

#define REDIS_GROUP_TYPE_REDIS_CLUSTER 1
#define REDIS_GROUP_TYPE_SINGLE 2

typedef struct rct_conf {
    sds           fname;             /* File name, absolute path. */

    int type;

    long long total_maxmemory;
    int total_master_count_with_slots;
    long long every_node_maxmemory;
    int every_node_maxclients;

    struct hiarray *nodes;  /* Element is struct redis_instance .*/
}rct_conf;

rct_conf *rct_conf_create_from_string(struct hiarray *configs);
rct_conf *rct_conf_create_from_file(char * filename);
void rct_conf_destroy(rct_conf *cf);

sds generate_conf_info_string(struct hiarray *nodes);
int dump_conf_file(char *filename, sds info_str);

void rct_conf_debug_show(rct_conf *cf);

#endif
