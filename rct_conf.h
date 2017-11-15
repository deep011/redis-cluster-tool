#ifndef _RCT_CONF_H_
#define _RCT_CONF_H_

typedef struct rct_conf {
    sds           fname;             /* File name, absolute path. */
    FILE          *fh;               /* File handle. */

    long long total_maxmemory;
    int total_master_count_with_slots;
    long long every_node_maxmemory;
    int every_node_maxclients;

    struct hiarray *nodes;  /* Element is struct redis_instance .*/
}rct_conf;


rct_conf *rct_conf_create_from_string(struct hiarray *configs);
rct_conf *rct_conf_create_from_file(char * filename);
void rct_conf_destroy(rct_conf *cf);

void rct_dump_conf(rct_conf *cf);

#endif
