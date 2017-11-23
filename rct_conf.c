#include "rct_core.h"
#include "rct_conf.h"

#include <math.h>
#include <fcntl.h>
#include <unistd.h>

static int conf_init(rct_conf *cf)
{
    int ret;

    if (cf == NULL) {
        return RCT_ERROR;
    }

    cf->fname = NULL;

    cf->type = REDIS_GROUP_TYPE_REDIS_CLUSTER;

    cf->total_maxmemory = 0;
    cf->total_master_count_with_slots = 0;
    cf->every_node_maxmemory = 0;
    cf->every_node_maxclients = 0;

    cf->nodes = hiarray_create(10, sizeof(redis_instance));
    if(cf->nodes == NULL){
        log_error("ERROR: Out of memory");
        return RCT_ERROR;
    }
    
    return RCT_OK;
}

static void conf_deinit(rct_conf *cf)
{
    if (cf == NULL) {
        return;
    }

    if (cf->fname != NULL) {
        sdsfree(cf->fname);
        cf->fname = NULL;
    }

    if (cf->nodes != NULL) {
        while (hiarray_n(cf->nodes) > 0) {
            redis_instance *master = hiarray_pop(cf->nodes);
            redis_instance_deinit(master);
        }
        
        hiarray_destroy(cf->nodes);
    }
}

static int assign_master_slots(struct hiarray *nodes)
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

static int rct_parse_config_from_string(rct_conf *cf, struct hiarray *configs)
{
    int i, k;
    int ret;
    sds *str;
    struct hiarray *nodes = NULL;
    redis_instance *master, *slave, *node, *node_slave;
    int master_count;
    sds *master_slaves = NULL, *slaves_str = NULL;
    int master_slaves_count, slaves_str_count;

    master_count = hiarray_n(configs);
    if(master_count < 3){
        log_error("ERROR: there must have at least three masters in the config string.");
        return RCT_ERROR;
    }

    nodes = hiarray_create(master_count, sizeof(redis_instance));
    if(nodes == NULL){
        log_stdout("ERROR: out of memory.");
        goto error;
    }

    for(i = 0; i < master_count; i ++){
        str = hiarray_get(configs, i);
        master_slaves = sdssplitlen(*str, sdslen(*str), "[", 1, &master_slaves_count);
        if(master_slaves == NULL || (master_slaves_count != 1 && 
            master_slaves_count != 2)){
            log_stderr("ERROR: the address is error in the config string.");
            goto error;
        }
        
        master = hiarray_push(nodes);
        ret = redis_instance_init(master, master_slaves[0], RCT_REDIS_ROLE_MASTER);
        if(ret != RCT_OK){
            log_stdout("ERROR: init redis master instance error in the config string.");
            goto error;
        }

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

    assign_master_slots(nodes);

    cf->nodes = nodes;
    
    return RCT_OK;

error:

    if (master_slaves != NULL)
        sdsfreesplitres(master_slaves, master_slaves_count);

    if (slaves_str != NULL)
        sdsfreesplitres(slaves_str, master_slaves_count);

    if(nodes != NULL){
        while(hiarray_n(nodes) > 0){
            master = hiarray_pop(nodes);
            redis_instance_deinit(master);
        }
        
        hiarray_destroy(nodes);
    }

    return RCT_ERROR;
}

rct_conf *rct_conf_create_from_string(struct hiarray *configs)
{
    int ret;
    rct_conf *cf = NULL;

    if (configs == NULL) {
        log_error("ERROR: configuration string is NULL.");
        return NULL;
    }
    
    cf = rct_alloc(sizeof(*cf));
    if (cf == NULL) {
        goto error;
    }

    ret = conf_init(cf);
    if (ret != RCT_OK) {
        goto error;
    }

    ret = rct_parse_config_from_string(cf, configs);
    if (ret != RCT_OK) {
        goto error;
    }

    return cf;

error:

    if (cf != NULL) {
        rct_conf_destroy(cf);
    }
    
    return NULL;
}

static int rct_parse_config_from_file(rct_conf *cf, FILE *fh)
{
    int ret;
    int idx;
    char line[256];
    sds str = NULL;
    sds *fields = NULL;
    int fields_count = 0;
    sds *key_value = NULL;
    int key_value_count = 0;
    redis_instance *node = NULL;
    redis_instance *last_master = NULL;
    int slots_weight;
    int node_is_master = 0;
    
    while (!feof(fh)) {
        if (fgets(line,256,fh) == NULL) {
            if(feof(fh)) break;

            log_error("ERROR: Read a line from conf file %s failed: %s", 
                cf->fname, strerror(errno));
            goto error;
        }

        if(str == NULL){
            str = sdsnew(line);
        } else {
            sdsclear(str);
            str = sdsMakeRoomFor(str, strlen(line)+1);
            str = sdscat(str, line);
        }
        
        sdstrim(str, " \n");

        if (sdslen(str) == 0) continue;

        log_debug(LOG_DEBUG, "%s", str);
        
        if (fields != NULL) {
            sdsfreesplitres(fields, fields_count);
            fields = NULL;
            fields_count = 0;
        }

        fields = sdssplitlen(str, sdslen(str), ",", 1, &fields_count);
        if (fields == NULL || fields_count <= 0)
            goto error;

        node_is_master = 0;

        if (fields_count == 1) {
            if (key_value!= NULL) {
                sdsfreesplitres(key_value, key_value_count);
                key_value = NULL;
                key_value_count = 0;
            }
            
            key_value = sdssplitlen(fields[0],sdslen(fields[0]),"=",1,&key_value_count);
            if (key_value == NULL || key_value_count != 2) {
                log_error("ERROR: failed to get key value from the config file.");
                goto error;
            }

            log_debug(LOG_DEBUG, "%s %s", key_value[0], key_value[1]);

            //This is master.
            if (!sds_compare_to_string(key_value[0], "master", -1)) {
                last_master = hiarray_push(cf->nodes);
                ret = redis_instance_init(last_master, key_value[1], RCT_REDIS_ROLE_MASTER);
                if(ret != RCT_OK){
                    log_stdout("ERROR: Init redis master instance(%s) error in the config file.", key_value[1]);
                    goto error;
                }
                
                node_is_master = 1;
            //This is slave.
            } else if (!sds_compare_to_string(key_value[0], "slave", -1)) {
                node = redis_instance_create(key_value[1],RCT_REDIS_ROLE_SLAVE);
                
                if (last_master == NULL) {
                    log_error("ERROR: there must have a master line before the slave(%s) in the config file.", key_value[1]);
                    goto error;
                }

                listAddNodeTail(last_master->slaves, node);
            //This is config : total_maxmemory
            } else if (!sds_compare_to_string(key_value[0], "total_maxmemory", -1)) {

            
            //This is config : total_master_count_with_slots
            } else if (!sds_compare_to_string(key_value[0], "total_master_count_with_slots", -1)) {
                if (!str_is_integer(key_value[1], sdslen(key_value[1]))) {
                    log_error("ERROR: total_master_count_with_slots must be a number in the config file.");
                    goto error;
                }

                cf->total_master_count_with_slots = rct_atoi(key_value[1]);
            //This is config : every_node_maxmemory
            } else if (!sds_compare_to_string(key_value[0], "every_node_maxmemory", -1)) {
                
            
            //This is config : every_node_maxclients
            } else if (!sds_compare_to_string(key_value[0], "every_node_maxclients", -1)) {
                if (!str_is_integer(key_value[1], sdslen(key_value[1]))) {
                    log_error("ERROR: every_node_maxclients must be a number in the config file.");
                    goto error;
                }

                cf->every_node_maxclients = rct_atoi(key_value[1]);
            } else {
                log_error("ERROR: config '%s' is not supported in the config file.", key_value[0]);
                goto error;
            }

            continue;
        }

        slots_weight = -1;
        //This is a node.
        for (idx = 0; idx < fields_count; idx ++) {
            if (key_value!= NULL) {
                sdsfreesplitres(key_value, key_value_count);
                key_value = NULL;
                key_value_count = 0;
            }
            
            key_value = sdssplitlen(fields[idx],sdslen(fields[idx]),"=",1,&key_value_count);
            if (key_value == NULL || key_value_count != 2) {
                log_error("ERROR: failed to get key value from the config file.");
                goto error;
            }

            log_debug(LOG_DEBUG, "%s %s", key_value[0], key_value[1]);

            

            //This is master
            if (!sds_compare_to_string(key_value[0], "master", -1)) {
                last_master = hiarray_push(cf->nodes);
                ret = redis_instance_init(last_master, key_value[1], RCT_REDIS_ROLE_MASTER);
                if(ret != RCT_OK){
                    log_stdout("ERROR: Init redis master instance(%s) error in the config file.", key_value[1]);
                    goto error;
                }
                
                node_is_master = 1;
            } else if (!sds_compare_to_string(key_value[0], "slave", -1)) {
                node = redis_instance_create(key_value[1],RCT_REDIS_ROLE_SLAVE);

                if (last_master == NULL) {
                    log_error("ERROR: there must have a master line before the slave(%s) in the config file.", key_value[1]);
                    goto error;
                }

                listAddNodeTail(last_master->slaves, node);
            } else if (!sds_compare_to_string(key_value[0], "slots_weight", -1)) {
                if (!str_is_integer(key_value[1], sdslen(key_value[1]))) {
                    log_error("ERROR: master slots_weight must be a number in the config file.");
                    goto error;
                }
                
                slots_weight = rct_atoi(key_value[1]);
                if (slots_weight < 0) {
                    log_error("ERROR: master slots_weight must be bigger then zero in the config file.");
                    goto error;
                }
            } else {
                log_error("ERROR: config '%s' is not supported in the config file.", key_value[0]);
                goto error;
            }
        }
        
        if (node_is_master == 1 && slots_weight >= 0) {
            last_master->slots_weight = slots_weight;
        }
    }


    if (str != NULL) sdsfree(str);
    if (fields != NULL) sdsfreesplitres(fields, fields_count);
    if (key_value!= NULL) sdsfreesplitres(key_value, key_value_count);

    assign_master_slots(cf->nodes);
    
    return RCT_OK;

error:

    if (str != NULL) sdsfree(str);
    if (fields != NULL) sdsfreesplitres(fields, fields_count);
    if (key_value!= NULL) sdsfreesplitres(key_value, key_value_count);
    
    return RCT_ERROR;
}

rct_conf *rct_conf_create_from_file(char * filename)
{
    int ret;
    rct_conf *cf = NULL;
    FILE *fh = NULL;
    sds path = NULL;

    if (filename == NULL) {
        log_error("ERROR: configuration file name is NULL.");
        return NULL;
    }

    path = get_absolute_path(filename);
    if (path == NULL) {
        log_error("ERROR: configuration file name '%s' is error.", filename);
        goto error;
    }

    fh = fopen(path, "r");
    if (fh == NULL) {
        log_error("ERROR: failed to open configuration '%s': %s", path,
                  strerror(errno));
        goto error;
    }
    
    cf = rct_alloc(sizeof(*cf));
    if (cf == NULL) {
        goto error;
    }

    ret = conf_init(cf);
    if (ret != RCT_OK) {
        goto error;
    }

    ret = rct_parse_config_from_file(cf, fh);
    if (ret != RCT_OK) {
        goto error;
    }

    cf->fname = path;

    fclose(fh);
    
    return cf;

error:

    if(fh != NULL) {
        fclose(fh);
    }

    if (cf != NULL) {
        rct_conf_destroy(cf);
    }

    if (path != NULL) {
        sdsfree(path);
    }
    
    return NULL;
}

void rct_conf_destroy(rct_conf *cf)
{
    if (cf == NULL) {
        return;
    }
    
    conf_deinit(cf);
    
    rct_free(cf);
}

sds generate_conf_info_string(struct hiarray *nodes)
{
    sds info_str = sdsempty();
    int i;
    
    if (nodes == NULL) {
        log_stderr("ERROR: nodes is NULL.");
        return NULL;
    }

    hiarray_sort(nodes, redis_instance_array_addr_cmp);

    for (i = 0; i < hiarray_n(nodes); i++) {
        redis_instance *master;

        master = hiarray_get(nodes, i);
        info_str = sdscatfmt(info_str,"master=%s:%i,slots_weight=%i\r\n", master->host, master->port, master->slots_count);

        if (master->slaves) {
            listIter *it = listGetIterator(master->slaves, AL_START_HEAD);
            listNode *ln;
            redis_instance *slave;
            while((ln = listNext(it)) != NULL){
                slave = listNodeValue(ln);
                info_str = sdscatfmt(info_str,"slave=%s:%i\r\n", slave->host, slave->port);
            }
            
            listReleaseIterator(it);
        }
    }

    return info_str;
}

int dump_conf_file(char *filename, sds info_str)
{
    int fd = -1;
    ssize_t len = 0;

    if (filename == NULL) {
        log_stdout("\r\nconf file:\r\n");
        log_stdout("%s", info_str);
        return RCT_OK;
    }

    if (access(filename, F_OK) == 0) {
        sds filename_bak = sdsnew(filename);
        filename_bak = sdscatfmt(filename_bak, ".%I", rct_usec_now());
        if (rename(filename,filename_bak) != 0) {
            log_stdout("WARN: The conf file is already exist, and can not rename to a bak file.");
        }
        sdsfree(filename_bak);
    }

    fd = open(filename, O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) {
        log_stderr("ERROR: open conf file %s failed: %s", filename, strerror(errno));
        return RCT_ERROR;
    }

    len = write(fd, info_str, sdslen(info_str));
    if (len < 0) {
        log_stderr("ERROR: write conf file %s failed: %s", filename, strerror(errno));
        close(fd);
        return RCT_ERROR;
    } else if (len != sdslen(info_str)) {
        log_stderr("ERROR: write not complete of conf file %s failed", filename);
        close(fd);
        return RCT_ERROR;
    }

    close(fd);
    
    return RCT_OK;
}

void rct_conf_debug_show(rct_conf *cf)
{
#ifdef RCT_DEBUG_LOG
    
    if (cf == NULL) {
        log_stderr("ERROR: conf is NULL.");
        return;
    }

    log_stdout("config file: %s", cf->fname == NULL?"NULL":cf->fname);
    log_stdout("config total_maxmemory: %lld", cf->total_maxmemory);
    log_stdout("config total_master_count_with_slots: %d", cf->total_master_count_with_slots);
    log_stdout("config every_node_maxmemory: %lld", cf->every_node_maxmemory);
    log_stdout("config every_node_maxclients: %d", cf->every_node_maxclients);
    
    log_stdout("config nodes:");
    rct_redis_instance_array_debug_show(cf->nodes);

#endif
}
