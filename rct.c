
#include "rct_core.h"

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
    dictSdsHash,   	   			/* hash function */
    NULL,                      	/* key dup */
    NULL,                      	/* val dup */
    dictSdsKeyCompare,     		/* key compare */
    dictSdsDestructor,         	/* key destructor */
    NULL                       	/* val destructor */
};

rctContext *
init_context(struct instance *nci)
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

	commands = dictCreate(&commandTableDictType,NULL);
	if(commands == NULL)
	{
		rct_free(rct_ctx);
		return NULL;
	}

	populateCommandTable(commands);
	rct_ctx->commands = commands;

	rct_ctx->address = nci->addr;

	cmd_parts = sdssplitlen(nci->command, strlen(nci->command), " ", 1, &cmd_parts_count);
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
	/*
	if(rct_ctx->cmd == NULL)
	{
		rct_free(rct_ctx);
		dictRelease(commands);
		return NULL;
	}
	*/

	return rct_ctx;
}

void destroy_context(rctContext *rct_ctx)
{
	while(hiarray_n(&rct_ctx->args) > 0)
	{
		sds *arg = hiarray_pop(&rct_ctx->args);
		sdsfree(*arg);
	}
	hiarray_deinit(&rct_ctx->args);
	
	
	sdsfree(rct_ctx->cmd);
	dictRelease(rct_ctx->commands);
	rct_free(rct_ctx);
}

int main(int argc,char *argv[])
{
    r_status status;
    struct instance nci;
	rctContext *rct_ctx;

	rct_set_default_options(&nci);
	
    status = rct_get_options(argc, argv, &nci);
    if (status != RCT_OK) {
        rct_show_usage();
        exit(1);
    }
    
    if (nci.show_version) {
        log_stderr("This is redis_cluster_tool-%s" CRLF, RCT_VERSION_STRING);
        if (nci.show_help) {
            rct_show_usage();
        }

        exit(0);
    }

	rct_ctx = init_context(&nci);
	if(rct_ctx == NULL)
	{
		return RCT_ERROR;
	}
	
    core_core(rct_ctx);

	destroy_context(rct_ctx);

	return RCT_OK;
}

