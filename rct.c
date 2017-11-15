#include "rct_core.h"

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
        log_stderr("This is redis-cluster-tool-%s" CRLF, RCT_VERSION_STRING);
        if (nci.show_help) {
            rct_show_usage();
        }

        exit(0);
    }

    status = log_init(nci.log_level, nci.log_filename);
    if (status != RCT_OK) {
        return status;
    }

    log_debug(LOG_DEBUG, "log enabled");

    rct_ctx = create_context(&nci);
    if(rct_ctx == NULL)
    {
        return RCT_ERROR;
    }
    
    core_core(rct_ctx);

    destroy_context(rct_ctx);

    return RCT_OK;
}

