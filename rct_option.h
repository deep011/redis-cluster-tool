#ifndef _RCT_OPTION_H_
#define _RCT_OPTION_H_

#define RCT_VERSION_STRING "0.2.0"

#define RCT_OPTION_REDIS_ROLE_ALL		"all"
#define RCT_OPTION_REDIS_ROLE_MASTER	RCT_REDIS_ROLE_NAME_MASTER
#define RCT_OPTION_REDIS_ROLE_SLAVE		RCT_REDIS_ROLE_NAME_SLAVE

struct instance;

void rct_show_usage(void);
void rct_set_default_options(struct instance *nci);
r_status rct_get_options(int argc, char **argv, struct instance *nci);

#endif
