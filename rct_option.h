#ifndef _RCT_OPTION_H_
#define _RCT_OPTION_H_

#define RCT_VERSION_STRING "0.1.0"

struct instance;

void rct_show_usage(void);
void rct_set_default_options(struct instance *nci);
r_status rct_get_options(int argc, char **argv, struct instance *nci);

#endif
