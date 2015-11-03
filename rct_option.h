#ifndef _RCT_OPTION_H_
#define _RCT_OPTION_H_

#define RCT_VERSION_STRING "0.1.0"

static struct option long_options[] = {
    { "help",           no_argument,        NULL,   'h' },
    { "version",        no_argument,        NULL,   'V' },
    { "daemonize",      no_argument,        NULL,   'd' },
    { "output",         required_argument,  NULL,   'o' },
    { "verbose",        required_argument,  NULL,   'v' },
    { "conf-file",      required_argument,  NULL,   'c' },
	{ "addr",           required_argument,  NULL,   'a' },
    { "interval",       required_argument,  NULL,   'i' },
    { "pid-file",       required_argument,  NULL,   'p' },
    { "command",        required_argument,  NULL,   'C' },
    { NULL,             0,                  NULL,    0  }
};

static char short_options[] = "hVdo:v:c:a:i:p:C:";

struct instance;

void rct_show_usage(void);
void rct_set_default_options(struct instance *nci);
r_status rct_get_options(int argc, char **argv, struct instance *nci);

#endif