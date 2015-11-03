#include "rct_core.h"

#define RCT_CONF_PATH        "conf/rct.yml"

#define RCT_LOG_DEFAULT      LOG_NOTICE
#define RCT_LOG_MIN          LOG_EMERG
#define RCT_LOG_MAX          LOG_PVERB
#define RCT_LOG_PATH         NULL

#define RCT_ADDR             "127.0.0.1:6379"
#define RCT_INTERVAL         1000

#define RCT_PID_FILE         NULL

#define RCT_COMMAND_DEFAULT	 RCT_CMD_CLUSTER_STATE

void
rct_show_usage(void)
{
    log_stderr(
        "Usage: redis_cluster_tool [-?hVd] [-v verbosity level] [-o output file]" CRLF
        "                  [-c conf file] [-a addr] [-i interval]" CRLF
        "                  [-p pid file]" CRLF
        "");
    log_stderr(
        "Options:" CRLF
        "  -h, --help             : this help" CRLF
        "  -V, --version          : show version and exit" CRLF
        "  -d, --daemonize        : run as a daemon");
    log_stderr(
        "  -v, --verbosity=N      : set logging level (default: %d, min: %d, max: %d)" CRLF
        "  -o, --output=S         : set logging file (default: %s)" CRLF
        "  -c, --conf-file=S      : set configuration file (default: %s)" CRLF
        "  -a, --addr=S           : set redis cluster address (default: %s)" CRLF
        "  -i, --interval=N       : set interval in msec (default: %d msec)" CRLF
        "  -p, --pid-file=S       : set pid file (default: %s)" CRLF
        "  -C, --command=S        : set command to execute (default: %s)" CRLF
        "",
        RCT_LOG_DEFAULT, RCT_LOG_MIN, RCT_LOG_MAX,
        RCT_LOG_PATH != NULL ? RCT_LOG_PATH : "stderr",
        RCT_CONF_PATH,
        RCT_ADDR, 
        RCT_INTERVAL,
        RCT_PID_FILE != NULL ? RCT_PID_FILE : "off",
        RCT_COMMAND_DEFAULT);

	rct_show_command_usage();
}

void
rct_set_default_options(struct instance *nci)
{
    int status;

	nci->show_version = 0;
	nci->show_help = 0;
	nci->daemonize = 0;
	
    nci->log_level = RCT_LOG_DEFAULT;
    nci->log_filename = RCT_LOG_PATH;

    nci->conf_filename = RCT_CONF_PATH;

    nci->addr = RCT_ADDR;
    nci->interval = RCT_INTERVAL;

    status = rct_gethostname(nci->hostname, RCT_MAXHOSTNAMELEN);
    if (status < 0) {
        log_warn("gethostname failed, ignored: %s", strerror(errno));
        rct_snprintf(nci->hostname, RCT_MAXHOSTNAMELEN, "unknown");
    }
    nci->hostname[RCT_MAXHOSTNAMELEN - 1] = '\0';

    nci->pid = (pid_t)-1;
    nci->pid_filename = NULL;
    nci->pidfile = 0;
    
    nci->command = RCT_COMMAND_DEFAULT;
    nci->start = 0;
    nci->end = 0;
}

r_status
rct_get_options(int argc, char **argv, struct instance *nci)
{
    int c, value;

	if(argc <= 1)
	{
		log_stderr("redis_cluster_tool needs some options.\n");
		return RCT_ERROR;
	}

    opterr = 0;

    for (;;) {
        c = getopt_long(argc, argv, short_options, long_options, NULL);
        if (c == -1) {
            /* no more options */
            break;
        }

        switch (c) {
        case 'h':
            nci->show_version = 1;
            nci->show_help = 1;
            break;

        case 'V':
            nci->show_version = 1;
            break;

        case 'd':
            nci->daemonize = 1;
            break;

        case 'v':
            value = rct_atoi(optarg);
            if (value < 0) {
                log_stderr("redis_cluster_tool: option -v requires a number");
                return RCT_ERROR;
            }
            nci->log_level = value;
            break;

        case 'o':
            nci->log_filename = optarg;
            break;

        case 'c':
            nci->conf_filename = optarg;
            break;

        case 'i':
            value = rct_atoi(optarg);
            if (value < 0) {
                log_stderr("redis_cluster_tool: option -i requires a number");
                return RCT_ERROR;
            }

            nci->interval = value;
            break;

        case 'a':
            nci->addr = optarg;
            break;

        case 'p':
            nci->pid_filename = optarg;
            break;

		case 'C':
            nci->command = optarg;
            break;

        case '?':
            switch (optopt) {
            case 'o':
            case 'c':
            case 'p':
                log_stderr("redis_cluster_tool: option -%c requires a file name",
                           optopt);
                break;

            case 'v':
            case 'i':
                log_stderr("redis_cluster_tool: option -%c requires a number", optopt);
                break;

            case 'a':
                log_stderr("redis_cluster_tool: option -%c requires a string", optopt);
                break;

            default:
                log_stderr("redis_cluster_tool: invalid option -- '%c'", optopt);
                break;
            }
            return RCT_ERROR;

        default:
            log_stderr("redis_cluster_tool: invalid option -- '%c'", optopt);
            return RCT_ERROR;

        }
    }

    return RCT_OK;
}