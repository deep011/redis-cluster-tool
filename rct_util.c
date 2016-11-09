
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>

#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>

#include <netinet/in.h>
#include <netinet/tcp.h>

#include "rct_core.h"

#ifdef RCT_HAVE_BACKTRACE
# include <execinfo.h>
#endif

int
rct_set_blocking(int sd)
{
    int flags;

    flags = fcntl(sd, F_GETFL, 0);
    if (flags < 0) {
        return flags;
    }

    return fcntl(sd, F_SETFL, flags & ~O_NONBLOCK);
}

int
rct_set_nonblocking(int sd)
{
    int flags;

    flags = fcntl(sd, F_GETFL, 0);
    if (flags < 0) {
        return flags;
    }

    return fcntl(sd, F_SETFL, flags | O_NONBLOCK);
}

int
rct_set_reuseaddr(int sd)
{
    int reuse;
    socklen_t len;

    reuse = 1;
    len = sizeof(reuse);

    return setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &reuse, len);
}

/*
 * Disable Nagle algorithm on TCP socket.
 *
 * This option helps to minimize transmit latency by disabling coalescing
 * of data to fill up a TCP segment inside the kernel. Sockets with this
 * option must use readv() or writev() to do data transfer in bulk and
 * hence avoid the overhead of small packets.
 */
int
rct_set_tcpnodelay(int sd)
{
    int nodelay;
    socklen_t len;

    nodelay = 1;
    len = sizeof(nodelay);

    return setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, &nodelay, len);
}

int
rct_set_linger(int sd, int timeout)
{
    struct linger linger;
    socklen_t len;

    linger.l_onoff = 1;
    linger.l_linger = timeout;

    len = sizeof(linger);

    return setsockopt(sd, SOL_SOCKET, SO_LINGER, &linger, len);
}

int
rct_set_sndbuf(int sd, int size)
{
    socklen_t len;

    len = sizeof(size);

    return setsockopt(sd, SOL_SOCKET, SO_SNDBUF, &size, len);
}

int
rct_set_rcvbuf(int sd, int size)
{
    socklen_t len;

    len = sizeof(size);

    return setsockopt(sd, SOL_SOCKET, SO_RCVBUF, &size, len);
}

int
rct_get_soerror(int sd)
{
    int status, err;
    socklen_t len;

    err = 0;
    len = sizeof(err);

    status = getsockopt(sd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (status == 0) {
        errno = err;
    }

    return status;
}

int
rct_get_sndbuf(int sd)
{
    int status, size;
    socklen_t len;

    size = 0;
    len = sizeof(size);

    status = getsockopt(sd, SOL_SOCKET, SO_SNDBUF, &size, &len);
    if (status < 0) {
        return status;
    }

    return size;
}

int
rct_get_rcvbuf(int sd)
{
    int status, size;
    socklen_t len;

    size = 0;
    len = sizeof(size);

    status = getsockopt(sd, SOL_SOCKET, SO_RCVBUF, &size, &len);
    if (status < 0) {
        return status;
    }

    return size;
}

int
rct_set_tcpkeepalive(int sd, int keepidle, int keepinterval, int keepcount)
{
	r_status status;
    int tcpkeepalive;
    socklen_t len;

    tcpkeepalive = 1;
    len = sizeof(tcpkeepalive);

    status = setsockopt(sd, SOL_SOCKET, SO_KEEPALIVE, &tcpkeepalive, len);
	if(status < 0)
	{
		log_error("error: setsockopt SO_KEEPALIVE call error(%s)", strerror(errno));
		return RCT_ERROR;
	}
	
	if(keepidle > 0)
	{
		len = sizeof(keepidle);
		status = setsockopt(sd, SOL_TCP, TCP_KEEPIDLE, &keepidle, len);
		if(status < 0)
		{
			log_error("error: setsockopt TCP_KEEPIDLE call error(%s)", strerror(errno));
			return RCT_ERROR;
		}
	}

	if(keepinterval > 0)
	{
		len = sizeof(keepinterval);
		status = setsockopt(sd, SOL_TCP, TCP_KEEPINTVL, &keepinterval, len);
		if(status < 0)
		{
			log_error("error: setsockopt TCP_KEEPINTVL call error(%s)", strerror(errno));
			return RCT_ERROR;
		}
	}

	if(keepcount > 0)
	{
		len = sizeof(keepcount);
		status = setsockopt(sd, SOL_TCP, TCP_KEEPCNT, &keepcount, len);
		if(status < 0)
		{
			log_error("error: setsockopt TCP_KEEPCNT call error(%s)", strerror(errno));
			return RCT_ERROR;
		}
	}

	return RCT_OK;
}

int
_rct_atoi(const char* str){
	if(str == NULL)
	{
		return 0;
	}

	int res=0;
	char sign='+';
	const char *pStr=str;

	while (*pStr==' ')
	  pStr++;

	if(*pStr=='+' || *pStr=='-')
	  sign=*pStr++;    

	while (*pStr>='0' && *pStr<='9')
	{
	  res=res*10+*pStr-'0';
	  pStr++;
	}

	return sign=='-'?-res:res;
}

long long
_rct_atoll(const char* str, int len){
	if(str == NULL)
	{
	  return 0;
	}

	long long res=0;
	char sign='+';
	const char *pStr=str;

	while (*pStr==' ')
	{
	  pStr++;
	  len --;
	}

	if(*pStr=='+' || *pStr=='-')
	{
	  sign=*pStr++;    
		len --;
	}

	while (*pStr>='0' && *pStr<='9' && len > 0)
	{
	  res=res*10+*pStr-'0';
	  pStr++;
	  len --;
	}

	return sign=='-'?-res:res;
}

static void
_rever(char s[]){
	if(s == NULL)
	{
	  return;
	}

	int len=strlen(s);
	int i=0;
	int j=len-1;
	char c;
	while (i<j)
	{
	  c=s[i];
	  s[i]=s[j];
	  s[j]=c;
	  i++;
	  j--;
	}
}

void
_rct_itoa(int n, char s[]){
	if(s == NULL)
	{
	  return;
	}

	int i=0;
	int sign=0;

	if((sign=n)<0)
	  n=-n;

	do {
	  s[i++]=n%10+'0';
	} while ((n/=10)>0);
	if(sign<0)
	  s[i++]='-';

	s[i]='\0';
	_rever(s);
}

char
_rct_tohex(int n)
{
	if(n>=10 && n<=15)
	{
		return 'A'+n-10;
	}

	return '0'+n;
}

void
_rct_dec2hex(int n,char s[])
{
	int i=0;
	int mod;

	if(n <= 0)
	{
		s[0] = '\0';
		return;
	}

	while(n)
	{
		mod = n%16;
		s[i++]=_rct_tohex(mod);
		n=n/16;
	}

	s[i]='\0';

	_rever(s);
}


int
rct_valid_port(int n)
{
    if (n < 1 || n > UINT16_MAX) {
        return 0;
    }

    return 1;
}

void *
_rct_alloc(size_t size, const char *name, int line)
{
    void *p;

    RCT_ASSERT(size != 0);

    p = malloc(size);
    if (p == NULL) {
        log_error("malloc(%zu) failed @ %s:%d", size, name, line);
    } else {
        log_debug(LOG_VVERB, "malloc(%zu) at %p @ %s:%d", size, p, name, line);
    }

    return p;
}

void *
_rct_zalloc(size_t size, const char *name, int line)
{
    void *p;

    p = _rct_alloc(size, name, line);
    if (p != NULL) {
        memset(p, 0, size);
    }

    return p;
}

void *
_rct_calloc(size_t nmemb, size_t size, const char *name, int line)
{
    return _rct_zalloc(nmemb * size, name, line);
}

void *
_rct_realloc(void *ptr, size_t size, const char *name, int line)
{
    void *p;

    RCT_ASSERT(size != 0);

    p = realloc(ptr, size);
    if (p == NULL) {
        log_error("realloc(%zu) failed @ %s:%d", size, name, line);
    } else {
        log_debug(LOG_VVERB, "realloc(%zu) at %p @ %s:%d", size, p, name, line);
    }

    return p;
}

void
_rct_free(void *ptr, const char *name, int line)
{
    RCT_ASSERT(ptr != NULL);
    log_debug(LOG_VVERB, "free(%p) @ %s:%d", ptr, name, line);
    free(ptr);
}

void
rct_stacktrace(int skip_count)
{
#ifdef RCT_HAVE_BACKTRACE
    void *stack[64];
    char **symbols;
    int size, i, j;

    size = backtrace(stack, 64);
    symbols = backtrace_symbols(stack, size);
    if (symbols == NULL) {
        return;
    }

    skip_count++; /* skip the current frame also */

    for (i = skip_count, j = 0; i < size; i++, j++) {
        loga("[%d] %s", j, symbols[i]);
    }

    free(symbols);
#endif
}

void
rct_stacktrace_fd(int fd)
{
#ifdef RCT_HAVE_BACKTRACE
    void *stack[64];
    int size;

    size = backtrace(stack, 64);
    backtrace_symbols_fd(stack, size, fd);
#endif
}

void
rct_assert(const char *cond, const char *file, int line, int panic)
{
    log_error("assert '%s' failed @ (%s, %d)", cond, file, line);
    if (panic) {
        rct_stacktrace(1);
        abort();
    }
}

static int
_vscnprintf(char *buf, size_t size, const char *fmt, va_list args)
{
    int n;

    n = vsnprintf(buf, size, fmt, args);

    /*
     * The return value is the number of characters which would be written
     * into buf not including the trailing '\0'. If size is == 0 the
     * function returns 0.
     *
     * On error, the function also returns 0. This is to allow idiom such
     * as len += _vscnprintf(...)
     *
     * See: http://lwn.net/Articles/69419/
     */
    if (n <= 0) {
        return 0;
    }

    if (n < (int) size) {
        return n;
    }

    return (int)(size - 1);
}

static int
_scnprintf(char *buf, size_t size, const char *fmt, ...)
{
    va_list args;
    int n;

    va_start(args, fmt);
    n = _vscnprintf(buf, size, fmt, args);
    va_end(args);

    return n;
}

/*
 * Send n bytes on a blocking descriptor
 */
ssize_t
_rct_sendn(int sd, const void *vptr, size_t n)
{
    size_t nleft;
    ssize_t	nsend;
    const char *ptr;

    ptr = vptr;
    nleft = n;
    while (nleft > 0) {
        nsend = send(sd, ptr, nleft, 0);
        if (nsend < 0) {
            if (errno == EINTR) {
                continue;
            }
            return nsend;
        }
        if (nsend == 0) {
            return -1;
        }

        nleft -= (size_t)nsend;
        ptr += nsend;
    }

    return (ssize_t)n;
}

/*
 * Recv n bytes from a blocking descriptor
 */
ssize_t
_rct_recvn(int sd, void *vptr, size_t n)
{
	size_t nleft;
	ssize_t	nrecv;
	char *ptr;

	ptr = vptr;
	nleft = n;
	while (nleft > 0) {
        nrecv = recv(sd, ptr, nleft, 0);
        if (nrecv < 0) {
            if (errno == EINTR) {
                continue;
            }
            return nrecv;
        }
        if (nrecv == 0) {
            break;
        }

        nleft -= (size_t)nrecv;
        ptr += nrecv;
    }

    return (ssize_t)(n - nleft);
}

/*
 * Return the current time in microseconds since Epoch
 */
int64_t
rct_usec_now(void)
{
    struct timeval now;
    int64_t usec;
    int status;

    status = gettimeofday(&now, NULL);
    if (status < 0) {
        log_error("gettimeofday failed: %s", strerror(errno));
        return -1;
    }

    usec = (int64_t)now.tv_sec * 1000000LL + (int64_t)now.tv_usec;

    return usec;
}

/*
 * Return the current time in milliseconds since Epoch
 */
int64_t
rct_msec_now(void)
{
    return rct_usec_now() / 1000LL;
}

static int
rct_resolve_inet(char *name, int port, struct sockinfo *si)
{
    int status;
    struct addrinfo *ai, *cai; /* head and current addrinfo */
    struct addrinfo hints;
    char *node, service[RCT_UINTMAX_MAXLEN];
    bool found;

    RCT_ASSERT(rct_valid_port(port));

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_NUMERICSERV;
    hints.ai_family = AF_UNSPEC;     /* AF_INET or AF_INET6 */
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = 0;
    hints.ai_addrlen = 0;
    hints.ai_addr = NULL;
    hints.ai_canonname = NULL;

    if (name != NULL) {
        node = name;
    } else {
        /*
         * If AI_PASSIVE flag is specified in hints.ai_flags, and node is
         * NULL, then the returned socket addresses will be suitable for
         * bind(2)ing a socket that will accept(2) connections. The returned
         * socket address will contain the wildcard IP address.
         */
        node = NULL;
        hints.ai_flags |= AI_PASSIVE;
    }

    rct_snprintf(service, RCT_UINTMAX_MAXLEN, "%d", port);

    status = getaddrinfo(node, service, &hints, &ai);
    if (status < 0) {
        log_error("address resolution of node '%s' service '%s' failed: %s",
                  node, service, gai_strerror(status));
        return -1;
    }

    /*
     * getaddrinfo() can return a linked list of more than one addrinfo,
     * since we requested for both AF_INET and AF_INET6 addresses and the
     * host itself can be multi-homed. Since we don't care whether we are
     * using ipv4 or ipv6, we just use the first address from this collection
     * in the order in which it was returned.
     *
     * The sorting function used within getaddrinfo() is defined in RFC 3484;
     * the order can be tweaked for a particular system by editing
     * /etc/gai.conf
     */
    for (cai = ai, found = 0; cai != NULL; cai = cai->ai_next) {
        si->family = cai->ai_family;
        si->addrlen = cai->ai_addrlen;
        rct_memcpy(&si->addr, cai->ai_addr, si->addrlen);
        found = 1;
        break;
    }

    freeaddrinfo(ai);

    return !found ? -1 : 0;
}

static int
rct_resolve_unix(char *name, struct sockinfo *si)
{
    struct sockaddr_un *un;

    if (strlen(name) >= RCT_UNIX_ADDRSTRLEN) {
        return -1;
    }

    un = &si->addr.un;

    un->sun_family = AF_UNIX;
    rct_memcpy(un->sun_path, name, strlen(name));
    un->sun_path[strlen(name)] = '\0';

    si->family = AF_UNIX;
    si->addrlen = sizeof(*un);
    /* si->addr is an alias of un */

    return 0;
}

/*
 * Resolve a hostname and service by translating it to socket address and
 * return it in si
 *
 * This routine is reentrant
 */
int
rct_resolve(char *name, int port, struct sockinfo *si)
{
    if (name != NULL && name[0] == '/') {
        return rct_resolve_unix(name, si);
    }

    return rct_resolve_inet(name, port, si);
}

/*
 * Unresolve the socket address by translating it to a character string
 * describing the host and service
 *
 * This routine is not reentrant
 */
char *
rct_unresolve_addr(struct sockaddr *addr, socklen_t addrlen)
{
    static char unresolve[NI_MAXHOST + NI_MAXSERV];
    static char host[NI_MAXHOST], service[NI_MAXSERV];
    int status;

    status = getnameinfo(addr, addrlen, host, sizeof(host),
                         service, sizeof(service),
                         NI_NUMERICHOST | NI_NUMERICSERV);
    if (status < 0) {
        return "unknown";
    }

    rct_snprintf(unresolve, sizeof(unresolve), "%s:%s", host, service);

    return unresolve;
}

/*
 * Unresolve the socket descriptor peer address by translating it to a
 * character string describing the host and service
 *
 * This routine is not reentrant
 */
char *
rct_unresolve_peer_desc(int sd)
{
    static struct sockinfo si;
    struct sockaddr *addr;
    socklen_t addrlen;
    int status;

    memset(&si, 0, sizeof(si));
    addr = (struct sockaddr *)&si.addr;
    addrlen = sizeof(si.addr);

    status = getpeername(sd, addr, &addrlen);
    if (status < 0) {
        return "unknown";
    }

    return rct_unresolve_addr(addr, addrlen);
}

/*
 * Unresolve the socket descriptor address by translating it to a
 * character string describing the host and service
 *
 * This routine is not reentrant
 */
char *
rct_unresolve_desc(int sd)
{
    static struct sockinfo si;
    struct sockaddr *addr;
    socklen_t addrlen;
    int status;

    memset(&si, 0, sizeof(si));
    addr = (struct sockaddr *)&si.addr;
    addrlen = sizeof(si.addr);

    status = getsockname(sd, addr, &addrlen);
    if (status < 0) {
        return "unknown";
    }

    return rct_unresolve_addr(addr, addrlen);
}


static char *
_safe_utoa(int _base, uint64_t val, char *buf)
{
    char hex[] = "0123456789abcdef";
    uint32_t base = (uint32_t) _base;
    *buf-- = 0;
    do {
        *buf-- = hex[val % base];
    } while ((val /= base) != 0);
    return buf + 1;
}

static char *
_safe_itoa(int base, int64_t val, char *buf)
{
    char hex[] = "0123456789abcdef";
    char *orig_buf = buf;
    const int32_t is_neg = (val < 0);
    *buf-- = 0;

    if (is_neg) {
        val = -val;
    }
    if (is_neg && base == 16) {
        int ix;
        val -= 1;
        for (ix = 0; ix < 16; ++ix)
            buf[-ix] = '0';
    }

    do {
        *buf-- = hex[val % base];
    } while ((val /= base) != 0);

    if (is_neg && base == 10) {
        *buf-- = '-';
    }

    if (is_neg && base == 16) {
        int ix;
        buf = orig_buf - 1;
        for (ix = 0; ix < 16; ++ix, --buf) {
            /* *INDENT-OFF* */
            switch (*buf) {
            case '0': *buf = 'f'; break;
            case '1': *buf = 'e'; break;
            case '2': *buf = 'd'; break;
            case '3': *buf = 'c'; break;
            case '4': *buf = 'b'; break;
            case '5': *buf = 'a'; break;
            case '6': *buf = '9'; break;
            case '7': *buf = '8'; break;
            case '8': *buf = '7'; break;
            case '9': *buf = '6'; break;
            case 'a': *buf = '5'; break;
            case 'b': *buf = '4'; break;
            case 'c': *buf = '3'; break;
            case 'd': *buf = '2'; break;
            case 'e': *buf = '1'; break;
            case 'f': *buf = '0'; break;
            }
            /* *INDENT-ON* */
        }
    }
    return buf + 1;
}

static const char *
_safe_check_longlong(const char *fmt, int32_t * have_longlong)
{
    *have_longlong = 0;
    if (*fmt == 'l') {
        fmt++;
        if (*fmt != 'l') {
            *have_longlong = (sizeof(long) == sizeof(int64_t));
        } else {
            fmt++;
            *have_longlong = 1;
        }
    }
    return fmt;
}

int
_safe_vsnprintf(char *to, size_t size, const char *format, va_list ap)
{
    char *start = to;
    char *end = start + size - 1;
    for (; *format; ++format) {
        int32_t have_longlong = false;
        if (*format != '%') {
            if (to == end) {    /* end of buffer */
                break;
            }
            *to++ = *format;    /* copy ordinary char */
            continue;
        }
        ++format;               /* skip '%' */

        format = _safe_check_longlong(format, &have_longlong);

        switch (*format) {
        case 'd':
        case 'i':
        case 'u':
        case 'x':
        case 'p':
            {
                int64_t ival = 0;
                uint64_t uval = 0;
                if (*format == 'p')
                    have_longlong = (sizeof(void *) == sizeof(uint64_t));
                if (have_longlong) {
                    if (*format == 'u') {
                        uval = va_arg(ap, uint64_t);
                    } else {
                        ival = va_arg(ap, int64_t);
                    }
                } else {
                    if (*format == 'u') {
                        uval = va_arg(ap, uint32_t);
                    } else {
                        ival = va_arg(ap, int32_t);
                    }
                }

                {
                    char buff[22];
                    const int base = (*format == 'x' || *format == 'p') ? 16 : 10;

		            /* *INDENT-OFF* */
                    char *val_as_str = (*format == 'u') ?
                        _safe_utoa(base, uval, &buff[sizeof(buff) - 1]) :
                        _safe_itoa(base, ival, &buff[sizeof(buff) - 1]);
		            /* *INDENT-ON* */

                    /* Strip off "ffffffff" if we have 'x' format without 'll' */
                    if (*format == 'x' && !have_longlong && ival < 0) {
                        val_as_str += 8;
                    }

                    while (*val_as_str && to < end) {
                        *to++ = *val_as_str++;
                    }
                    continue;
                }
            }
        case 's':
            {
                const char *val = va_arg(ap, char *);
                if (!val) {
                    val = "(null)";
                }
                while (*val && to < end) {
                    *to++ = *val++;
                }
                continue;
            }
        }
    }
    *to = 0;
    return (int)(to - start);
}

int
_safe_snprintf(char *to, size_t n, const char *fmt, ...)
{
    int result;
    va_list args;
    va_start(args, fmt);
    result = _safe_vsnprintf(to, n, fmt, args);
    va_end(args);
    return result;
}


uint64_t
size_string_to_integer_byte(char *size, int size_len)
{
    int i;
    uint8_t ch;
	uint8_t *pos;
    uint64_t num = 0;
	uint64_t multiple_for_unit = 1;
	int first_nonzero_flag = 0;

    if(size == NULL || size_len <= 0)
    {
        return 0;
    }
    
	if(size_len > 2)
	{
		pos = size + size_len - 2;
		if((*pos == 'G' || *pos == 'g') && (*(pos+1) == 'B' || *(pos+1) == 'b'))
		{
			multiple_for_unit = 1073741824;
			size_len -= 2;
		}
		else if((*pos == 'M' || *pos == 'm') && (*(pos+1) == 'B' || *(pos+1) == 'b'))
		{
			multiple_for_unit = 1048576;
			size_len -= 2;
		}
        else if((*pos == 'K' || *pos == 'k') && (*(pos+1) == 'B' || *(pos+1) == 'b'))
		{
			multiple_for_unit = 1024;
			size_len -= 2;
		}
		else if(*(pos+1) == 'G' || *(pos+1) == 'g')
		{
    		multiple_for_unit = 1000000000;
			size_len -= 1;
		}
		else if(*(pos+1) == 'M' || *(pos+1) == 'm')
		{
			multiple_for_unit = 1000000;
			size_len -= 1;
		}
        else if(*(pos+1) == 'K' || *(pos+1) == 'k')
		{
			multiple_for_unit = 1000;
			size_len -= 1;
		}
		else if(*(pos+1) == 'B' || *(pos+1) == 'b')
		{
			size_len -= 1;
		}
	}
	else if(size_len > 1)
	{
		pos = size + size_len - 1;
		if(*pos == 'G' || *pos == 'g')
		{   
			multiple_for_unit = 1000000000;
			size_len -= 1;
		}
		else if(*pos == 'M' || *pos == 'm')
		{   
			multiple_for_unit = 1000000;
			size_len -= 1;
		}
        else if(*pos == 'K' || *pos == 'k')
		{   
			multiple_for_unit = 1000000;
			size_len -= 1;
		}
		else if(*pos == 'B' || *pos == 'b')
		{
		    size_len -= 1;
		}
	}

	for(i = 0; i < size_len; i ++)
	{
		ch = *(size + i);
		if(ch < '0' || ch > '9')
		{
			return 0;
		}
		else if(!first_nonzero_flag && ch != '0')
		{
			first_nonzero_flag = 1;
		}
		
		if(first_nonzero_flag)
		{
			num = 10*num + (ch - 48);
		}
	}
    
	num *= multiple_for_unit;

	if(first_nonzero_flag == 0)
	{
		return 0;
	}

    return num;
}

int str_is_integer(char *str, int len)
{
    if(str == NULL || len <= 0){
        return RCT_ERROR;
    }

    while(--len >= 0){
        if(*(str+len) < '0' || *(str+len) > '9'){
            return 0;
        }
    }

    return 1;
}

