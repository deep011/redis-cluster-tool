#ifndef _RCT_UTIL_H_
#define _RCT_UTIL_H_

#include <stdarg.h>
#include <sys/un.h>
#include <netinet/in.h>

#define LF                  (uint8_t) 10
#define CR                  (uint8_t) 13
#define CRLF                "\x0d\x0a"
#define CRLF_LEN            (sizeof("\x0d\x0a") - 1)

#define NELEMS(a)           ((sizeof(a)) / sizeof((a)[0]))

#define MIN(a, b)           ((a) < (b) ? (a) : (b))
#define MAX(a, b)           ((a) > (b) ? (a) : (b))

#define SQUARE(d)           ((d) * (d))
#define VAR(s, s2, n)       (((n) < 2) ? 0.0 : ((s2) - SQUARE(s)/(n)) / ((n) - 1))
#define STDDEV(s, s2, n)    (((n) < 2) ? 0.0 : sqrt(VAR((s), (s2), (n))))

#define RCT_INET4_ADDRSTRLEN (sizeof("255.255.255.255") - 1)
#define RCT_INET6_ADDRSTRLEN \
    (sizeof("ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255") - 1)
#define RCT_INET_ADDRSTRLEN  MAX(RCT_INET4_ADDRSTRLEN, RCT_INET6_ADDRSTRLEN)
#define RCT_UNIX_ADDRSTRLEN  \
    (sizeof(struct sockaddr_un) - offsetof(struct sockaddr_un, sun_path))

#define RCT_MAXHOSTNAMELEN   256

/*
 * Length of 1 byte, 2 bytes, 4 bytes, 8 bytes and largest integral
 * type (uintmax_t) in ascii, including the null terminator '\0'
 *
 * From stdint.h, we have:
 * # define UINT8_MAX	(255)
 * # define UINT16_MAX	(65535)
 * # define UINT32_MAX	(4294967295U)
 * # define UINT64_MAX	(__UINT64_C(18446744073709551615))
 */
#define RCT_UINT8_MAXLEN     (3 + 1)
#define RCT_UINT16_MAXLEN    (5 + 1)
#define RCT_UINT32_MAXLEN    (10 + 1)
#define RCT_UINT64_MAXLEN    (20 + 1)
#define RCT_UINTMAX_MAXLEN   RCT_UINT64_MAXLEN

/*
 * Make data 'd' or pointer 'p', n-byte aligned, where n is a power of 2
 * of 2.
 */
#define RCT_ALIGNMENT        sizeof(unsigned long) /* platform word */
#define RCT_ALIGN(d, n)      (((d) + (n - 1)) & ~(n - 1))
#define RCT_ALIGN_PTR(p, n)  \
    (void *) (((uintptr_t) (p) + ((uintptr_t) n - 1)) & ~((uintptr_t) n - 1))

/*
 * Wrapper to workaround well known, safe, implicit type conversion when
 * invoking system calls.
 */
#define rct_gethostname(_name, _len) \
    gethostname((char *)_name, (size_t)_len)

#define rct_atoi(_line)          \
    _rct_atoi((char *)_line)

#define rct_atoll(_line, len)          \
    _rct_atoll((char *)_line, (int)len)

#define rct_itoa(n, _line)          \
    _rct_itoa((int)n, (char *)_line)


int rct_set_blocking(int sd);
int rct_set_nonblocking(int sd);
int rct_set_reuseaddr(int sd);
int rct_set_tcpnodelay(int sd);
int rct_set_linger(int sd, int timeout);
int rct_set_sndbuf(int sd, int size);
int rct_set_rcvbuf(int sd, int size);
int rct_get_soerror(int sd);
int rct_get_sndbuf(int sd);
int rct_get_rcvbuf(int sd);

int rct_set_tcpkeepalive(int sd, int keepidle, int keepinterval, int keepcount);

int _rct_atoi(const char* str);
long long _rct_atoll(const char* str, int len);
void _rct_itoa(int n, char s[]);
char _rct_tohex(int n);
void _rct_dec2hex(int n,char s[]);

int rct_valid_port(int n);

/*
 * Memory allocation and free wrappers.
 *
 * These wrappers enables us to loosely detect double free, dangling
 * pointer access and zero-byte alloc.
 */
#define rct_alloc(_s)                    \
    _rct_alloc((size_t)(_s), __FILE__, __LINE__)

#define rct_zalloc(_s)                   \
    _rct_zalloc((size_t)(_s), __FILE__, __LINE__)

#define rct_calloc(_n, _s)               \
    _rct_calloc((size_t)(_n), (size_t)(_s), __FILE__, __LINE__)

#define rct_realloc(_p, _s)              \
    _rct_realloc(_p, (size_t)(_s), __FILE__, __LINE__)

#define rct_free(_p) do {                \
    _rct_free(_p, __FILE__, __LINE__);   \
    (_p) = NULL;                        \
} while (0)

void *_rct_alloc(size_t size, const char *name, int line);
void *_rct_zalloc(size_t size, const char *name, int line);
void *_rct_calloc(size_t nmemb, size_t size, const char *name, int line);
void *_rct_realloc(void *ptr, size_t size, const char *name, int line);
void _rct_free(void *ptr, const char *name, int line);

/*
 * Wrappers to send or receive n byte message on a blocking
 * socket descriptor.
 */
#define rct_sendn(_s, _b, _n)    \
    _rct_sendn(_s, _b, (size_t)(_n))

#define rct_recvn(_s, _b, _n)    \
    _rct_recvn(_s, _b, (size_t)(_n))

/*
 * Wrappers to read or write data to/from (multiple) buffers
 * to a file or socket descriptor.
 */
#define rct_read(_d, _b, _n)     \
    read(_d, _b, (size_t)(_n))

#define rct_readv(_d, _b, _n)    \
    readv(_d, _b, (int)(_n))

#define rct_write(_d, _b, _n)    \
    write(_d, _b, (size_t)(_n))

#define rct_writev(_d, _b, _n)   \
    writev(_d, _b, (int)(_n))

ssize_t _rct_sendn(int sd, const void *vptr, size_t n);
ssize_t _rct_recvn(int sd, void *vptr, size_t n);

/*
 * Wrappers for defining custom assert based on whether macro
 * RCT_ASSERT_PANIC or RCT_ASSERT_LOG was defined at the moment
 * ASSERT was called.
 */
#ifdef RCT_ASSERT_PANIC

#define RCT_ASSERT(_x) do {                         \
    if (!(_x)) {                                \
        rct_assert(#_x, __FILE__, __LINE__, 1);  \
    }                                           \
} while (0)

#define RCT_NOT_REACHED() RCT_ASSERT(0)

#elif RCT_ASSERT_LOG

#define RCT_ASSERT(_x) do {                         \
    if (!(_x)) {                                \
        rct_assert(#_x, __FILE__, __LINE__, 0);  \
    }                                           \
} while (0)

#define RCT_NOT_REACHED() RCT_ASSERT(0)

#else

#define RCT_ASSERT(_x)

#define RCT_NOT_REACHED()

#endif

void rct_assert(const char *cond, const char *file, int line, int panic);
void rct_stacktrace(int skip_count);
void rct_stacktrace_fd(int fd);

int _scnprintf(char *buf, size_t size, const char *fmt, ...);
int _vscnprintf(char *buf, size_t size, const char *fmt, va_list args);
int64_t rct_usec_now(void);
int64_t rct_msec_now(void);

/*
 * Address resolution for internet (ipv4 and ipv6) and unix domain
 * socket address.
 */

struct sockinfo {
    int       family;              /* socket address family */
    socklen_t addrlen;             /* socket address length */
    union {
        struct sockaddr_in  in;    /* ipv4 socket address */
        struct sockaddr_in6 in6;   /* ipv6 socket address */
        struct sockaddr_un  un;    /* unix domain address */
    } addr;
};

int rct_resolve(char *name, int port, struct sockinfo *si);
char *rct_unresolve_addr(struct sockaddr *addr, socklen_t addrlen);
char *rct_unresolve_peer_desc(int sd);
char *rct_unresolve_desc(int sd);

/*
 * Wrapper around common routines for manipulating C character
 * strings
 */
#define rct_memcpy(_d, _c, _n)           \
    memcpy(_d, _c, (size_t)(_n))

#define rct_memmove(_d, _c, _n)          \
    memmove(_d, _c, (size_t)(_n))

#define rct_memchr(_d, _c, _n)           \
    memchr(_d, _c, (size_t)(_n))

#define rct_strlen(_s)                   \
    strlen((char *)(_s))

#define rct_strncmp(_s1, _s2, _n)        \
    strncmp((char *)(_s1), (char *)(_s2), (size_t)(_n))

#define rct_strchr(_p, _l, _c)           \
    _rct_strchr((uint8_t *)(_p), (uint8_t *)(_l), (uint8_t)(_c))

#define rct_strrchr(_p, _s, _c)          \
    _rct_strrchr((uint8_t *)(_p),(uint8_t *)(_s), (uint8_t)(_c))

#define rct_strndup(_s, _n)              \
    (uint8_t *)strndup((char *)(_s), (size_t)(_n));

#define rct_snprintf(_s, _n, ...)        \
    snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define rct_scnprintf(_s, _n, ...)       \
    _scnprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define rct_vscnprintf(_s, _n, _f, _a)   \
    _vscnprintf((char *)(_s), (size_t)(_n), _f, _a)

#define rct_strftime(_s, _n, fmt, tm)        \
    (int)strftime((char *)(_s), (size_t)(_n), fmt, tm)


/*
 * A (very) limited version of snprintf
 * @param   to   Destination buffer
 * @param   n    Size of destination buffer
 * @param   fmt  printf() style format string
 * @returns Number of bytes written, including terminating '\0'
 * Supports 'd' 'i' 'u' 'x' 'p' 's' conversion
 * Supports 'l' and 'll' modifiers for integral types
 * Does not support any width/precision
 * Implemented with simplicity, and async-signal-safety in mind
 */
int _safe_vsnprintf(char *to, size_t size, const char *format, va_list ap);
int _safe_snprintf(char *to, size_t n, const char *fmt, ...);

#define rct_safe_snprintf(_s, _n, ...)       \
    _safe_snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define rct_safe_vsnprintf(_s, _n, _f, _a)   \
    _safe_vsnprintf((char *)(_s), (size_t)(_n), _f, _a)

static inline uint8_t *
_rct_strchr(uint8_t *p, uint8_t *last, uint8_t c)
{
    while (p < last) {
        if (*p == c) {
            return p;
        }
        p++;
    }

    return NULL;
}

static inline uint8_t *
_rct_strrchr(uint8_t *p, uint8_t *start, uint8_t c)
{
    while (p >= start) {
        if (*p == c) {
            return p;
        }
        p--;
    }

    return NULL;
}

uint64_t size_string_to_integer_byte(char *size, int size_len);

#endif

