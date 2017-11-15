# Fallback to gcc when $CC is not in $PATH.
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
OPTIMIZATION?=-O3
WARNINGS=-Wall -W -Wstrict-prototypes -Wwrite-strings
DEBUG?= -g -ggdb
HIREDIS_VIP_INSTALL_DIR?=/usr/local
CFLAGS+=-I$(HIREDIS_VIP_INSTALL_DIR)/include/hiredis-vip -I./ae
LDFLAGS+=-L$(HIREDIS_VIP_INSTALL_DIR)/lib
REAL_CFLAGS=$(OPTIMIZATION) -lhiredis_vip -lpthread -lm -fPIC $(CFLAGS) $(WARNINGS) $(DEBUG) $(ARCH)
REAL_LDFLAGS=$(LDFLAGS) $(ARCH)

# Installation related variables and target
PREFIX?=/usr/local
INCLUDE_PATH?=include
LIBRARY_PATH?=lib
BINARY_PATH?=bin
INSTALL_INCLUDE_PATH= $(DESTDIR)$(PREFIX)/$(INCLUDE_PATH)
INSTALL_PATH= $(DESTDIR)$(PREFIX)/$(BINARY_PATH)

INSTALL?= cp -a

redis-cluster-tool : rct.o rct_command.o rct_core.o rct_log.o rct_option.o rct_conf.o rct_util.o rct_mttlist.o rct_locklist.o ae/ae.o
	$(CC) -o redis-cluster-tool rct.o rct_command.o rct_core.o rct_log.o rct_option.o rct_conf.o rct_util.o rct_mttlist.o rct_locklist.o ae/ae.o $(REAL_CFLAGS) $(REAL_LDFLAGS)

# Deps (use make dep to generate this)
rct.o: rct.c rct_core.h config.h rct_util.h rct_option.h rct_log.h \
 rct_command.h rct_mttlist.h rct_locklist.h rct_conf.h
rct_command.o: rct_command.c rct_core.h config.h rct_util.h rct_option.h \
 rct_log.h rct_command.h rct_mttlist.h rct_locklist.h rct_conf.h
rct_conf.o: rct_conf.c rct_core.h config.h rct_util.h rct_option.h \
 rct_log.h rct_command.h rct_mttlist.h rct_locklist.h rct_conf.h
rct_core.o: rct_core.c rct_core.h config.h rct_util.h rct_option.h \
 rct_log.h rct_command.h rct_mttlist.h rct_locklist.h rct_conf.h
rct_locklist.o: rct_locklist.c rct_core.h config.h rct_util.h \
 rct_option.h rct_log.h rct_command.h rct_mttlist.h rct_locklist.h \
 rct_conf.h
rct_log.o: rct_log.c rct_core.h config.h rct_util.h rct_option.h \
 rct_log.h rct_command.h rct_mttlist.h rct_locklist.h rct_conf.h
rct_mttlist.o: rct_mttlist.c rct_core.h config.h rct_util.h rct_option.h \
 rct_log.h rct_command.h rct_mttlist.h rct_locklist.h rct_conf.h
rct_option.o: rct_option.c rct_core.h config.h rct_util.h rct_option.h \
 rct_log.h rct_command.h rct_mttlist.h rct_locklist.h rct_conf.h
rct_util.o: rct_util.c rct_core.h config.h rct_util.h rct_option.h \
 rct_log.h rct_command.h rct_mttlist.h rct_locklist.h rct_conf.h
ae.o: ae/ae.c ae/ae.h ae/../rct_util.h ae/../config.h ae/ae_epoll.c
ae_epoll.o: ae/ae_epoll.c
ae_evport.o: ae/ae_evport.c
ae_kqueue.o: ae/ae_kqueue.c
ae_select.o: ae/ae_select.c 

install:
	mkdir -p $(INSTALL_PATH)
	$(INSTALL) redis-cluster-tool $(INSTALL_PATH)
 
clean :
	rm -rf redis-cluster-tool *.o ae/*o

dep:
	$(CC) -MM *.c ae/*.c