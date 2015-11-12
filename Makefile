# Fallback to gcc when $CC is not in $PATH.
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
OPTIMIZATION?=-O3
WARNINGS=-Wall -W -Wstrict-prototypes -Wwrite-strings
DEBUG?= -g -ggdb
HIREDIS_VIP_INSTALL_DIR?=/usr/local
CFLAGS+=-I$(HIREDIS_VIP_INSTALL_DIR)/include/hiredis-vip
LDFLAGS+=-L$(HIREDIS_VIP_INSTALL_DIR)/lib
REAL_CFLAGS=$(OPTIMIZATION) -lhiredis_vip -fPIC $(CFLAGS) $(WARNINGS) $(DEBUG) $(ARCH)
REAL_LDFLAGS=$(LDFLAGS) $(ARCH)

# Installation related variables and target
PREFIX?=/usr/local
INCLUDE_PATH?=include
LIBRARY_PATH?=lib
BINARY_PATH?=bin
INSTALL_INCLUDE_PATH= $(DESTDIR)$(PREFIX)/$(INCLUDE_PATH)
INSTALL_PATH= $(DESTDIR)$(PREFIX)/$(BINARY_PATH)

INSTALL?= cp -a

redis-cluster-tool : rct.o rct_command.o rct_core.o rct_log.o rct_option.o rct_util.o
	$(CC) -o redis-cluster-tool rct.o rct_command.o rct_core.o rct_log.o rct_option.o rct_util.o $(REAL_CFLAGS) $(REAL_LDFLAGS)

# Deps (use make dep to generate this)
rct.o: rct.c rct_core.h rct_util.h rct_option.h rct_log.h rct_command.h
rct_command.o: rct_command.c rct_core.h rct_util.h rct_option.h rct_log.h \
 rct_command.h
rct_core.o: rct_core.c rct_core.h rct_util.h rct_option.h rct_log.h \
 rct_command.h
rct_log.o: rct_log.c rct_core.h rct_util.h rct_option.h rct_log.h \
 rct_command.h
rct_option.o: rct_option.c rct_core.h rct_util.h rct_option.h rct_log.h \
 rct_command.h
rct_util.o: rct_util.c rct_core.h rct_util.h rct_option.h rct_log.h \
 rct_command.h

install:
	mkdir -p $(INSTALL_PATH)
	$(INSTALL) redis-cluster-tool $(INSTALL_PATH)
 
clean :
	rm -rf redis-cluster-tool *.o

dep:
	$(CC) -MM *.c