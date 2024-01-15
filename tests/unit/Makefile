# Makefile to launch unit tests

SHELL=/bin/bash
CPPFLAGS := -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv
LDFLAGS := -Wl,--copy-dt-needed-entries,--build-id
LIBFLAGS := -lm -lrt -ldl -lcheck_pic -pthread -lsubunit
# LIBFLAGS := $(pkg-config --cflags --libs check)
CURRENT_DIR = ${PWD}
SRC_DIR = ${PWD}/../../../../src/

OBJFILES := $(shell find ${SRC_DIR}/backend -name '*.o' | egrep -v '(main/main\.o|snowball|libpqwalreceiver|conversion_procs|pgoutput)' | xargs echo)
OBJFILESTZ := ${SRC_DIR}/timezone/localtime.o ${SRC_DIR}/timezone/strftime.o ${SRC_DIR}/timezone/pgtz.o
OBJFILESSRV := ${SRC_DIR}/common/libpgcommon_srv.a ${SRC_DIR}/port/libpgport_srv.a
OBJFILESPGSS := ../../pg_stat_query_plans_storage.o ../../pg_stat_query_plans.o ../../pg_stat_query_plans_parser.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

packages:
	[ -z `dpkg -s check | grep -q "install ok"` ] && sudo apt-get install check

check_pgss_store.o: check_pgss_store.c

check_pgss_store: $(PROGRAM) $(DATA_built) $(HEADER_allbuilt) check_pgss_store.o
	gcc -g ${CPPFLAGS} -L/usr/local/lib -pthread -Lport -Lcommon -I include ${CURRENT_DIR}/check_pgss_store.c ${OBJFILES} ${OBJFILESTZ} ${OBJFILESSRV} ${OBJFILESPGSS} ${LIBFLAGS} -o ${CURRENT_DIR}/check_pgss_store

test: check_pgss_store
	./check_pgss_store

clean:
	rm -f check_pgss_store

all: check_pgss_store test clean

.PHONY: packages check_pgss_store test all
