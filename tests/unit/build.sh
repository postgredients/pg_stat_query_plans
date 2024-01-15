#!/bin/bash -e

CURRENT_DIR=$PWD
SRC_DIR=`pwd`/../../../../src/

# CPPFLAGS="-O2 -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv"

LIBFLAGS="-lm"
if [ `uname` != "Darwin" ]; then
	LIBFLAGS+=" -lrt -ldl"
fi

export LDFLAGS="-Wl,--copy-dt-needed-entries"

OBJFILES=`find ${SRC_DIR}/backend -name '*.o' | egrep -v '(main/main\.o|snowball|libpqwalreceiver|conversion_procs|pgoutput)' | xargs echo`
OBJFILES+=" ${SRC_DIR}/timezone/localtime.o ${SRC_DIR}/timezone/strftime.o ${SRC_DIR}/timezone/pgtz.o"
OBJFILES+=" ${SRC_DIR}/common/libpgcommon_srv.a ${SRC_DIR}/port/libpgport_srv.a"
OBJFILES+=" ../../pg_stat_query_plans_storage.o ../../pg_stat_query_plans.o ../../pg_stat_query_plans_parser.o"

gcc -g -I/usr/include/postgresql/15/server -Wall -o check_pgss_store check_pgss_store.c $OBJFILES $LIBFLAGS $(pkg-config --cflags --libs check)

# gcc $CPPFLAGS -L/usr/local/lib -pthread -Lport -Lcommon -I include $CURRENT_DIR/check_pgss_store.c $OBJFILES $LIBFLAGS -o CURRENT_DIR/check_pgss_store
