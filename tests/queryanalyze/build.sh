#!/bin/bash -e

cd ~/dev/postgresql-15.0
cd src

CPPFLAGS="-O2 -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv"

LIBFLAGS="-lm"
if [ `uname` != "Darwin" ]; then
	LIBFLAGS+=" -lrt -ldl"
fi

export LDFLAGS="-Wl,--copy-dt-needed-entries"

OBJFILES=`find backend -name '*.o' | egrep -v '(main/main\.o|snowball|libpqwalreceiver|conversion_procs)' | xargs echo`
OBJFILES+=" timezone/localtime.o timezone/strftime.o timezone/pgtz.o"
OBJFILES+=" common/libpgcommon_srv.a port/libpgport_srv.a"

gcc $CPPFLAGS -L/usr/local/lib -pthread -Lport -Lcommon -I include ~/dev/postgresql-15.0/contrib/pg_stat_query_plans/tests/queryanalyze/queryparser.c $OBJFILES $LIBFLAGS -o ~/dev/postgresql-15.0/contrib/pg_stat_query_plans/tests/queryanalyze/queryparser
