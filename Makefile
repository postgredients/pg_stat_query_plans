# contrib/pg_stat_query_plans/Makefile

MODULE_big = pg_stat_query_plans
OBJS = \
	$(WIN32RES) \
	pg_stat_query_plans.o \
	pg_stat_query_plans_storage.o \
	pg_stat_query_plans_parser.o

EXTENSION = pg_stat_query_plans
DATA = pg_stat_query_plans--1.0.sql
PGFILEDESC = "pg_stat_query_plans - execution statistics and plans of SQL statements"

# LDFLAGS_SL += $(filter -lm, $(LIBS))
ifneq ($(shell uname), SunOS)
LDFLAGS+=-Wl,--build-id
endif

REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/pg_stat_query_plans/pg_stat_query_plans.conf
REGRESS = pg_stat_query_plans_sql pg_stat_query_plans
# Disabled because these tests require
# "shared_preload_libraries=pg_stat_query_plans",
# which typical installcheck users do not have (e.g. buildfarm clients).
# NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_stat_query_plans
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
