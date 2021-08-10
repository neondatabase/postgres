# contrib/zenith_test_utils/Makefile


MODULE_big = zenith_test_utils
OBJS = \
	$(WIN32RES) \
	zenithtest.o

EXTENSION = zenith_test_utils
DATA = zenith_test_utils--1.0.sql
PGFILEDESC = "zenith_test_utils - helpers for zenith testing and debugging"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/zenith_test_utils
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
