# contrib/neon_test_utils/Makefile


MODULE_big = neon_test_utils
OBJS = \
	$(WIN32RES) \
	neontest.o

EXTENSION = neon_test_utils
DATA = neon_test_utils--1.0.sql
PGFILEDESC = "neon_test_utils - helpers for neon testing and debugging"

EXTRA_INSTALL=contrib/neon

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
PG_CPPFLAGS = -I$(top_srcdir)/contrib
subdir = contrib/neon_test_utils
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
