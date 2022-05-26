# contrib/neon/Makefile


MODULE_big = neon
OBJS = \
	$(WIN32RES) \
	inmem_smgr.o libpagestore.o pagestore_smgr.o relsize_cache.o neon.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = neon
DATA = neon--1.0.sql
PGFILEDESC = "neon - cloud storage for PostgreSQL"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/neon
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
