#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for contrib/neon/libpqwalproposer
#
# IDENTIFICATION
#    contrib/neon/libpqwalproposer/Makefile
#
#-------------------------------------------------------------------------

MODULE_big = libpqwalproposer
OBJS = \
	$(WIN32RES) \
	libpqwalproposer.o
PGFILEDESC = "libpqwalproposer - libpq interface for WAL proposer"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/neon/libpqwalproposer
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
