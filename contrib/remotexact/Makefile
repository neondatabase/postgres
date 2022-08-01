# contrib/remotexact/Makefile

MODULE_big = remotexact
OBJS = \
	$(WIN32RES)\
	remotexact.o \
	rwset.o \
	funcs.o 

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = remotexact
PGFILEDESC = "remotexact - remote transactions for multi-master PostgreSQL"
DATA = remotexact--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/remotexact
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
