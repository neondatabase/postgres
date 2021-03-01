# contrib/file_fdw/Makefile
EXTENSION = zenith_store
OBJS = receiver_worker.o zenith_store.o
MODULE_big = zenith_store
PGFILEDESC = "zenith_store"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/zenith_store
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

check: temp-install
	$(prove_check)