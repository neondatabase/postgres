# contrib/file_fdw/Makefile
EXTENSION = zenith_store
OBJS = receiver_worker.o zenith_store.o memstore.o
MODULE_big = zenith_store
PGFILEDESC = "zenith_store"
DATA = zenith_store--1.0.sql

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTRA_INSTALL=contrib/pageinspect

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

start: temp-install
	rm -rf '$(CURDIR)'/tmp_check
	$(MKDIR_P) '$(CURDIR)'/tmp_check
	cd $(srcdir) && TESTDIR='$(CURDIR)' \
		$(with_temp_install) \
		PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' \
		perl $(PG_PROVE_FLAGS) control.pl --action=start $(RUN_OPTS)

stop:
	cd $(srcdir) && TESTDIR='$(CURDIR)' \
		PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' \
		perl $(PG_PROVE_FLAGS) control.pl --action=stop $(RUN_OPTS)

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all
	$(MAKE) -C $(top_builddir)/src/test/regress tablespace-setup

run-regress: submake-regress
	cd $(CURDIR)/$(top_builddir)/src/test/regress && \
	$(with_temp_install) \
	PGPORT='65432' \
	PGHOST='127.0.0.1' \
	PGUSER='$(USER)' \
	./pg_regress \
	--bindir='' \
	--use-existing \
	--schedule=$(abs_top_srcdir)/src/test/regress/parallel_schedule \
	--dlpath=$(CURDIR)/$(top_builddir)/src/test/regress \
	--inputdir=$(abs_top_srcdir)/src/test/regress
