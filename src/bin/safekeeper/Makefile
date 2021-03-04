#-------------------------------------------------------------------------
#
# Makefile for src/bin/safekeeper
#
# Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/safekeeper/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "safekeeper - streaming WAL to safekeepers"
PGAPPICON=win32

EXTRA_INSTALL=contrib/test_decoding

subdir = src/bin/safekeeper
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) -I../pg_basebackup $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

OBJS = $(WIN32RES) \
	utils.o \
    streamutil.o

all: safekeeper safekeeper_proxy

safekeeper: safekeeper.o $(OBJS) | submake-libpq submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) safekeeper.o $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

safekeeper_proxy: safekeeper_proxy.o $(OBJS) | submake-libpq submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) safekeeper_proxy.o $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

streamutil.o: ../pg_basebackup/streamutil.c
	$(CC) -c $(CFLAGS) $(CPPFLAGS) ../pg_basebackup/streamutil.c -o $@

install: all installdirs
	$(INSTALL_PROGRAM) safekeeper$(X) '$(DESTDIR)$(bindir)/safekeeper$(X)'
	$(INSTALL_PROGRAM) safekeeper_proxy$(X) '$(DESTDIR)$(bindir)/safekeeper_proxy$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/safekeeper$(X)'
	rm -f '$(DESTDIR)$(bindir)/safekeeper_proxy$(X)'

clean distclean maintainer-clean:
	rm -f safekeeper$(X) safekeeper_proxy$(X) \
		safekeeper.o safekeeper_proxy.o streamutil.o \
		$(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
