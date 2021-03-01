# src/bin/zenith_push/Makefile

PGFILEDESC = "s3push - push an existing PostgreSQL cluster to S3"
PGAPPICON=win32

subdir = src/bin/zenith_push
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(WIN32RES) \
	compat.o \
	zenith_push.o

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/zenith_s3 -lzeniths3
LDFLAGS_INTERNAL += -lcurl

all: zenith_push

zenith_push: $(OBJS) | submake-libpgport submake-libpgfeutils submake-libzeniths3
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) zenith_push$(X) '$(DESTDIR)$(bindir)/zenith_push$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/zenith_push$(X)'

clean distclean maintainer-clean:
	rm -f zenith_push$(X) $(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
