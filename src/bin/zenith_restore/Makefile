# src/bin/zenith_restore/Makefile

PGFILEDESC = "zenith_restore - decode and redistribute WAL per datafile"
PGAPPICON=win32

subdir = src/bin/zenith_restore
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(WIN32RES) \
	compat.o \
	zenith_restore.o

#	$(RMGRDESCOBJS) \


override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

#RMGRDESCSOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/access/rmgrdesc/*desc.c)))
#RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))

LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/zenith_s3 -lzeniths3
LDFLAGS_INTERNAL += -lcurl

all: zenith_restore

zenith_restore: $(OBJS) | submake-libpgport submake-libpgfeutils submake-libzeniths3
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

#$(RMGRDESCSOURCES): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
#	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) zenith_restore$(X) '$(DESTDIR)$(bindir)/zenith_restore$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/zenith_restore$(X)'

clean distclean maintainer-clean:
	rm -f zenith_restore$(X) $(OBJS) $(RMGRDESCSOURCES) xlogreader.c
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
