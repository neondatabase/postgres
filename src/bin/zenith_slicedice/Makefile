# src/bin/zenith_slicedice/Makefile

PGFILEDESC = "zenith_slicedice - decode and redistribute WAL per datafile"
PGAPPICON=win32

subdir = src/bin/zenith_slicedice
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(RMGRDESCOBJS) \
	$(WIN32RES) \
	compat.o \
	zenith_slicedice_main.o \
	zenith_slicedice.o \
	xlogreader.o

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/zenith_s3 -lzeniths3
LDFLAGS_INTERNAL += -lcurl

RMGRDESCSOURCES = xactdesc.c standbydesc.c
RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))

#RMGRDESCSOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/access/rmgrdesc/*desc.c)))
#RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))


all: zenith_slicedice

zenith_slicedice: $(OBJS) | submake-libpgport submake-libpgfeutils submake-libzeniths3
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

$(RMGRDESCSOURCES): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) zenith_slicedice$(X) '$(DESTDIR)$(bindir)/zenith_slicedice$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/zenith_slicedice$(X)'

clean distclean maintainer-clean:
	rm -f zenith_slicedice$(X) $(OBJS) xactdesc.c standbydesc.c xlogreader.c
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
