# Copied from src/fe_utils/Makefile

subdir = src/redo
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = \
	redo.o

all: libredo.a

libredo.a: $(OBJS)
	rm -f $@
	$(AR) $(AROPT) $@ $^

psqlscan.c: FLEXFLAGS = -Cfe -p -p
psqlscan.c: FLEX_NO_BACKUP=yes
psqlscan.c: FLEX_FIX_WARNING=yes

distprep: psqlscan.c

# libpgfeutils could be useful to contrib, so install it
install: all installdirs
	$(INSTALL_STLIB) libredo.a '$(DESTDIR)$(libdir)/libredo.a'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libredo.a'

clean distclean:
	rm -f libredo.a $(OBJS) lex.backup

# psqlscan.c is supposed to be in the distribution tarball,
# so do not clean it in the clean/distclean rules
maintainer-clean: distclean
	rm -f psqlscan.c
