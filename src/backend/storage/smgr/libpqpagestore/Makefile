#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/storage/smgr/libpqpagestore
#
# IDENTIFICATION
#    src/backend/storage/smgr/libpqpagestore/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/smgr/libpqpagestore
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(srcdir) -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = \
	$(WIN32RES) \
	libpqpagestore.o
SHLIB_LINK_INTERNAL = $(libpq)
SHLIB_LINK = $(filter -lintl, $(LIBS))
SHLIB_PREREQS = submake-libpq
PGFILEDESC = "libpqpagestore"
NAME = libpqpagestore

all: all-shared-lib

include $(top_srcdir)/src/Makefile.shlib

install: all installdirs install-lib

installdirs: installdirs-lib

uninstall: uninstall-lib

clean distclean maintainer-clean: clean-lib
	rm -f $(OBJS)
