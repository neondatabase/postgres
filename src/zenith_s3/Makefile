#-------------------------------------------------------------------------
#
# Makefile
#    Makefile for src/zenith_s3
#
# These files are used by the Postgres backend, and also by frontend
# programs.  These could be in src/common, but don't seem general-purpose
# enough for that.
#
# This makefile generates three outputs:
#
#	libzeniths3.a - contains object files with FRONTEND defined,
#		for use by client applications
#
#	libzeniths3_shlib.a - contains object files with FRONTEND defined,
#		built suitably for use in shared libraries; for use
#		by frontend libraries
#
#	libzeniths3_srv.a - contains object files without FRONTEND defined,
#		for use only by the backend
#
# IDENTIFICATION
#    src/zenith_s3/Makefile
#
#-------------------------------------------------------------------------

subdir = src/zenith_s3
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND -I. -I$(top_srcdir)/src/zenith_s3 $(CPPFLAGS)
LIBS += $(PTHREAD_LIBS)

# If you add objects here, see also src/tools/msvc/Mkvcbuild.pm
# FIXME: windows

OBJS = \
	s3_ops.o \
	s3_sign.o

# foo.o, foo_shlib.o, and foo_srv.o are all built from foo.c
OBJS_SHLIB = $(OBJS:%.o=%_shlib.o)
OBJS_SRV = $(OBJS:%.o=%_srv.o)

all: libzeniths3.a libzeniths3_shlib.a libzeniths3_srv.a

libzeniths3.a: $(OBJS)
	rm -f $@
	$(AR) $(AROPT) $@ $^

# currently not used by contrib, so don't install. (See fe_utils or common)

#
# Shared library versions of object files
#

libzeniths3_shlib.a: $(OBJS_SHLIB)
	rm -f $@
	$(AR) $(AROPT) $@ $^

# Because this uses its own compilation rule, it doesn't use the
# dependency tracking logic from Makefile.global.  To make sure that
# dependency tracking works anyway for the *_shlib.o files, depend on
# their *.o siblings as well, which do have proper dependencies.  It's
# a hack that might fail someday if there is a *_shlib.o without a
# corresponding *.o, but there seems little reason for that.
%_shlib.o: %.c %.o
	$(CC) $(CFLAGS) $(CFLAGS_SL) $(CPPFLAGS) -c $< -o $@

#
# Server versions of object files
#

libzeniths3_srv.a: $(OBJS_SRV)
	rm -f $@
	$(AR) $(AROPT) $@ $^

# Because this uses its own compilation rule, it doesn't use the
# dependency tracking logic from Makefile.global.  To make sure that
# dependency tracking works anyway for the *_srv.o files, depend on
# their *.o siblings as well, which do have proper dependencies.  It's
# a hack that might fail someday if there is a *_srv.o without a
# corresponding *.o, but it works for now.
%_srv.o: %.c %.o
	$(CC) $(CFLAGS) $(subst -DFRONTEND,, $(CPPFLAGS)) -c $< -o $@

clean distclean maintainer-clean:
	rm -f libzeniths3.a libzeniths3_shlib.a libzeniths3_srv.a
	rm -f $(OBJS) $(OBJS_SHLIB) $(OBJS_SRV)
