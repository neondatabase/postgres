#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/smgr
#
# IDENTIFICATION
#    src/backend/storage/smgr/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/smgr
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	fetch_s3.o \
	s3_sign.o \
	lazyrestore.o \
	md.o \
	smgr.o

include $(top_srcdir)/src/backend/common.mk
