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
	lazyrestore.o \
	md.o \
	pageserver.o \
	smgr.o

SUBDIRS = libpqpageserver

include $(top_srcdir)/src/backend/common.mk
