/*-------------------------------------------------------------------------
 *
 * plpython_system.h - pull in Python's system header files
 *
 * We break this out as a separate header file to precisely control
 * the scope of the "system_header" pragma.  No Postgres-specific
 * declarations should be put here.  However, we do include some stuff
 * that is meant to prevent conflicts between our code and Python.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/pl/plpython/plpython_system.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLPYTHON_SYSTEM_H
#define PLPYTHON_SYSTEM_H

/*
 * Newer versions of the Python headers trigger a lot of warnings with our
 * preferred compiler flags (at least -Wdeclaration-after-statement is known
 * to be problematic). The system_header pragma hides warnings from within
 * the rest of this file, if supported.
 */
#ifdef HAVE_PRAGMA_GCC_SYSTEM_HEADER
#pragma GCC system_header
#endif

/*
 * Undefine some things that get (re)defined in the Python headers. They aren't
 * used by the PL/Python code, and all PostgreSQL headers should be included
 * earlier, so this should be pretty safe.
 */
#undef _POSIX_C_SOURCE
#undef _XOPEN_SOURCE

/*
 * Sometimes python carefully scribbles on our *printf macros.
 * So we undefine them here and redefine them after it's done its dirty deed.
 */
#undef vsnprintf
#undef snprintf
#undef vsprintf
#undef sprintf
#undef vfprintf
#undef fprintf
#undef vprintf
#undef printf

#if defined(_MSC_VER) && defined(_DEBUG)
/* Python uses #pragma to bring in a non-default libpython on VC++ if
 * _DEBUG is defined */
#undef _DEBUG
/* Also hide away errcode, since we load Python.h before postgres.h */
#define errcode __msvc_errcode
#include <Python.h>
#undef errcode
#define _DEBUG
#elif defined (_MSC_VER)
#define errcode __msvc_errcode
#include <Python.h>
#undef errcode
#else
#include <Python.h>
#endif

/* put back our *printf macros ... this must match src/include/port.h */
#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif
#ifdef vsprintf
#undef vsprintf
#endif
#ifdef sprintf
#undef sprintf
#endif
#ifdef vfprintf
#undef vfprintf
#endif
#ifdef fprintf
#undef fprintf
#endif
#ifdef vprintf
#undef vprintf
#endif
#ifdef printf
#undef printf
#endif

#define vsnprintf		pg_vsnprintf
#define snprintf		pg_snprintf
#define vsprintf		pg_vsprintf
#define sprintf			pg_sprintf
#define vfprintf		pg_vfprintf
#define fprintf			pg_fprintf
#define vprintf			pg_vprintf
#define printf(...)		pg_printf(__VA_ARGS__)

#endif							/* PLPYTHON_SYSTEM_H */
