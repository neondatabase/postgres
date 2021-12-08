/* contrib/remotexact/remotexact--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION remotexact" to load this file. \quit

-- Register the function.
CREATE FUNCTION print_bytes(IN bytes bytea)
RETURNS bool
AS 'MODULE_PATHNAME', 'print_bytes'
LANGUAGE C STRICT;