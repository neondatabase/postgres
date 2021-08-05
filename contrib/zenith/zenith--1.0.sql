-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION zenith" to load this file. \quit

CREATE FUNCTION test_consume_xids(nxids int)
RETURNS VOID
AS 'MODULE_PATHNAME', 'test_consume_xids'
LANGUAGE C STRICT
PARALLEL UNSAFE;
