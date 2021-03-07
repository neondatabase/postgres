-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION zenith_store" to load this file. \quit


CREATE FUNCTION zenith_store.get_page(int8, Oid, Oid, Oid, int4, int8) RETURNS bytea
AS 'MODULE_PATHNAME','zenith_store_get_page'
LANGUAGE C;

CREATE FUNCTION zenith_store.dispatcher_loop() RETURNS void
AS 'MODULE_PATHNAME','zenith_store_dispatcher'
LANGUAGE C;

