\echo Use "CREATE EXTENSION zenith" to load this file. \quit

CREATE FUNCTION pg_cluster_size()
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_cluster_size'
LANGUAGE C STRICT
PARALLEL UNSAFE;