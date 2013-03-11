\echo Use "CREATE EXTENSION pghll" to load this file. \quit

CREATE FUNCTION hll_decode(bytea)
     RETURNS bytea
     AS 'MODULE_PATHNAME'
     LANGUAGE C STRICT IMMUTABLE;
     
CREATE FUNCTION hll_count(bytea)
     RETURNS bigint
     AS 'MODULE_PATHNAME'
     LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hll_merge2(bytea, bytea)
     RETURNS bytea
     AS 'MODULE_PATHNAME', 'hll_merge'
     LANGUAGE C;
     
CREATE AGGREGATE hll_merge(bytea) (
    sfunc = hll_merge2,
    stype = bytea
);