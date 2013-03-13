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

CREATE FUNCTION hll_sum2(internal, bytea)
     RETURNS internal
     AS 'MODULE_PATHNAME', 'hll_sum'
     LANGUAGE C;

CREATE FUNCTION hll_sum_fin(internal)
     RETURNS bigint
     AS 'MODULE_PATHNAME'
     LANGUAGE C;
     
CREATE AGGREGATE hll_sum(bytea) (
    sfunc = hll_sum2,
    stype = internal,
    finalfunc = hll_sum_fin
);