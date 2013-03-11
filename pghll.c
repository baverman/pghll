#include <stdint.h>
#include <math.h>
#include <byteswap.h>
#include <zlib.h>

#include "postgres.h"
#include "fmgr.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define UBUF_LEN 16384

static double alpha_MM(int count) {
	return (0.7213 / (1.0 + 1.079 / count)) * count * count;
}

static double calc_sum_and_zeros(int count, uint32_t *data, int *zeros) {
	double sum = 0;
	int zcount = 0;
	int i = 0;
	int j = 0;
	int shift = 0;
	for (i = 0; j < count; i++) {
		uint32_t value = bswap_32(data[i]);
		for (shift = 0; j < count && shift < 30; j++, shift += 5) {
			int v = (value & (0x1f << shift)) >> shift;
			sum += pow(2, -v);
			if ( v == 0 ) {
				zcount++;	
			}		
		}
	}
	*zeros = zcount;
	return sum;
}

static int64 cardinality(int count, uint32_t *data, int enable_long_range_correction) {
	int zeros = 0;
	double POW_2_32 = pow(2, 32);
	double register_sum = calc_sum_and_zeros(count, data, &zeros);
    double estimate = alpha_MM(count) * (1.0 / register_sum);
	double result = 0;

    if ( estimate <= (5.0 / 2.0) * count ) {
        result = count * log(count * 1.0 / zeros);
    } else if ( estimate <= (1.0 / 30.0) * POW_2_32 ) {
        result = estimate;
    } else if ( estimate > (1.0 / 30.0) * POW_2_32 ) {
        if ( enable_long_range_correction ) {
            result = (-POW_2_32 * log(1.0 - (estimate / POW_2_32)));
        } else {
            result = estimate;
		}
	}
 	
    return (int64)(result + 0.5);
}

static void merge_sets(int count, uint32_t *source, uint32_t *dest) {
	int i = 0;
	int j = 0;
	int shift = 0;
	for (i = 0; j < count; i++) {
		uint32_t svalue = bswap_32(source[i]);
		uint32_t dvalue = bswap_32(dest[i]);
		uint32_t result = 0;
		for (shift = 0; j < count && shift < 30; j++, shift += 5) {
			int sv = (svalue & (0x1f << shift)) >> shift;
			int dv = (dvalue & (0x1f << shift)) >> shift;
			if ( sv > dv ) {
				result += sv << shift;	
			} else {
				result += dv << shift;
			}		
		}
		dest[i] = bswap_32(result);
	}
}

PG_FUNCTION_INFO_V1(hll_decode);
Datum hll_decode(PG_FUNCTION_ARGS) {
    bytea *data = PG_GETARG_BYTEA_P(0);
    bytea *udata = (bytea *) palloc(UBUF_LEN);

    uLongf dest_size = UBUF_LEN;
    int res = uncompress(VARDATA(udata), &dest_size, VARDATA(data), VARSIZE(data));
    if ( res != Z_OK ) {
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("can't decode value")));		    	
    }
    SET_VARSIZE(udata, dest_size + VARHDRSZ);
    PG_RETURN_BYTEA_P(udata);
}

PG_FUNCTION_INFO_V1(hll_count);
Datum hll_count(PG_FUNCTION_ARGS) {
    bytea *arg = PG_GETARG_BYTEA_P(0);
	uint32_t *data = (uint32_t *) VARDATA(arg);
    int count = 1 << bswap_32(data[0]);
    int64 result = cardinality(count, data + 2, 1);
    PG_RETURN_INT64(result);
}

PG_FUNCTION_INFO_V1(hll_merge);
Datum hll_merge(PG_FUNCTION_ARGS) {
	bytea    *state;
	uint32_t *sdata;

	bytea    *value = PG_GETARG_BYTEA_P(1);
	uint32_t *vdata = (uint32_t *) VARDATA(value);
	 
	if ( PG_ARGISNULL(0) ) {
		state = (bytea *) palloc(VARSIZE(value));
		SET_VARSIZE(state, VARSIZE(value));
		sdata = (uint32_t *) VARDATA(state); 
		memset(sdata, '\0', VARSIZE(value) - VARHDRSZ);
		sdata[0] = vdata[0];
		sdata[1] = vdata[1];				
	} else {
		state = PG_GETARG_BYTEA_P(0);
		sdata = (uint32_t *) VARDATA(state); 
	}

	merge_sets(1 << bswap_32(sdata[0]), vdata + 2, sdata + 2);
	PG_RETURN_BYTEA_P(state);
}