#include "postgres.h"

#include <time.h>

#include "common/cryptohash.h"
#include "common/hex.h"
#include "common/sha2.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "pgtime.h"

#include "storage/fetch_s3.h"


static char *
gethex(uint8 *input, int inputlen)
{
	int			bufsize;
	char	   *buf;
	int			len;

	bufsize = pg_hex_enc_len(inputlen);
	buf = palloc(bufsize + 1);
	len = pg_hex_encode((const char *) input, inputlen, buf, bufsize);
	buf[len] = '\0';

	return buf;
}


static void
appendDate(StringInfo buf, pg_time_t t, const char *fmt)
{
	enlargeStringInfo(buf, 128);
	buf->len += pg_strftime(&buf->data[buf->len], 128, fmt, pg_gmtime(&t));
}

static void
hmac_sha256(uint8 *result, size_t resultlen, const uint8 *key, int keylen, const uint8 *msg, int msglen)
{
	uint8		k_ipad[PG_SHA256_BLOCK_LENGTH];
	uint8		k_opad[PG_SHA256_BLOCK_LENGTH];
	int			i;
	uint8		keybuf[PG_SHA256_DIGEST_LENGTH];
	pg_cryptohash_ctx *sha256ctx;
	uint8		h[PG_SHA256_DIGEST_LENGTH];

#define HMAC_IPAD 0x36
#define HMAC_OPAD 0x5C

	/*
	 * If the key is longer than the block size (64 bytes for SHA-256), pass
	 * it through SHA-256 once to shrink it down.
	 */
	if (keylen > PG_SHA256_BLOCK_LENGTH)
	{
		pg_cryptohash_ctx *sha256_ctx;

		sha256_ctx = pg_cryptohash_create(PG_SHA256);
		if (sha256_ctx == NULL)
			elog(ERROR, "out of memory");
		if (pg_cryptohash_init(sha256_ctx) < 0 ||
			pg_cryptohash_update(sha256_ctx, (uint8 *) key, keylen) < 0 ||
			pg_cryptohash_final(sha256_ctx, keybuf, sizeof(keybuf)) < 0)
		{
			pg_cryptohash_free(sha256_ctx);
			elog(ERROR, "cryptohash failure");
		}
		key = keybuf;
		keylen = PG_SHA256_DIGEST_LENGTH;
		pg_cryptohash_free(sha256_ctx);
	}

	memset(k_ipad, HMAC_IPAD, PG_SHA256_BLOCK_LENGTH);
	memset(k_opad, HMAC_OPAD, PG_SHA256_BLOCK_LENGTH);

	for (i = 0; i < keylen; i++)
	{
		k_ipad[i] ^= key[i];
		k_opad[i] ^= key[i];
	}

	sha256ctx = pg_cryptohash_create(PG_SHA256);
	if (sha256ctx == NULL)
		elog(ERROR, "out of memory");
		
	/* tmp = H(K XOR ipad, text) */
	if (pg_cryptohash_init(sha256ctx) < 0 ||
		pg_cryptohash_update(sha256ctx, k_ipad, PG_SHA256_BLOCK_LENGTH) < 0)
	{
		pg_cryptohash_free(sha256ctx);
		elog(ERROR, "cryptohash failure");
	}

	if (pg_cryptohash_update(sha256ctx, (const uint8 *) msg, msglen) < 0)
	{
		pg_cryptohash_free(sha256ctx);
		elog(ERROR, "cryptohash failure");
	}

	if (pg_cryptohash_final(sha256ctx, h, sizeof(h)) < 0)
	{
		pg_cryptohash_free(sha256ctx);
		elog(ERROR, "cryptohash failure");
	}

	/* H(K XOR opad, tmp) */
	if (pg_cryptohash_init(sha256ctx) < 0 ||
		pg_cryptohash_update(sha256ctx, k_opad, PG_SHA256_BLOCK_LENGTH) < 0 ||
		pg_cryptohash_update(sha256ctx, h, PG_SHA256_DIGEST_LENGTH) < 0 ||
		pg_cryptohash_final(sha256ctx, result, resultlen) < 0)
	{
		pg_cryptohash_free(sha256ctx);
		elog(ERROR, "cryptohash failure");
	}
	pg_cryptohash_free(sha256ctx);
}

//230c5a5ca9104202538ccb40fcbbfd4f4a8ac1c857ac855fab1492f5c7c94be3
static void
derive_signingkey(uint8 *result, pg_time_t today, const char *region, const char *service,
				  const char *secret)
{
	char		today_buf[128];
	int			today_len;
	uint8		hmacbuf[PG_SHA256_DIGEST_LENGTH];
	char	   *awssecret;

	today_len = pg_strftime(today_buf, 128, "%Y%m%d", pg_gmtime(&today));

	/* Derive SigningKey */
	awssecret = psprintf("AWS4%s", secret);

	hmac_sha256(hmacbuf, sizeof(hmacbuf), (uint8 *) awssecret, strlen(awssecret), (const uint8 *) today_buf, today_len); /* DateKey */
	hmac_sha256(hmacbuf, sizeof(hmacbuf), hmacbuf, PG_SHA256_DIGEST_LENGTH, (const uint8 *) region, strlen(region)); /* DateRegionKey */
	hmac_sha256(hmacbuf, sizeof(hmacbuf), hmacbuf, PG_SHA256_DIGEST_LENGTH, (const uint8 *) service, strlen(service)); /* DateRegionServiceKey */
	hmac_sha256(hmacbuf, sizeof(hmacbuf), hmacbuf, PG_SHA256_DIGEST_LENGTH, (const uint8 *) "aws4_request", strlen("aws4_request")); /* SigningKey */

	memcpy(result, hmacbuf, PG_SHA256_DIGEST_LENGTH);

	pfree(awssecret);
}


static char *
construct_string_to_sign(pg_time_t now, const char *method, const char *host, const char *region, const char *path, const char *query_string, const char *bodyhash)
{
	/* First, construct "canonical request" */
	StringInfoData canonical_request;
	StringInfoData string_to_sign;
	char		today_yymmdd[128];
	uint8		sha256hashbuf[PG_SHA256_DIGEST_LENGTH];
	pg_cryptohash_ctx *sha256ctx;
	const char *service = "s3";
	char	   *hex;

	(void) pg_strftime(today_yymmdd, sizeof(today_yymmdd), "%Y%m%d", pg_gmtime(&now));

	/* Construct CanonicalRequest */
	initStringInfo(&canonical_request);
	appendStringInfo(&canonical_request, "%s\n", method);
	/* FIXME: url-encode these */
	appendStringInfo(&canonical_request, "%s\n", path);
	appendStringInfo(&canonical_request, "%s\n", query_string); /* CANONICAL_QUERY_STRING */
	/* CANONICAL_HEADERS */
	appendStringInfo(&canonical_request, "host:%s\n", host);
	appendStringInfo(&canonical_request, "x-amz-content-sha256:%s\n", bodyhash);
	appendStringInfo(&canonical_request, "x-amz-date:");
	appendDate(&canonical_request, now, "%Y%m%dT%H%M%SZ");
	appendStringInfo(&canonical_request, "\n");

	appendStringInfo(&canonical_request, "\n");

	/* SIGNED_HEADERS */
	appendStringInfo(&canonical_request, "host;x-amz-content-sha256;x-amz-date\n");
	/* PAYLOAD */
	appendStringInfo(&canonical_request, "%s", bodyhash);

	//elog(LOG, "canonicalRequest:\n%s", canonical_request.data);

	/*
	 * Construct StringToSign
	 *
	 * SIGNING_ALGORITHM
	 * ACTIVE_DATETIME
	 * CREDENTIAL_SCOPE
	 * HASHED_CANONICAL_REQUEST
	 */
	initStringInfo(&string_to_sign);

	/* SIGNING_ALGORITHM: AWS4-HMAC-SHA256 */
	appendStringInfoString(&string_to_sign, "AWS4-HMAC-SHA256\n");

	/* ACTIVE_DATETIME: YYYYMMDD'T'HHMMSS'Z' */
	appendDate(&string_to_sign, now, "%Y%m%dT%H%M%SZ");
	appendStringInfoChar(&string_to_sign, '\n');

	/* CREDENTIAL_SCOPE: DATE/LOCATION/SERVICE/REQUEST_TYPE */
	appendStringInfo(&string_to_sign, "%s/%s/%s/aws4_request\n", today_yymmdd, region, service);

	/* FIXME: check errors */
	sha256ctx = pg_cryptohash_create(PG_SHA256);
	pg_cryptohash_init(sha256ctx);
	pg_cryptohash_update(sha256ctx, (uint8 *) canonical_request.data, canonical_request.len);
	pg_cryptohash_final(sha256ctx, sha256hashbuf, sizeof(sha256hashbuf));

	hex = gethex(sha256hashbuf, PG_SHA256_DIGEST_LENGTH);

	appendStringInfoString(&string_to_sign, hex);
	pfree(hex);
	pg_cryptohash_free(sha256ctx);

	//elog(LOG, "stringToSign:\n%s", string_to_sign.data);

	return string_to_sign.data;
}

List *
s3_get_authorization_hdrs(const char *host,
						  const char *region,
						  const char *method,
						  const char *path,
						  const char *accesskeyid,
						  const char *secret)
{
	char	   *string_to_sign;
	char	   *authhdr;
	StringInfoData datehdr;
	pg_time_t	now;
	uint8		SigningKey[PG_SHA256_DIGEST_LENGTH];
	uint8		signaturebuf[PG_SHA256_DIGEST_LENGTH];
	uint8		bodyhashbuf[PG_SHA256_DIGEST_LENGTH];
	char	   *bodyhash;
	char	   *signaturehex;
	List	   *headers = NIL;
	char		today_buf[128];
	pg_cryptohash_ctx *sha256ctx;

	/* Calculate hash of request body */
	/* FIXME: check errors */
	sha256ctx = pg_cryptohash_create(PG_SHA256);
	pg_cryptohash_init(sha256ctx);
	pg_cryptohash_update(sha256ctx, (uint8 *) "", 0);
	pg_cryptohash_final(sha256ctx, bodyhashbuf, sizeof(bodyhashbuf));
	bodyhash = gethex(bodyhashbuf, PG_SHA256_DIGEST_LENGTH);
	pg_cryptohash_free(sha256ctx);
	
	now = (pg_time_t) time(NULL);

	(void) pg_strftime(today_buf, 128, "%Y%m%d", pg_gmtime(&now));

	derive_signingkey(SigningKey, now, region, "s3", secret);

	string_to_sign = construct_string_to_sign(now, method, host, region, path, "", bodyhash);

	/* Construct Signature */
	hmac_sha256(signaturebuf, sizeof(signaturebuf), SigningKey, PG_SHA256_DIGEST_LENGTH, (uint8 *) string_to_sign, strlen(string_to_sign));

	signaturehex = gethex(signaturebuf, PG_SHA256_DIGEST_LENGTH);

	authhdr = psprintf("Authorization: AWS4-HMAC-SHA256 Credential=%s/%s/%s/%s/%s, SignedHeaders=%s, Signature=%s",
					   accesskeyid,
					   today_buf, /* scope */
					   region, /* scope */
					   "s3", /* scope */
					   "aws4_request", /* scope */
					   "host;x-amz-content-sha256;x-amz-date" /* signedheaders */,
					   signaturehex);

	pfree(signaturehex);

	headers = lappend(headers, authhdr);

	/* Contsruct date header */
	initStringInfo(&datehdr);
	appendStringInfo(&datehdr, "X-Amz-Date: ");
	appendDate(&datehdr, now, "%Y%m%dT%H%M%SZ");
	headers = lappend(headers, datehdr.data);

	/* add X-Amz-Content-Sha256 header */
	headers = lappend(headers, psprintf("X-Amz-Content-Sha256: %s", bodyhash));
	
	return headers;
}
