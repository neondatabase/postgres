#include "postgres.h"

#include <stdio.h>
#include <curl/curl.h>

#include "common/hex.h"
#include "fe_utils/zenith_s3_sign.h"
#include "put_s3.h"
#include "zenith_push.h"

/* FIXME: 'auto' is a googlism, not sure what to put here for others */
static const char *region = "auto";
static const char *endpoint = "https://localhost:9000";
static const char *host; /* derived from endpoint */
static const char *bucket = "foobucket";
static const char *accesskeyid = "minioadmin";
static const char *secret = "";

static bool initialized = false;

static char *getenv_with_default(const char *name, char *default_value);
static size_t read_callback(char *ptr, size_t size, size_t nmemb, void *stream);

static void
init_s3()
{
	if (initialized)
		return;
	/* read config from env variables */
	region = getenv_with_default("S3_REGION", "auto");
	endpoint = getenv_with_default("S3_ENDPOINT", "https://localhost:9000");
	if (strncmp(endpoint, "https://", 8) == 0)
		host = &endpoint[8];
	else if (strncmp(endpoint, "http://", 7) == 0)
		host = &endpoint[7];
	else
		host = endpoint;
	bucket = getenv_with_default("S3_BUCKET", "zenith-testbucket");
	accesskeyid = getenv_with_default("S3_ACCESSKEY", "");
	secret = getenv_with_default("S3_SECRET", "");

	initialized = true;
}

static char *
getenv_with_default(const char *name, char *default_value)
{
	char	   *value;

	value = getenv(name);
	if (value)
		return value;
	else
		return default_value;
}

struct read_file_source
{
	size_t		filesize;
	size_t		total_read;
	FILE	   *fp;
};

static size_t
read_callback(char *ptr, size_t size, size_t nmemb, void *stream)
{
	struct read_file_source *src = (struct read_file_source *) stream;
	size_t		nread;
	size_t		nleft;

	nleft = src->filesize - src->total_read;
	if (nmemb * size > nleft)
	{
		nmemb = nleft / size;
		if (nmemb == 0)
			return 0;
	}

	nread = fread(ptr, size, nmemb, src->fp);
	src->total_read += nread;

	return nread;
}

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

static char *
compute_file_hash(FILE *fp)
{
	pg_cryptohash_ctx *sha256ctx;
	uint8		bodyhashbuf[PG_SHA256_DIGEST_LENGTH];

	/* Calculate hash of request body */
	/* FIXME: check errors */
	sha256ctx = pg_cryptohash_create(PG_SHA256);
	pg_cryptohash_init(sha256ctx);

	for (;;)
	{
		char		buf[65536];
		size_t		nread;
		
		nread = fread(buf, 1, sizeof(buf), fp);
		if (nread == 0)
			break;

		pg_cryptohash_update(sha256ctx, (unsigned char *) buf, nread);
	}

	pg_cryptohash_final(sha256ctx, bodyhashbuf, sizeof(bodyhashbuf));
	return gethex(bodyhashbuf, PG_SHA256_DIGEST_LENGTH);
}

/*
 * Put a file to S3
 */
void
put_s3_file(const char *localpath, const char *s3path, size_t filesize)
{
	CURL	   *curl;
	CURLcode	res;
	FILE	   *fp;
	char	   *url;
	char	   *hosthdr;
	struct curl_slist *headers;
	SimpleStringList *auth_headers;
	SimpleStringListCell *h;
	long		http_code = 0;
	struct read_file_source src;
	char	   *bodyhash;
	char	   *urlpath;

	init_s3();

	urlpath = psprintf("/%s/%s", bucket, s3path);

	url = psprintf("%s%s", endpoint, urlpath);

	//fprintf(stderr, "putting: %s\n", url);

	hosthdr = psprintf("Host: %s", host);

	curl = curl_easy_init();
	if (!curl)
		pg_fatal("could not init libcurl");

    curl_easy_setopt(curl, CURLOPT_URL, url);

	fp = fopen(localpath, PG_BINARY_R);
	if (!fp)
		pg_fatal("could not open file \"%s\" for reading: %m", localpath);

	bodyhash = compute_file_hash(fp);
	fseek(fp, 0, SEEK_SET);

    /* enable uploading (implies PUT over HTTP) */
	src.filesize = filesize;
	src.total_read = 0;
	src.fp = fp;
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback);
    curl_easy_setopt(curl, CURLOPT_READDATA, &src);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE,
                     (curl_off_t) filesize);

	auth_headers = s3_get_authorization_hdrs(endpoint, region, "PUT", urlpath,
											 bodyhash, accesskeyid, secret);
	headers = NULL;
	headers = curl_slist_append(headers, hosthdr);

	for (h = auth_headers->head; h != NULL; h = h->next)
	{
		headers = curl_slist_append(headers, h->val);
		//fprintf(stderr, "%s\n", h->val);
	}
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);

	curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    /* Perform the request, res will get the return code */
    res = curl_easy_perform(curl);
    /* Check for errors */
    if (res != CURLE_OK)
		pg_fatal("curl_easy_perform() failed: %s", curl_easy_strerror(res));

	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
	if (http_code == 200)
	{
		//Succeeded
	}
	else
	{
		//Failed
		pg_fatal("got http error: %ld on path %s", http_code, s3path);
	}

    /* always cleanup */
    curl_easy_cleanup(curl);

	if (fclose(fp) < 0)
		pg_fatal("could not read file %s", localpath);

	fprintf(stderr, "uploaded file \"%s\" to \"%s\"\n", localpath, url);

	pfree(url);
	pfree(hosthdr);
}

