
#include "postgres.h"

#include <stdio.h>
#include <curl/curl.h>

#include <libxml/chvalid.h>
#include <libxml/parser.h>
#include <libxml/parserInternals.h>
#include <libxml/tree.h>
#include <libxml/uri.h>
#include <libxml/xmlerror.h>
#include <libxml/xmlversion.h>
#include <libxml/xmlwriter.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#include "common/cryptohash.h"
#include "common/sha2.h"
#include "common/hex.h"
#include "common/logging.h"
#include "lib/stringinfo.h"
#include "fe_utils/simple_list.h"
#include "fe_utils/zenith_s3_sign.h"
#include "s3_ops.h"


/* FIXME: 'auto' is a googlism, not sure what to put here for others */
static const char *region = "auto";
static const char *endpoint = "https://localhost:9000";
static const char *host; /* derived from endpoint */
static const char *bucket = "foobucket";
static const char *accesskeyid = "minioadmin";
static const char *secret = "";

static bool initialized = false;

/* logging support */
#define pg_fatal(...) do { pg_log_fatal(__VA_ARGS__); exit(1); } while(0)

static char *getenv_with_default(const char *name, char *default_value);
static size_t write_callback(void *data, size_t size, size_t nmemb, void *userp);
static ListObjectsResult *parse_listobjects_result(StringInfo response);
static void parseContents(xmlDocPtr doc, xmlNodePtr contents, ListObjectsResult *result);

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

/* Curl WRITEDATA callback. Collects the data to a StringInfo */
static size_t
write_callback(void *data, size_t size, size_t nmemb, void *userp)
{
	size_t		realsize = size * nmemb;
	StringInfo	buf = (StringInfo) userp;

	appendBinaryStringInfo(buf, data, realsize);
 
	return realsize;
}

ListObjectsResult *
s3_ListObjects(const char *s3path)
{
	CURL	   *curl;
	CURLcode	res;
	char	   *url;
	char	   *hosthdr;
	struct curl_slist *headers;
	SimpleStringList *auth_headers;
	SimpleStringListCell *h;
	long		http_code = 0;
	StringInfoData responsebuf;
	ListObjectsResult *result = NULL;
	char	   *urlpath;

	init_s3();

	urlpath = psprintf("/%s/%s", bucket, s3path);

	url = psprintf("%s%s", endpoint, urlpath);

	fprintf(stderr, "listing: %s\n", url);

	hosthdr = psprintf("Host: %s", host);
 
	curl = curl_easy_init();
	if (!curl)
		pg_fatal("could not init libcurl");

	initStringInfo(&responsebuf);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &responsebuf);
    curl_easy_setopt(curl, CURLOPT_URL, url);

	auth_headers = s3_get_authorization_hdrs(host, region, "GET", urlpath, NULL,
											 accesskeyid, secret);
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
		result = parse_listobjects_result(&responsebuf);
	}
	else
	{
		//Failed
		pg_fatal("got http error: %ld on path %s", http_code, s3path);
	}

	/* FIXME: One ListObjects call returns only 1000 entries. Continue! */
	if (result->numfiles == 1000)
		pg_fatal("TODO: ListObjects continuation not implemented");
	
    /* always cleanup */ 
    curl_easy_cleanup(curl);

	pfree(url);
	pfree(hosthdr);

	return result;
}

static ListObjectsResult *
parse_listobjects_result(StringInfo response)
{
	xmlDocPtr	doc;
	xmlNodePtr	cur;
	ListObjectsResult *result;

	result = palloc(sizeof(ListObjectsResult));
	result->filenames = palloc(sizeof(char *) * 1000);
	result->numfiles = 0;

	doc = xmlParseMemory(response->data, response->len);
	if (doc == NULL)
		pg_fatal("could not parse XML response");

	cur = xmlDocGetRootElement(doc);
	if (cur == NULL)
		pg_fatal("empty document\n");

	if (xmlStrcmp(cur->name, (const xmlChar *) "ListBucketResult") != 0)
		pg_fatal("unexpected result document type for ListBucket: %s", cur->name);

	cur = cur->xmlChildrenNode;
	while (cur != NULL)
	{
		if (xmlStrcmp(cur->name, (const xmlChar *) "Contents") == 0)
		{
			parseContents(doc, cur, result);
		}
		cur = cur->next;
	}

	xmlFreeDoc(doc);
	return result;
}

static void
parseContents(xmlDocPtr doc, xmlNodePtr contents, ListObjectsResult *result)
{
	xmlChar *key;
	xmlNodePtr cur;

	cur = contents->xmlChildrenNode;
	while (cur != NULL)
	{
	    if (xmlStrcmp(cur->name, (const xmlChar *) "Key") == 0)
		{
		    key = xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
			result->filenames = repalloc(result->filenames, (result->numfiles + 1) * sizeof(char *));
			result->filenames[result->numfiles++] = pstrdup((char *) key);
		    xmlFree(key);
 	    }
		cur = cur->next;
	}
}

/*
 * Fetch a file from S3 compatible storage, returning it as a blob of memory
 */
StringInfo
fetch_s3_file_memory(const char *s3path)
{
	CURL	   *curl;
	CURLcode	res;
	char	   *url;
	char	   *hosthdr;
	struct curl_slist *headers;
	SimpleStringList *auth_headers;
	SimpleStringListCell *h;
	long		http_code = 0;
	StringInfo	result;
	char	   *urlpath;

	init_s3();

	urlpath = psprintf("/%s/%s", bucket, s3path);

	url = psprintf("%s%s", endpoint, urlpath);

	fprintf(stderr, "fetching: %s\n", url);

	hosthdr = psprintf("Host: %s", host);
 
	curl = curl_easy_init();
	if (!curl)
		pg_fatal("could not init libcurl");

    curl_easy_setopt(curl, CURLOPT_URL, url);

	result = makeStringInfo();
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) result);

	auth_headers = s3_get_authorization_hdrs(host, region, "GET", urlpath, NULL,
											 accesskeyid, secret);
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

	fprintf(stderr, "fetched \"%s\"\n", url);

	pfree(url);
	pfree(hosthdr);

	return result;
}


struct read_file_source
{
	size_t filesize;
	size_t total_read;
	FILE *fp;
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

	auth_headers = s3_get_authorization_hdrs(host, region, "PUT", urlpath,
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
