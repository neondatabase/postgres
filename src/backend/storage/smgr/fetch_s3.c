
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

#include "storage/fd.h"
#include "storage/fetch_s3.h"

/* FIXME: 'auto' is a googlism, not sure what to put here for others */
static const char *region = "auto";
static const char *endpoint = "https://localhost:9000";
static const char *host; /* derived from endpoint */
static const char *bucket = "foobucket";
static const char *accesskeyid = "minioadmin";
static const char *secret = "";

static bool initialized = false;

static char *getenv_with_default(const char *name, char *default_value);

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

/*
 * Fetch a file from S3 compatible storage, and write it to 'dstpath'
 */
void
fetch_s3_file_restore(const char *s3path, const char *dstpath)
{
	CURL	   *curl;
	CURLcode	res;
	FILE	   *fp;
	char	   *url;
	char	   *hosthdr;
	struct curl_slist *headers;
	List	   *auth_headers;
	ListCell   *lc;
	long		http_code = 0;
	char	   *urlpath;

	init_s3();

	urlpath = psprintf("/%s/%s", bucket, s3path);

	url = psprintf("%s%s", endpoint, urlpath);

	hosthdr = psprintf("Host: %s", host);
 
	curl = curl_easy_init();
	if (!curl)
		elog(ERROR, "could not init libcurl");

    curl_easy_setopt(curl, CURLOPT_URL, url);

	fp = AllocateFile(dstpath, PG_BINARY_W);
	if (!fp)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for writing: %m",
						dstpath)));

	/* use the internal callback that writes to file */
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) fp);

	auth_headers = s3_get_authorization_hdrs(host, region, "GET", urlpath,
											 accesskeyid, secret);
	headers = NULL;
	headers = curl_slist_append(headers, hosthdr);
	foreach(lc, auth_headers)
	{
		headers = curl_slist_append(headers, (char *) lfirst(lc));
	}
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);

	curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    /* Perform the request, res will get the return code */ 
    res = curl_easy_perform(curl);
    /* Check for errors */ 
    if (res != CURLE_OK)
		elog(ERROR, "curl_easy_perform() failed: %s",
			 curl_easy_strerror(res));

	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
	if (http_code == 200)
	{
		//Succeeded
	}
	else
	{
		//Failed
		elog(ERROR, "got http error: %ld", http_code);
	}
	
    /* always cleanup */ 
    curl_easy_cleanup(curl);

	if (FreeFile(fp) < 0)
		elog(ERROR, "could not write file %s", dstpath);

	elog(LOG, "restored file \"%s\" from \"%s\"", dstpath, url);

	pfree(url);
	pfree(hosthdr);
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

static ListObjectsResult *parse_listobjects_result(StringInfo response);
static void parseContents(xmlDocPtr doc, xmlNodePtr contents, ListObjectsResult *result);

ListObjectsResult *
s3_ListObjects(const char *s3path)
{
	CURL	   *curl;
	CURLcode	res;
	char	   *url;
	char	   *hosthdr;
	struct curl_slist *headers;
	List	   *auth_headers;
	ListCell   *lc;
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
		elog(ERROR, "could not init libcurl");

	initStringInfo(&responsebuf);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &responsebuf);
	
    curl_easy_setopt(curl, CURLOPT_URL, url);

	auth_headers = s3_get_authorization_hdrs(host, region, "GET", urlpath,
											 accesskeyid, secret);
	headers = NULL;
	headers = curl_slist_append(headers, hosthdr);

	foreach(lc, auth_headers)
	{
		headers = curl_slist_append(headers, (char *) lfirst(lc));
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
		elog(ERROR, "curl_easy_perform() failed: %s", curl_easy_strerror(res));

	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
	if (http_code == 200)
	{
		//Succeeded
		result = parse_listobjects_result(&responsebuf);
	}
	else
	{
		//Failed
		elog(ERROR, "got http error: %ld on path %s", http_code, s3path);
	}

	/* FIXME: One ListObjects call returns only 1000 entries. Continue! */
	if (result->numfiles == 1000)
		elog(ERROR, "TODO: ListObjects continuation not implemented");
	
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
		elog(ERROR, "could not parse XML response");

	cur = xmlDocGetRootElement(doc);
	if (cur == NULL)
		elog(ERROR, "empty document\n");

	if (xmlStrcmp(cur->name, (const xmlChar *) "ListBucketResult") != 0)
		elog(ERROR, "unexpected result document type for ListBucket: %s", cur->name);

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
