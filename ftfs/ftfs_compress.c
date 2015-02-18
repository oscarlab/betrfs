/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

#include <linux/types.h>
#include <linux/zlib.h>
#include <linux/zconf.h>
#include <linux/bug.h>
#include <linux/slab.h>
#include <linux/vmalloc.h>
#include "ftfs.h"

enum toku_compression_method { zlib };

size_t toku_compress_bound (enum toku_compression_method a, size_t size)
{
  return deflateBound(size);
}

static int init_stream_workspace(struct z_stream_s * pstr) {
	pstr->workspace = vmalloc(max(zlib_deflate_workspacesize(MAX_WBITS,MAX_MEM_LEVEL),zlib_inflate_workspacesize()));
	if (!pstr->workspace)
	      return -ENOMEM;
	return 0;
}

static void destroy_stream_workspace(struct z_stream_s * pstr) {
	vfree(pstr->workspace);
}
void toku_compress (enum toku_compression_method a,
		    // the following types and naming conventions come from zlib.h
		    void       *dest,   unsigned long *destLen,
		    const void *source, unsigned long   sourceLen)
{
  struct z_stream_s stream;
  int r;

  r = init_stream_workspace(&stream);
  if (r) {
	  ftfs_error(__func__, "zlib stream init failed!");
	  BUG();
  }

  r = zlib_deflateInit(&stream, 6);
  if (r != Z_OK) {
	  ftfs_error(__func__, "zlib_deflatInit failed!");
	  BUG();
  }
  stream.next_in = source;
  stream.avail_in = sourceLen;
  stream.total_in = 0;
  stream.next_out = dest;
  stream.avail_out = *destLen;
  stream.total_out = 0;

  r = zlib_deflate(&stream, Z_FINISH);

  if (r != Z_STREAM_END)
    BUG();

  r = zlib_deflateEnd(&stream);
  if (r != Z_OK)
    BUG();

  *destLen = stream.total_out;
   destroy_stream_workspace(&stream);
}

static int _toku_inflate(void*, unsigned int, const void *, unsigned int);

void toku_decompress (void       *dest,   unsigned long destLen,
		      const void *source, unsigned long sourceLen)
{
   int rc = _toku_inflate(dest, destLen, source, sourceLen);
   //printk("rc = %d, destlen = %d\n",rc, destLen);
   if(rc != destLen)
	 BUG();
}


//code borrowed from logfs
static int _toku_inflate(void *out, unsigned int outlen, const void *in, unsigned int inlen){
	const u8 *inbuf = in;
	struct z_stream_s stream;
	int r;
	int rc;
	r=init_stream_workspace(&stream);
	if(r){
		ftfs_error(__func__, "zlib stream init failed!");
		BUG();
	 }

	rc = -ENOMEM;

	stream.next_in = inbuf;
	stream.avail_in = inlen;
	stream.total_in = 0;
	stream.next_out = out;
	stream.avail_out = outlen;
	stream.total_out = 0;
	rc = zlib_inflateInit(&stream);
	if (rc == Z_OK) {
		rc = zlib_inflate(&stream, Z_FINISH);
		/*  after Z_FINISH, only Z_STREAM_END is "we unpacked it all" */
		if (rc == Z_STREAM_END)
			rc = outlen - stream.avail_out;
		else {
			ftfs_error(__func__, "zlib_inflate failed, rc = %d",rc);
			rc = -EINVAL;
		}
		zlib_inflateEnd(&stream);
	} else {
		ftfs_error(__func__, "zlib_inflateInit2 failed, rc=%d",rc);
		rc = -EINVAL;

	}

	destroy_stream_workspace(&stream);
	return rc; /*  returns Z_OK (0) if successful */
}
