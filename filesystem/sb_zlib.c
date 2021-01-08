/* -*- mode: C++; c-basic-offset: 8; indent-tabs-mode: t -*- */
// vim: set tabstop=8 softtabstop=8 shiftwidth=8 noexpandtab:

/* This file is part of the southbound (klibc) code.
 *
 * It implements zlib compression functions, using the Linux zlib
 * functions.
 */

#include <linux/zlib.h>
#include <linux/vmalloc.h>
#include "ftfs.h"

static int init_stream_workspace(struct z_stream_s * pstr) {
    pstr->workspace = vmalloc(max(zlib_deflate_workspacesize(MAX_WBITS,MAX_MEM_LEVEL), zlib_inflate_workspacesize()));
    if (!pstr->workspace)
          return -ENOMEM;
    return 0;
}

static void destroy_stream_workspace(struct z_stream_s * pstr) {
    vfree(pstr->workspace);
}

int deflateInit2_(z_streamp strm, int level, int method,
		  int windowBits, int memLevel, int strategy) {
    vfree(strm->workspace);
    init_stream_workspace(strm);
	return zlib_deflateInit2(strm, level,  method,
                    windowBits, memLevel, strategy);
}

int deflate (z_streamp strm, int flush) {
    return zlib_deflate(strm, Z_FINISH);
}

int deflateEnd (z_streamp strm) {
	int err = 0;
    err = zlib_deflateEnd(strm);
    destroy_stream_workspace(strm);
    return err;
}

int inflateInit2_(z_streamp strm, int  windowBits) {
    vfree(strm->workspace);
    init_stream_workspace(strm);
	return zlib_inflateInit2(strm, windowBits);
}

int inflate (z_streamp strm, int flush) {
	return zlib_inflate (strm, flush);
}

int inflateEnd (z_streamp strm) {
    int err = 0;
    err = zlib_inflateEnd (strm);
    destroy_stream_workspace(strm);
    return err;
}

/* ===========================================================================
     Compresses the source buffer into the destination buffer. The level
   parameter has the same meaning as in deflateInit.  sourceLen is the byte
   length of the source buffer. Upon entry, destLen is the total size of the
   destination buffer, which must be at least 0.1% larger than sourceLen plus
   12 bytes. Upon exit, destLen is the actual size of the compressed buffer.

     compress2 returns Z_OK if success, Z_MEM_ERROR if there was not enough
   memory, Z_BUF_ERROR if there was not enough room in the output buffer,
   Z_STREAM_ERROR if the level parameter is invalid.
*/
/* dp: Cribbed from zlib */
int compress2 (char *dest, unsigned long *destLen, const char *source, unsigned long sourceLen, int level)
{
    z_stream stream;
    int err;

    err = init_stream_workspace(&stream);
    if (err) {
        ftfs_error(__func__, "zlib stream init failed!");
        BUG();
    }

    stream.next_in = (const char *)source;
    stream.avail_in = (unsigned int)sourceLen;
#ifdef MAXSEG_64K
    /* Check for source > 64K on 16-bit machine: */
    if ((unsigned long)stream.avail_in != sourceLen) return Z_BUF_ERROR;
#endif
    stream.next_out = dest;
    stream.avail_out = (unsigned int)*destLen;
    if ((unsigned long)stream.avail_out != *destLen) return Z_BUF_ERROR;

    err = zlib_deflateInit(&stream, level);
    if (err != Z_OK) return err;

    err = zlib_deflate(&stream, Z_FINISH);
    if (err != Z_STREAM_END) {
        zlib_deflateEnd(&stream);
        return err == Z_OK ? Z_BUF_ERROR : err;
    }
    *destLen = stream.total_out;

    err = zlib_deflateEnd(&stream);

    destroy_stream_workspace(&stream);
    return err;
}

/* ===========================================================================
     Decompresses the source buffer into the destination buffer.  sourceLen is
   the byte length of the source buffer. Upon entry, destLen is the total
   size of the destination buffer, which must be large enough to hold the
   entire uncompressed data. (The size of the uncompressed data must have
   been saved previously by the compressor and transmitted to the decompressor
   by some mechanism outside the scope of this compression library.)
   Upon exit, destLen is the actual size of the compressed buffer.

     uncompress returns Z_OK if success, Z_MEM_ERROR if there was not
   enough memory, Z_BUF_ERROR if there was not enough room in the output
   buffer, or Z_DATA_ERROR if the input data was corrupted.
*/
int uncompress (char *dest, unsigned long *destLen, const char *source, unsigned long sourceLen)
{
    z_stream stream;
    int err;

    err = init_stream_workspace(&stream);
    if (err) {
        ftfs_error(__func__, "zlib stream init failed!");
        BUG();
    }

    stream.next_in = (const char *)source;
    stream.avail_in = (unsigned int)sourceLen;
    /* Check for source > 64K on 16-bit machine: */

    if ((unsigned long)stream.avail_in != sourceLen) return Z_BUF_ERROR;

    stream.next_out = dest;
    stream.avail_out = (unsigned int)*destLen;
    if ((unsigned long)stream.avail_out != *destLen) return Z_BUF_ERROR;

    err = zlib_inflateInit(&stream);
    if (err != Z_OK) return err;

    err = zlib_inflate(&stream, Z_FINISH);
    if (err != Z_STREAM_END) {
        zlib_inflateEnd(&stream);
        if (err == Z_NEED_DICT || (err == Z_BUF_ERROR && stream.avail_in == 0))
            return Z_DATA_ERROR;
        return err;
    }
    *destLen = stream.total_out;

    err = zlib_inflateEnd(&stream);

    destroy_stream_workspace(&stream);
    return err;
}


unsigned long compressBound (unsigned long sourceLen) {
    return sourceLen + (sourceLen >> 12) + (sourceLen >> 14) +
           (sourceLen >> 25) + 13;
}
