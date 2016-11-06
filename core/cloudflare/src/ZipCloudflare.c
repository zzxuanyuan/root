// @(#)root/lzma:$Id$
// Author: David Dagenhart   May 2011

/*************************************************************************
 * Copyright (C) 1995-2011, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#include "RConfigure.h"
#include "ZipCloudflare.h"
#include "zlib.h"
#include <stdio.h>
#include <assert.h>

static const int HDRSIZE = 9;

void R__zipCloudflare(int cxlevel, int *srcsize, char *src, int *tgtsize, char *tgt, int *irep)
{
   int err;
   int method   = Z_DEFLATED;
   z_stream stream;
   //Don't use the globals but want name similar to help see similarities in code
   unsigned l_in_size, l_out_size;
   *irep = 0;
    
   /* error_flag   = 0; */
   if (*tgtsize <= 0) {
      R__error("target buffer too small");
      return;
   }
   if (*srcsize > 0xffffff) {
      R__error("source buffer too big");
      return;
   }
 
   stream.next_in   = (Bytef*)src;
   stream.avail_in  = (uInt)(*srcsize);

   stream.next_out  = (Bytef*)(&tgt[HDRSIZE]);
   stream.avail_out = (uInt)(*tgtsize);

   stream.zalloc    = (alloc_func)0;
   stream.zfree     = (free_func)0;
   stream.opaque    = (voidpf)0;

   if (cxlevel > 9) cxlevel = 9;
   err = deflateInit(&stream, cxlevel);
   if (err != Z_OK) {
      printf("error %d in deflateInit (zlib)\n",err);
      return;
   }

   err = deflate(&stream, Z_FINISH);
   if (err != Z_STREAM_END) {
      deflateEnd(&stream);
      /* No need to print an error message. We simply abandon the compression
         the buffer cannot be compressed or compressed buffer would be larger than original buffer
         printf("error %d in deflate (zlib) is not = %d\n",err,Z_STREAM_END);
      */
      return;
   }

   err = deflateEnd(&stream);

   tgt[0] = 'Z';               /* Signature ZLib */
   tgt[1] = 'L';
   tgt[2] = (char) method;

   l_in_size   = (unsigned) (*srcsize);
   l_out_size  = stream.total_out;             /* compressed size */
   tgt[3] = (char)(l_out_size & 0xff);
   tgt[4] = (char)((l_out_size >> 8) & 0xff);
   tgt[5] = (char)((l_out_size >> 16) & 0xff);

   tgt[6] = (char)(l_in_size & 0xff);         /* decompressed size */
   tgt[7] = (char)((l_in_size >> 8) & 0xff);
   tgt[8] = (char)((l_in_size >> 16) & 0xff);

   *irep = stream.total_out + HDRSIZE;

   return;
}

void R__unzipCloudflare(int *srcsize, unsigned char *src, int *tgtsize, unsigned char *tgt, int *irep)
{
}


