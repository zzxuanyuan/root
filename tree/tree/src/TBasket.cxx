// @(#)root/tree:$Id: 4e77188fbf1e7fd026a984989de66663c49b12fc $
// Author: Rene Brun   19/01/96
/*************************************************************************
 * Copyright (C) 1995-2000, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#include "TBasket.h"
#include "TBuffer.h"
#include "TBufferFile.h"
#include "TTree.h"
#include "TBranch.h"
#include "TFile.h"
#include "TBufferFile.h"
#include "TMath.h"
#include "TROOT.h"
#include "TTreeCache.h"
#include "TVirtualMutex.h"
#include "TVirtualPerfStats.h"
#include "TTimeStamp.h"
#include "RZip.h"

// TODO: Copied from TBranch.cxx
#if (__GNUC__ >= 3) || defined(__INTEL_COMPILER)
#if !defined(R__unlikely)
  #define R__unlikely(expr) __builtin_expect(!!(expr), 0)
#endif
#if !defined(R__likely)
  #define R__likely(expr) __builtin_expect(!!(expr), 1)
#endif
#else
  #define R__unlikely(expr) expr
  #define R__likely(expr) expr
#endif

const UInt_t kDisplacementMask = 0xFF000000;  // In the streamer the two highest bytes of
                                              // the fEntryOffset are used to stored displacement.

ClassImp(TBasket)

/** \class TBasket
Manages buffers for branches of a Tree.

See picture in TTree.
*/

////////////////////////////////////////////////////////////////////////////////
/// Default contructor.

TBasket::TBasket() : fCompressedBufferRef(0), fOwnsCompressedBuffer(kFALSE), fLastWriteBufferSize(0)
{
   fDisplacement  = 0;
   fEntryOffset   = 0;
   fCompressedEntryOffset = 0;
   fBufferRef     = 0;
   fBuffer        = 0;
   fHeaderOnly    = kFALSE;
   fBufferSize    = 0;
   fNevBufSize    = 0;
   fNevBuf        = 0;
   fLast          = 0;
   fBranch        = 0;
   fRandomAccessCompression = gROOT->IsRandomAccessCompression();
   printf("TBasket(1):random=%s\n",fRandomAccessCompression?"True":"False");
}

////////////////////////////////////////////////////////////////////////////////
/// Constructor used during reading.

TBasket::TBasket(TDirectory *motherDir) : TKey(motherDir),fCompressedBufferRef(0), fOwnsCompressedBuffer(kFALSE), fLastWriteBufferSize(0)
{
   fDisplacement  = 0;
   fEntryOffset   = 0;
   fCompressedEntryOffset = 0;
   fBufferRef     = 0;
   fBuffer        = 0;
   fHeaderOnly    = kFALSE;
   fBufferSize    = 0;
   fNevBufSize    = 0;
   fNevBuf        = 0;
   fLast          = 0;
   fBranch        = 0;
   fRandomAccessCompression = gROOT->IsRandomAccessCompression();
   printf("TBasket(2):random=%s\n",fRandomAccessCompression?"True":"False");
}

////////////////////////////////////////////////////////////////////////////////
/// Basket normal constructor, used during writing.

TBasket::TBasket(const char *name, const char *title, TBranch *branch) :
   TKey(branch->GetDirectory()),fCompressedBufferRef(0), fOwnsCompressedBuffer(kFALSE), fLastWriteBufferSize(0)
{
   SetName(name);
   SetTitle(title);
   fClassName   = "TBasket";
   fBufferSize  = branch->GetBasketSize();
   fNevBufSize  = branch->GetEntryOffsetLen();
   fNevBuf      = 0;
   fEntryOffset = 0;
   fCompressedEntryOffset = 0;
   fDisplacement= 0;
   fBuffer      = 0;
   fBufferRef   = new TBufferFile(TBuffer::kWrite, fBufferSize);
   fVersion    += 1000;
   if (branch->GetDirectory()) {
      TFile *file = branch->GetFile();
      fBufferRef->SetParent(file);
   }
   fHeaderOnly  = kTRUE;
   fLast        = 0; // Must initialize before calling Streamer()
   if (branch->GetTree()) {
#ifdef R__USE_IMT
      fCompressedBufferRef = branch->GetTransientBuffer(fBufferSize);
#else
      fCompressedBufferRef = branch->GetTree()->GetTransientBuffer(fBufferSize);
#endif
      fOwnsCompressedBuffer = kFALSE;
      if (!fCompressedBufferRef) {
         fCompressedBufferRef = new TBufferFile(TBuffer::kRead, fBufferSize);
         fOwnsCompressedBuffer = kTRUE;
      }
   }
   printf("before streamer\n");
   Streamer(*fBufferRef);
   printf("end streamer\n");
   fKeylen      = fBufferRef->Length();
   fObjlen      = fBufferSize - fKeylen;
   fLast        = fKeylen;
   fBuffer      = 0;
   fBranch      = branch;
   fHeaderOnly  = kFALSE;
   if (fNevBufSize) {
      fEntryOffset = new Int_t[fNevBufSize];
      for (Int_t i=0;i<fNevBufSize;i++) fEntryOffset[i] = 0;
   }
   branch->GetTree()->IncrementTotalBuffers(fBufferSize);
   fRandomAccessCompression = gROOT->IsRandomAccessCompression();
   printf("TBasket(3):random=%s\n",fRandomAccessCompression?"True":"False");

   if (fRandomAccessCompression!=0 && fNevBufSize!=0) {
      printf("Allocating fCompressedEntryOffset\n");
      fCompressedEntryOffset = new Int_t[fNevBufSize];
      for (Int_t i=0;i<fNevBufSize;i++) fCompressedEntryOffset[i] = 0;
   }
   branch->GetTree()->IncrementTotalBuffers(fBufferSize);

}

////////////////////////////////////////////////////////////////////////////////
/// Basket destructor.

TBasket::~TBasket()
{
   if (fDisplacement) delete [] fDisplacement;
   if (fEntryOffset)  delete [] fEntryOffset;
   if (fCompressedEntryOffset) delete [] fCompressedEntryOffset;
   if (fBufferRef) delete fBufferRef;
   fBufferRef = 0;
   fBuffer = 0;
   fDisplacement= 0;
   fEntryOffset = 0;
   fCompressedEntryOffset = 0;
   // Note we only delete the compressed buffer if we own it
   if (fCompressedBufferRef && fOwnsCompressedBuffer) {
      delete fCompressedBufferRef;
      fCompressedBufferRef = 0;
   }
}

////////////////////////////////////////////////////////////////////////////////
/// Increase the size of the current fBuffer up to newsize.

void TBasket::AdjustSize(Int_t newsize)
{
   if (fBuffer == fBufferRef->Buffer()) {
      fBufferRef->Expand(newsize);
      fBuffer = fBufferRef->Buffer();
   } else {
      fBufferRef->Expand(newsize);
   }
   fBranch->GetTree()->IncrementTotalBuffers(newsize-fBufferSize);
   fBufferSize  = newsize;
}

////////////////////////////////////////////////////////////////////////////////
/// Copy the basket of this branch onto the file to.

Long64_t TBasket::CopyTo(TFile *to)
{
   fBufferRef->SetWriteMode();
   Int_t nout = fNbytes - fKeylen;
   fBuffer = fBufferRef->Buffer();
   Create(nout, to);
   fBufferRef->SetBufferOffset(0);
   fHeaderOnly = kTRUE;
   Streamer(*fBufferRef);
   fHeaderOnly = kFALSE;
   Int_t nBytes = WriteFileKeepBuffer(to);

   return nBytes>0 ? nBytes : -1;
}

////////////////////////////////////////////////////////////////////////////////
///  Delete fEntryOffset array.

void TBasket::DeleteEntryOffset()
{
   if (fEntryOffset) delete [] fEntryOffset;
   fEntryOffset = 0;
   fNevBufSize  = 0;
}

////////////////////////////////////////////////////////////////////////////////
/// Drop buffers of this basket if it is not the current basket.

Int_t TBasket::DropBuffers()
{
   if (!fBuffer && !fBufferRef) return 0;

   if (fDisplacement) delete [] fDisplacement;
   if (fEntryOffset)  delete [] fEntryOffset;
   if (fCompressedEntryOffset) delete [] fCompressedEntryOffset;
   if (fBufferRef)    delete fBufferRef;
   if (fCompressedBufferRef && fOwnsCompressedBuffer) delete fCompressedBufferRef;
   fBufferRef   = 0;
   fCompressedBufferRef = 0;
   fBuffer      = 0;
   fDisplacement= 0;
   fEntryOffset = 0;
   fCompressedEntryOffset = 0;
   fBranch->GetTree()->IncrementTotalBuffers(-fBufferSize);
   return fBufferSize;
}

////////////////////////////////////////////////////////////////////////////////
/// Get pointer to buffer for internal entry.

Int_t TBasket::GetEntryPointer(Int_t entry)
{
   Int_t offset;
   if (fEntryOffset) offset = fEntryOffset[entry];
   else              offset = fKeylen + entry*fNevBufSize;
   fBufferRef->SetBufferOffset(offset);
   return offset;
}

////////////////////////////////////////////////////////////////////////////////
/// Get pointer to compressed buffer for internal entry.

Int_t TBasket::GetCompressedEntryPointer(Int_t entry)
{
   Int_t offset;
   if (fCompressedEntryOffset) {
      offset = fCompressedEntryOffset[entry];
      fCompressedBufferRef->SetBufferOffset(offset);
      return offset;
   } else {
      printf("There is no compressed entry offset.\n");
      return 0;
   }
}

////////////////////////////////////////////////////////////////////////////////
/// Load basket buffers in memory without unziping.
/// This function is called by TTreeCloner.
/// The function returns 0 in case of success, 1 in case of error.

Int_t TBasket::LoadBasketBuffers(Long64_t pos, Int_t len, TFile *file, TTree *tree)
{
   if (fBufferRef) {
      // Reuse the buffer if it exist.
      fBufferRef->Reset();

      // We use this buffer both for reading and writing, we need to
      // make sure it is properly sized for writing.
      fBufferRef->SetWriteMode();
      if (fBufferRef->BufferSize() < len) {
         fBufferRef->Expand(len);
      }
      fBufferRef->SetReadMode();
   } else {
      fBufferRef = new TBufferFile(TBuffer::kRead, len);
   }
   fBufferRef->SetParent(file);
   char *buffer = fBufferRef->Buffer();
   file->Seek(pos);
   TFileCacheRead *pf = file->GetCacheRead(tree);
   if (pf) {
      TVirtualPerfStats* temp = gPerfStats;
      if (tree->GetPerfStats()) gPerfStats = tree->GetPerfStats();
      Int_t st = pf->ReadBuffer(buffer,pos,len);
      if (st < 0) {
         return 1;
      } else if (st == 0) {
         // fOffset might have been changed via TFileCacheRead::ReadBuffer(), reset it
         file->Seek(pos);
         // If we are using a TTreeCache, disable reading from the default cache
         // temporarily, to force reading directly from file
         TTreeCache *fc = dynamic_cast<TTreeCache*>(file->GetCacheRead());
         if (fc) fc->Disable();
         Int_t ret = file->ReadBuffer(buffer,len);
         if (fc) fc->Enable();
         pf->AddNoCacheBytesRead(len);
         pf->AddNoCacheReadCalls(1);
         if (ret) {
            return 1;
         }
      }
      gPerfStats = temp;
      // fOffset might have been changed via TFileCacheRead::ReadBuffer(), reset it
      file->SetOffset(pos + len);
   } else {
      TVirtualPerfStats* temp = gPerfStats;
      if (tree->GetPerfStats() != 0) gPerfStats = tree->GetPerfStats();
      if (file->ReadBuffer(buffer,len)) {
         gPerfStats = temp;
         return 1; //error while reading
      }
      else gPerfStats = temp;
   }

   fBufferRef->SetReadMode();
   fBufferRef->SetBufferOffset(0);
   Streamer(*fBufferRef);

   return 0;
}

////////////////////////////////////////////////////////////////////////////////
/// Remove the first dentries of this basket, moving entries at
/// dentries to the start of the buffer.

void TBasket::MoveEntries(Int_t dentries)
{
   Int_t i;

   if (dentries >= fNevBuf) return;
   Int_t bufbegin;
   Int_t moved;

   if (fEntryOffset) {
      bufbegin = fEntryOffset[dentries];
      moved = bufbegin-GetKeylen();

      // First store the original location in the fDisplacement array
      // and record the new start offset

      if (!fDisplacement) {
         fDisplacement = new Int_t[fNevBufSize];
      }
      for (i = 0; i<(fNevBufSize-dentries); ++i) {
         fDisplacement[i] = fEntryOffset[i+dentries];
         fEntryOffset[i]  = fEntryOffset[i+dentries] - moved;
      }
      for (i = fNevBufSize-dentries; i<fNevBufSize; ++i) {
         fDisplacement[i] = 0;
         fEntryOffset[i]  = 0;
      }

   } else {
      // If there is no EntryOffset array, this means
      // that each entry has the same size and that
      // it does not point to other objects (hence there
      // is no need for a displacement array).
      bufbegin = GetKeylen() + dentries*fNevBufSize;
      moved = bufbegin-GetKeylen();
   }
   TBuffer *buf = GetBufferRef();
   char *buffer = buf->Buffer();
   memmove(buffer+GetKeylen(),buffer+bufbegin,buf->Length()-bufbegin);
   buf->SetBufferOffset(buf->Length()-moved);
   fNevBuf -= dentries;
}

#define OLD_CASE_EXPRESSION fObjlen==fNbytes-fKeylen && GetBranch()->GetCompressionLevel()!=0 && file->GetVersion()<=30401
////////////////////////////////////////////////////////////////////////////////
/// By-passing buffer unzipping has been requested and is
/// possible (only 1 entry in this basket).

Int_t TBasket::ReadBasketBuffersUncompressedCase()
{
   fBuffer = fBufferRef->Buffer();

   // Make sure that the buffer is set at the END of the data
   fBufferRef->SetBufferOffset(fNbytes);

   // Indicate that this buffer is weird.
   fBufferRef->SetBit(TBufferFile::kNotDecompressed);

   // Usage of this mode assume the existance of only ONE
   // entry in this basket.
   delete [] fEntryOffset; fEntryOffset = 0;
   delete [] fDisplacement; fDisplacement = 0;

   fBranch->GetTree()->IncrementTotalBuffers(fBufferSize);
   return 0;
}

////////////////////////////////////////////////////////////////////////////////
/// We always create the TBuffer for the basket but it hold the buffer from the cache.

Int_t TBasket::ReadBasketBuffersUnzip(char* buffer, Int_t size, Bool_t mustFree, TFile* file)
{
   if (fBufferRef) {
      fBufferRef->SetBuffer(buffer, size, mustFree);
      fBufferRef->SetReadMode();
      fBufferRef->Reset();
   } else {
      fBufferRef = new TBufferFile(TBuffer::kRead, size, buffer, mustFree);
   }
   fBufferRef->SetParent(file);

   Streamer(*fBufferRef);

   if (IsZombie()) {
      return -1;
   }

   Bool_t oldCase = OLD_CASE_EXPRESSION;

   if ((fObjlen > fNbytes-fKeylen || oldCase) && TestBit(TBufferFile::kNotDecompressed) && (fNevBuf==1)) {
      return TBasket::ReadBasketBuffersUncompressedCase();
   }

   fBuffer = fBufferRef->Buffer();
   return fObjlen+fKeylen;
}

////////////////////////////////////////////////////////////////////////////////
/// Initialize a buffer for reading if it is not already initialized

static inline TBuffer* R__InitializeReadBasketBuffer(TBuffer* bufferRef, Int_t len, TFile* file)
{
   TBuffer* result;
   if (R__likely(bufferRef)) {
      bufferRef->SetReadMode();
      Int_t curBufferSize = bufferRef->BufferSize();
      if (curBufferSize < len) {
         // Experience shows that giving 5% "wiggle-room" decreases churn.
         bufferRef->Expand(Int_t(len*1.05));
      }
      bufferRef->Reset();
      result = bufferRef;
   } else {
      result = new TBufferFile(TBuffer::kRead, len);
   }
   result->SetParent(file);
   return result;
}

////////////////////////////////////////////////////////////////////////////////
/// Initialize the compressed buffer; either from the TTree or create a local one.

void inline TBasket::InitializeCompressedBuffer(Int_t len, TFile* file)
{
   Bool_t compressedBufferExists = fCompressedBufferRef != NULL;
   fCompressedBufferRef = R__InitializeReadBasketBuffer(fCompressedBufferRef, len, file);
   if (R__unlikely(!compressedBufferExists)) {
      fOwnsCompressedBuffer = kTRUE;
   }
}

////////////////////////////////////////////////////////////////////////////////
/// Read basket buffers in memory and cleanup.
///
/// Read a basket buffer. Check if buffers of previous ReadBasket
/// should not be dropped. Remember, we keep buffers in memory up to
/// fMaxVirtualSize.
/// The function returns 0 in case of success, 1 in case of error
/// This function was modified with the addition of the parallel
/// unzipping, it will try to get the unzipped file from the cache
/// receiving only a pointer to that buffer (so we shall not
/// delete that pointer), although we get a new buffer in case
/// it's not found in the cache.
/// There is a lot of code duplication but it was necesary to assure
/// the expected behavior when there is no cache.

Int_t TBasket::ReadBasketBuffers(Long64_t pos, Int_t len, TFile *file, Bool_t random, Long64_t relativeentry)
{
   if(!fBranch->GetDirectory()) {
      return -1;
   }

   Bool_t oldCase;
   char *rawUncompressedBuffer, *rawCompressedBuffer;
   Int_t uncompressedBufferLen;

   // See if the cache has already unzipped the buffer for us.
   TFileCacheRead *pf = nullptr;
   {
      R__LOCKGUARD_IMT2(gROOTMutex); // Lock for parallel TTree I/O
      pf = file->GetCacheRead(fBranch->GetTree());
   }
   if (pf) {
      Int_t res = -1;
      Bool_t free = kTRUE;
      char *buffer;
      res = pf->GetUnzipBuffer(&buffer, pos, len, &free);
      if (R__unlikely(res >= 0)) {
         len = ReadBasketBuffersUnzip(buffer, res, free, file);
         // Note that in the kNotDecompressed case, the above function will return 0;
         // In such a case, we should stop processing
         if (len <= 0) return -len;
         goto AfterBuffer;
      }
   }

   // Determine which buffer to use, so that we can avoid a memcpy in case of
   // the basket was not compressed.
   TBuffer* readBufferRef;
   if (R__unlikely(fBranch->GetCompressionLevel()==0)) {
      readBufferRef = fBufferRef;
   } else {
      readBufferRef = fCompressedBufferRef;
   }

   // fBufferSize is likely to be change in the Streamer call (below)
   // and we will re-add the new size later on.
   fBranch->GetTree()->IncrementTotalBuffers(-fBufferSize);

   // Initialize the buffer to hold the compressed data.
   readBufferRef = R__InitializeReadBasketBuffer(readBufferRef, len, file);
   if (!readBufferRef) {
      Error("ReadBasketBuffers", "Unable to allocate buffer.");
      return 1;
   }

   if (pf) {
      TVirtualPerfStats* temp = gPerfStats;
      if (fBranch->GetTree()->GetPerfStats() != 0) gPerfStats = fBranch->GetTree()->GetPerfStats();
      Int_t st = 0;
      {
         R__LOCKGUARD_IMT2(gROOTMutex); // Lock for parallel TTree I/O
         st = pf->ReadBuffer(readBufferRef->Buffer(),pos,len);
      }
      if (st < 0) {
         return 1;
      } else if (st == 0) {
         // Read directly from file, not from the cache
         // If we are using a TTreeCache, disable reading from the default cache
         // temporarily, to force reading directly from file
         R__LOCKGUARD_IMT2(gROOTMutex);  // Lock for parallel TTree I/O
         TTreeCache *fc = dynamic_cast<TTreeCache*>(file->GetCacheRead());
         if (fc) fc->Disable();
         Int_t ret = file->ReadBuffer(readBufferRef->Buffer(),pos,len);
         if (fc) fc->Enable();
         pf->AddNoCacheBytesRead(len);
         pf->AddNoCacheReadCalls(1);
         if (ret) {
            return 1;
         }
      }
      gPerfStats = temp;
   } else {
      // Read from the file and unstream the header information.
      TVirtualPerfStats* temp = gPerfStats;
      if (fBranch->GetTree()->GetPerfStats() != 0) gPerfStats = fBranch->GetTree()->GetPerfStats();
      R__LOCKGUARD_IMT2(gROOTMutex);  // Lock for parallel TTree I/O
      if (file->ReadBuffer(readBufferRef->Buffer(),pos,len)) {
         gPerfStats = temp;
         return 1;
      }
      else gPerfStats = temp;
   }
   Streamer(*readBufferRef);
   if (IsZombie()) {
      return 1;
   }

   rawCompressedBuffer = readBufferRef->Buffer();

   // Are we done?
   if (R__unlikely(readBufferRef == fBufferRef)) // We expect most basket to be compressed.
   {
      if (R__likely(fObjlen+fKeylen == fNbytes)) {
         // The basket was really not compressed as expected.
         goto AfterBuffer;
      } else {
         // Well, somehow the buffer was compressed anyway, we have the compressed data in the uncompressed buffer
         // Make sure the compressed buffer is initialized, and memcpy.
         InitializeCompressedBuffer(len, file);
         if (!fCompressedBufferRef) {
            Error("ReadBasketBuffers", "Unable to allocate buffer.");
            return 1;
         }
         fBufferRef->Reset();
         rawCompressedBuffer = fCompressedBufferRef->Buffer();
         memcpy(rawCompressedBuffer, fBufferRef->Buffer(), len);
      }
   }

   // Initialize buffer to hold the uncompressed data
   // Note that in previous versions we didn't allocate buffers until we verified
   // the zip headers; this is no longer beforehand as the buffer lifetime is scoped
   // to the TBranch.
   uncompressedBufferLen = len > fObjlen+fKeylen ? len : fObjlen+fKeylen;
   fBufferRef = R__InitializeReadBasketBuffer(fBufferRef, uncompressedBufferLen, file);
   rawUncompressedBuffer = fBufferRef->Buffer();
   fBuffer = rawUncompressedBuffer;

   oldCase = OLD_CASE_EXPRESSION;
   // Case where ROOT thinks the buffer is compressed.  Copy over the key and uncompress the object
   if (fObjlen > fNbytes-fKeylen || oldCase) {
      if (R__unlikely(TestBit(TBufferFile::kNotDecompressed) && (fNevBuf==1))) {
         return ReadBasketBuffersUncompressedCase();
      }

      // Optional monitor for zip time profiling.
      Double_t start = 0;
      if (R__unlikely(gPerfStats)) {
         start = TTimeStamp();
      }

      memcpy(rawUncompressedBuffer, rawCompressedBuffer, fKeylen);
      char *rawUncompressedObjectBuffer = rawUncompressedBuffer+fKeylen;
      UChar_t *rawCompressedObjectBuffer;

      // the following two parameters help us to find the boundary of this entry
      Int_t relativeoffset = 0; // start offset of this entry
      Int_t relativelength = 0; // length of this entry
      if (random & fRandomAccessCompression) {
         rawCompressedObjectBuffer = (UChar_t*)rawCompressedBuffer+fKeylen+sizeof(Int_t)*(fNevBuf+1+1);
         relativeoffset = fCompressedEntryOffset[relativeentry];
         relativelength = fCompressedEntryOffset[relativeentry+1] - relativeoffset;
      } else {
         rawCompressedObjectBuffer = (UChar_t*)rawCompressedBuffer+fKeylen;
         for(int cnt=0;cnt<1024;++cnt){
            printf("rawCompressedBuffer[%d]=%c(%d), ",cnt,rawCompressedBuffer[cnt],rawCompressedBuffer[cnt]);
            if(cnt%6==0) printf("\n");
         }
      }
      Int_t nin, nbuf;
      Int_t nout = 0, noutot = 0, nintot = 0;

      // Unzip all the compressed objects in the compressed object buffer.
      while (1) {
         // Check the header for errors.
         if (R__unlikely(R__unzip_header(&nin, rawCompressedObjectBuffer, &nbuf) != 0)) {
            Error("ReadBasketBuffers", "Inconsistency found in header (nin=%d, nbuf=%d)", nin, nbuf);
            break;
         }
         if (R__unlikely(oldCase && (nin > fObjlen || nbuf > fObjlen))) {
            //buffer was very likely not compressed in an old version
            memcpy(rawUncompressedBuffer+fKeylen, rawCompressedObjectBuffer+fKeylen, fObjlen);
            goto AfterBuffer;
         }
         printf("nin=%d,nbuf=%d\n",nin,nbuf);
         if (!random) {
            printf("run R__unzip\n");
            R__unzip(&nin, rawCompressedObjectBuffer, &nbuf, (unsigned char*) rawUncompressedObjectBuffer, &nout);
         } else {
            R__unzip_RAC(&nin, rawCompressedObjectBuffer, &nbuf, (unsigned char*) rawUncompressedObjectBuffer, &nout, relativeoffset, relativelength);
         }
         printf("nout=%d\n",nout);
         if (!nout) break;
         noutot += nout;
         nintot += nin;
         if (random & fRandomAccessCompression) { // if random access compression, do not wait until noutot >= fObjlen since single entry is high likely smaller than fObjlen
            fObjlen     = nout;
            fBufferSize = fKeylen + nout;
            fLast       = fBufferSize;
            fNevBuf     = 0;
            fNevBufSize = 0;
            
            if (fEntryOffset) {
               delete [] fEntryOffset;
               fEntryOffset = 0;
            }
            if (fDisplacement) {
               delete [] fDisplacement;
               fDisplacement = 0;
            }
            fBranch->GetTree()->IncrementTotalBuffers(fBufferSize);
            len = fObjlen+fKeylen;
            TVirtualPerfStats* temp = gPerfStats;
            if (fBranch->GetTree()->GetPerfStats() != 0) gPerfStats = fBranch->GetTree()->GetPerfStats();
            if (R__unlikely(gPerfStats)) {
               gPerfStats->UnzipEvent(fBranch->GetTree(),pos,start,nintot,fObjlen);
            }
            gPerfStats = temp;
            return 0;
         }
         if (noutot >= fObjlen) break;
         rawCompressedObjectBuffer += nin;
         rawUncompressedObjectBuffer += nout;
      }

      // Make sure the uncompressed numbers are consistent with header.
      if (R__unlikely(noutot != fObjlen)) {
         Error("ReadBasketBuffers", "fNbytes = %d, fKeylen = %d, fObjlen = %d, noutot = %d, nout=%d, nin=%d, nbuf=%d", fNbytes,fKeylen,fObjlen, noutot,nout,nin,nbuf);
         fBranch->GetTree()->IncrementTotalBuffers(fBufferSize);
         return 1;
      }
      len = fObjlen+fKeylen;
      TVirtualPerfStats* temp = gPerfStats;
      if (fBranch->GetTree()->GetPerfStats() != 0) gPerfStats = fBranch->GetTree()->GetPerfStats();
      if (R__unlikely(gPerfStats)) {
         gPerfStats->UnzipEvent(fBranch->GetTree(),pos,start,nintot,fObjlen);
      }
      gPerfStats = temp;
   } else {
      // Nothing is compressed - copy over wholesale.
      memcpy(rawUncompressedBuffer, rawCompressedBuffer, len);
   }
AfterBuffer:

   fBranch->GetTree()->IncrementTotalBuffers(fBufferSize);

   // Read offsets table if needed.
   if (!fBranch->GetEntryOffsetLen()) {
      return 0;
   }
   delete [] fEntryOffset;
   fEntryOffset = 0;
   fBufferRef->SetBufferOffset(fLast);
   fBufferRef->ReadArray(fEntryOffset);
   if (!fEntryOffset) {
      fEntryOffset = new Int_t[fNevBuf+1];
      fEntryOffset[0] = fKeylen;
      Warning("ReadBasketBuffers","basket:%s has fNevBuf=%d but fEntryOffset=0, pos=%lld, len=%d, fNbytes=%d, fObjlen=%d, trying to repair",GetName(),fNevBuf,pos,len,fNbytes,fObjlen);
      return 0;
   }
   // Read the array of diplacement if any.
   delete [] fDisplacement;
   fDisplacement = 0;
   if (fBufferRef->Length() != len) {
      // There is more data in the buffer!  It is the displacement
      // array.  If len is less than TBuffer::kMinimalSize the actual
      // size of the buffer is too large, so we can not use the
      // fBufferRef->BufferSize()
      fBufferRef->ReadArray(fDisplacement);
   }

   for(int i=0; i<fNevBuf; ++i){ //##
      printf("fEntryOffset[%d]=%d\n",i,fEntryOffset[i]);
   }

   return 0;
}

////////////////////////////////////////////////////////////////////////////////
/// Read basket buffers in memory and cleanup
///
/// Read first bytes of a logical record starting at position pos
/// return record length (first 4 bytes of record).

Int_t TBasket::ReadBasketBytes(Long64_t pos, TFile *file)
{
   const Int_t len = 128;
   char buffer[len];
   Int_t keylen;
   file->GetRecordHeader(buffer, pos,len, fNbytes, fObjlen, keylen);
   fKeylen = keylen;
   return fNbytes;
}

////////////////////////////////////////////////////////////////////////////////
/// Reset the basket to the starting state. i.e. as it was after calling
/// the constructor (and potentially attaching a TBuffer.)
/// Reduce memory used by fEntryOffset and the TBuffer if needed ..

void TBasket::Reset()
{
   // Name, Title, fClassName, fBranch
   // stay the same.

   // Downsize the buffer if needed.
   Int_t curSize = fBufferRef->BufferSize();
   // fBufferLen at this point is already reset, so use indirect measurements
   Int_t curLen = (GetObjlen() + GetKeylen());
   Long_t newSize = -1;
   if (curSize > 2*curLen)
   {
      Long_t curBsize = fBranch->GetBasketSize();
      if (curSize > 2*curBsize ) {
         Long_t avgSize = (Long_t)(fBranch->GetTotBytes() / (1+fBranch->GetWriteBasket())); // Average number of bytes per basket so far
         if (curSize > 2*avgSize) {
            newSize = curBsize;
            if (curLen > newSize) {
               newSize = curLen;
            }
            if (avgSize > newSize) {
               newSize = avgSize;
            }
            newSize = newSize + 512 - newSize%512;  // Wiggle room and alignment (512 is same as in OptimizeBaskets)
         }
      }
   }
   /*
      Philippe has asked us to keep this turned off until we finish memory fragmentation studies.
   // If fBufferRef grew since we last saw it, shrink it to 105% of the occupied size
   if (curSize > fLastWriteBufferSize) {
      if (newSize == -1) {
         newSize = Int_t(1.05*Float_t(fBufferRef->Length()));
      }
      fLastWriteBufferSize = newSize;
   }
   */
   if (newSize != -1) {
      fBufferRef->Expand(newSize,kFALSE);     // Expand without copying the existing data.
   }

   TKey::Reset();

   Int_t newNevBufSize = fBranch->GetEntryOffsetLen();
   if (newNevBufSize==0) {
      delete [] fEntryOffset;
      fEntryOffset = 0;
      delete [] fCompressedEntryOffset;
      fCompressedEntryOffset = 0;
   } else if (newNevBufSize != fNevBufSize) {
      delete [] fEntryOffset;
      fEntryOffset = new Int_t[newNevBufSize];
      delete [] fCompressedEntryOffset;
      fCompressedEntryOffset = new Int_t[newNevBufSize];
   } else if (!fEntryOffset || !fCompressedEntryOffset) {
      if (!fEntryOffset)
         fEntryOffset = new Int_t[newNevBufSize];
      if (!fCompressedEntryOffset)
         fCompressedEntryOffset = new Int_t[newNevBufSize];
   } 
   fNevBufSize = newNevBufSize;

   fNevBuf      = 0;
   Int_t *storeEntryOffset = fEntryOffset;
   fEntryOffset = 0;
   Int_t *storeCompressedEntryOffset = fCompressedEntryOffset;
   fCompressedEntryOffset = 0;
   Int_t *storeDisplacement = fDisplacement;
   fDisplacement= 0;
   fBuffer      = 0;

   fBufferRef->Reset();
   fBufferRef->SetWriteMode();

   fHeaderOnly  = kTRUE;
   fLast        = 0;  //Must initialize before calling Streamer()

   Streamer(*fBufferRef);

   fKeylen      = fBufferRef->Length();
   fObjlen      = fBufferSize - fKeylen;
   fLast        = fKeylen;
   fBuffer      = 0;
   fHeaderOnly  = kFALSE;
   fDisplacement= storeDisplacement;
   fCompressedEntryOffset = storeCompressedEntryOffset;
   fEntryOffset = storeEntryOffset;
   if (fNevBufSize) {
      for (Int_t i=0;i<fNevBufSize;i++) {
         fEntryOffset[i] = 0;
         fCompressedEntryOffset[i] = 0;
      }
   }
}

////////////////////////////////////////////////////////////////////////////////
/// Set read mode of basket.

void TBasket::SetReadMode()
{
   fLast = fBufferRef->Length();
   fBufferRef->SetReadMode();
}

////////////////////////////////////////////////////////////////////////////////
/// Set write mode of basket.

void TBasket::SetWriteMode()
{
   fBufferRef->SetWriteMode();
   fBufferRef->SetBufferOffset(fLast);
}

////////////////////////////////////////////////////////////////////////////////
/// Stream a class object.

void TBasket::Streamer(TBuffer &b)
{
   char flag;
   if (b.IsReading()) {
      TKey::Streamer(b); //this must be first
      Version_t v = b.ReadVersion();

      b >> fBufferSize;
      b >> fNevBufSize;
      if (fNevBufSize < 0) {
         Error("Streamer","The value of fNevBufSize is incorrect (%d) ; trying to recover by setting it to zero",fNevBufSize);
         MakeZombie();
         fNevBufSize = 0;
      }
      b >> fNevBuf;
      b >> fLast;
      b >> flag;
      printf("in streamer read, fBufferSize=%d,fNevBufSize=%d,fNevBuf=%d,fLast=%d,flag=%d\n",fBufferSize,fNevBufSize,fNevBuf,fLast,flag);
      if (fLast > fBufferSize) fBufferSize = fLast;
      if (!flag) {
         return;
      }
      if (flag%10 == 5) {
         delete [] fCompressedEntryOffset;
         fCompressedEntryOffset = new Int_t[fNevBufSize];
         if (fNevBuf) b.ReadArray(fCompressedEntryOffset);
         for(int i=0; i<fNevBuf+1; ++i) printf("in streamer read, fCompressedEntryOffset[%d]=%d\n",i,fCompressedEntryOffset[i]);
         return;
      }
      if (flag%10 != 2) {
         printf("flag is not 2, indicating there exists fEntryOffset, flag=%d\n",flag);
         delete [] fEntryOffset;
         fEntryOffset = new Int_t[fNevBufSize];
         if (fNevBuf) b.ReadArray(fEntryOffset);
         for(int i=0; i<fNevBuf+1; ++i) printf("in streamer read, fEntryOffset[%d]=%d\n",i,fEntryOffset[i]);
         if (20<flag && flag<40) {
            for(int i=0; i<fNevBuf; i++){
               fEntryOffset[i] &= ~kDisplacementMask;
            }
         }
         if (flag>40) {
            fDisplacement = new Int_t[fNevBufSize];
            b.ReadArray(fDisplacement);
         }
      }
      if (flag == 1 || flag > 10) {
         fBufferRef = new TBufferFile(TBuffer::kRead,fBufferSize);
         fBufferRef->SetParent(b.GetParent());
         char *buf  = fBufferRef->Buffer();
         if (v > 1) b.ReadFastArray(buf,fLast);
         else       b.ReadArray(buf);
         fBufferRef->SetBufferOffset(fLast);
         printf("in streamer read, fLast=%d,fBufferSize=%d\n",fLast,fBufferSize);
         // This is now done in the TBranch streamer since fBranch might not
         // yet be set correctly.
         //   fBranch->GetTree()->IncrementTotalBuffers(fBufferSize);
      }
   } else {
      TKey::Streamer(b);   //this must be first
      b.WriteVersion(TBasket::IsA());
      if (fBufferRef) {
         Int_t curLast = fBufferRef->Length();
         if (!fHeaderOnly && !fSeekKey && curLast > fLast) {printf("fLast=%d\n",fLast);fLast = curLast;}
         printf("curLast=%d\n",curLast);
      }
      if (fLast > fBufferSize) fBufferSize = fLast;
//   static TStopwatch timer;
//   timer.Start(kFALSE);

//       //  Check may be fEntryOffset is equidistant
//       //  This attempts by Victor fails :(
//       int equidist = 0;
//       if (1 && fEntryOffset && fNevBuf>=3) {
//          equidist = 1;
//          int dist = fEntryOffset[1]-fEntryOffset[0];
//          int curr = fEntryOffset[1];
//          for (int i=1;i<fNevBuf;i++,curr+=dist) {
//             if (fEntryOffset[i]==curr) continue;
//             equidist = 0;
//             break;
//          }
//          if (equidist) {
//             fNevBufSize=dist;
//             delete [] fEntryOffset; fEntryOffset = 0;
//          }
//           if (equidist) {
//              fprintf(stderr,"detected an equidistant case fNbytes==%d fLast==%d\n",fNbytes,fLast);
//           }
//       }
//  also he add (a little further
//       if (!fEntryOffset || equidist)  flag  = 2;

//   timer.Stop();
//   Double_t rt1 = timer.RealTime();
//   Double_t cp1 = timer.CpuTime();
//   fprintf(stderr,"equidist cost :  RT=%6.2f s  Cpu=%6.2f s\n",rt1,cp1);

      b << fBufferSize;
      b << fNevBufSize;
      b << fNevBuf;
      b << fLast;
      if (fHeaderOnly) {
         flag = 0;
         if (fRandomAccessCompression && fCompressedEntryOffset) {
            flag = 5;
            b << flag;
            if (fNevBuf) {
               b.WriteArray(fCompressedEntryOffset, fNevBuf+1);
               if(fEntryOffset) {
                  printf("fEntryOffset is not null\n");
                  for(int i=0; i<fNevBuf+1; ++i) printf("in streamer write, flag=%d,fBufferSize=%d,fNevBufSize=%d,fNevBuf=%d,fLast=%d,fEntryOffset[%d]=%d,fCompressedEntryOffset[%d]=%d\n",flag,fBufferSize,fNevBufSize,fNevBuf,fLast,i,fEntryOffset[i],i,fCompressedEntryOffset[i]);
               } else {
                  printf("fEntryOffset is null\n");
               }
            }
         } else {
            b << flag;
         }
      } else {
         flag = 1;
         if (!fEntryOffset)  flag  = 2;
         if (fBufferRef)     flag += 10;
         if (fDisplacement)  flag += 40;
         b << flag;

         if (fEntryOffset && fNevBuf) {
            b.WriteArray(fEntryOffset, fNevBuf);
            if (fDisplacement) b.WriteArray(fDisplacement, fNevBuf);
         }
         if (fBufferRef) {
            printf("in streamer write, write fBufferRef data\n");
            char *buf  = fBufferRef->Buffer();
            b.WriteFastArray(buf, fLast);
         }
      }
   }
}

////////////////////////////////////////////////////////////////////////////////
/// Update basket header and EntryOffset table.

void TBasket::Update(Int_t offset, Int_t skipped)
{
   if (fEntryOffset) {
      if (fNevBuf+1 >= fNevBufSize) {
         Int_t newsize = TMath::Max(10,2*fNevBufSize);
         Int_t *newoff = TStorage::ReAllocInt(fEntryOffset, newsize,
                                              fNevBufSize);
         Int_t *newcompressedoff = TStorage::ReAllocInt(fCompressedEntryOffset, newsize,
                                              fNevBufSize);
         if (fDisplacement) {
            Int_t *newdisp = TStorage::ReAllocInt(fDisplacement, newsize,
                                                  fNevBufSize);
            fDisplacement = newdisp;
         }
         fEntryOffset  = newoff;
         fCompressedEntryOffset = newcompressedoff;
         fNevBufSize   = newsize;

         //Update branch only for the first 10 baskets
         if (fBranch->GetWriteBasket() < 10) {
            fBranch->SetEntryOffsetLen(newsize);
         }
      }
      fEntryOffset[fNevBuf] = offset;

      if (skipped!=offset && !fDisplacement){
         fDisplacement = new Int_t[fNevBufSize];
         for (Int_t i = 0; i<fNevBufSize; i++) fDisplacement[i] = fEntryOffset[i];
      }
      if (fDisplacement) {
         fDisplacement[fNevBuf] = skipped;
         fBufferRef->SetBufferDisplacement(skipped);
      }
   }

   fNevBuf++;
}

////////////////////////////////////////////////////////////////////////////////
/// Write buffer of this basket on the current file.
///
/// The function returns the number of bytes committed to the memory.
/// If a write error occurs, the number of bytes returned is -1.
/// If no data are written, the number of bytes returned is 0.

Int_t TBasket::WriteBuffer()
{
   const Int_t kWrite = 1;

   TFile *file = fBranch->GetFile(kWrite);
   if (!file) return 0;
   if (!file->IsWritable()) {
      return -1;
   }
   fMotherDir = file; // fBranch->GetDirectory();

   if (R__unlikely(fBufferRef->TestBit(TBufferFile::kNotDecompressed))) {
      // Read the basket information that was saved inside the buffer.
      Bool_t writing = fBufferRef->IsWriting();
      fBufferRef->SetReadMode();
      fBufferRef->SetBufferOffset(0);

      Streamer(*fBufferRef);
      if (writing) fBufferRef->SetWriteMode();
      Int_t nout = fNbytes - fKeylen;

      fBuffer = fBufferRef->Buffer();

      Create(nout,file);
      fBufferRef->SetBufferOffset(0);
      fHeaderOnly = kTRUE;
      Streamer(*fBufferRef);         //write key itself again
      int nBytes = WriteFileKeepBuffer();
      fHeaderOnly = kFALSE;
      return nBytes>0 ? fKeylen+nout : -1;
   }

   // Transfer fEntryOffset table at the end of fBuffer.
   fLast = fBufferRef->Length();
   printf("before fEntryOffset table, fLast=%d,fNbytes=%d\n",fLast,fNbytes);
   if (fEntryOffset) {
      // Note: We might want to investigate the compression gain if we
      // transform the Offsets to fBuffer in entry length to optimize
      // compression algorithm.  The aggregate gain on a (random) CMS files
      // is around 5.5%. So the code could something like:
      //      for(Int_t z = fNevBuf; z > 0; --z) {
      //         if (fEntryOffset[z]) fEntryOffset[z] = fEntryOffset[z] - fEntryOffset[z-1];
      //      }
      fBufferRef->WriteArray(fEntryOffset,fNevBuf+1);
      for(int i=0; i< fNevBuf+1; ++i)
         printf("Name:%s, fEntryOffset[%d]=%d\n", this->GetBranch()->GetName(), i, fEntryOffset[i]);
      if (fDisplacement) {
         fBufferRef->WriteArray(fDisplacement,fNevBuf+1);
         for(int i=0; i< fNevBuf+1; ++i)
         delete [] fDisplacement; fDisplacement = 0;
      }
   }
   Int_t lbuf, nout, noutot, bufmax, nzip;
   lbuf       = fBufferRef->Length();
   printf("in in lbuf=%d,fNbytes=%d\n",lbuf,fNbytes);
   fObjlen    = lbuf - fKeylen;
   int compoffkey = 0; //Compressed entry offsets in TBasket key.
   fHeaderOnly = kTRUE;
   fCycle = fBranch->GetWriteBasket();
   Int_t cxlevel = fBranch->GetCompressionLevel();
   Int_t cxAlgorithm = fBranch->GetCompressionAlgorithm();
   if (cxlevel > 0) {
      Int_t nbuffers = 1 + (fObjlen - 1) / kMAXZIPBUF;
      Int_t buflen;
      if (!fRandomAccessCompression) {
         buflen = fKeylen + fObjlen + 9 * nbuffers + 28; //add 28 bytes in case object is placed in a deleted gap
      } else {
         if (fCompressedEntryOffset && fNevBuf) {
            compoffkey = sizeof(Int_t)*(fNevBuf+1+1);
         }
         buflen = fKeylen + compoffkey + 2 * fObjlen + 9 * nbuffers + 28; //reserve enough space for compressed entry offsets
      }
      InitializeCompressedBuffer(buflen, file);
      if (!fCompressedBufferRef) {
         Warning("WriteBuffer", "Unable to allocate the compressed buffer");
         return -1;
      }
      fCompressedBufferRef->SetWriteMode();
      fBuffer = fCompressedBufferRef->Buffer();
      char *objbuf = fBufferRef->Buffer() + fKeylen;
      printf("compressed:fKeylen=%d,fCompressedBufferRef->Length()=%d\n",fKeylen,fCompressedBufferRef->Length());
      printf("compoffkey=%d,fRandomAccessCompression=%s\n",compoffkey,(fRandomAccessCompression?"True":"False"));
      char *bufcur = &fBuffer[fKeylen+compoffkey];//reserve space for compressed buffer
      noutot = 0;
      nzip   = 0;
      for (Int_t i = 0; i < nbuffers; ++i) {
         if (i == nbuffers - 1) bufmax = fObjlen - nzip;
         else bufmax = kMAXZIPBUF;
         printf("kMAXZIPBUF=%d,bufmax=%d,nbuffers=%d,fObjlen=%d,nzip=%d\n",kMAXZIPBUF,bufmax,nbuffers,fObjlen,nzip);
         //compress the buffer
         if (!fRandomAccessCompression) {
            R__zipMultipleAlgorithm(cxlevel, &bufmax, objbuf, &bufmax, bufcur, &nout, cxAlgorithm);
         } else {
            Int_t entries = fNevBuf+1;
            Bool_t haveoffset    = (fEntryOffset ? kTRUE : kFALSE);
            Int_t lastbyte     = fLast;
            Int_t *entryoffset = fEntryOffset;
            Int_t *compressedentryoffset = fCompressedEntryOffset;
            Int_t compressedbufmax = 2 * bufmax;
            printf("bufmax=%d,fKeylen=%d,fObjlen=%d,lbuf=%d,fNbytes=%d\n",bufmax,fKeylen,fObjlen,lbuf,fNbytes);
            R__zipMultipleAlgorithm_RAC(cxlevel, &bufmax, objbuf, &compressedbufmax, bufcur, &nout, cxAlgorithm, entries, haveoffset, lastbyte, entryoffset, compressedentryoffset);
         }
         // test if buffer has really been compressed. In case of small buffers
         // when the buffer contains random data, it may happen that the compressed
         // buffer is larger than the input. In this case, we write the original uncompressed buffer
         if (nout == 0 || nout >= fObjlen) {
            fCompressedEntryOffset = 0;
            nout = fObjlen;
            // We used to delete fBuffer here, we no longer want to since
            // the buffer (held by fCompressedBufferRef) might be re-used later.
            fBuffer = fBufferRef->Buffer();
            Create(fObjlen,file);
            fBufferRef->SetBufferOffset(0);
            printf("streamer3\n");
            Streamer(*fBufferRef);         //write key itself again
            if ((nout+fKeylen)>buflen) {
               Warning("WriteBuffer","Possible memory corruption due to compression algorithm, wrote %d bytes past the end of a block of %d bytes. fNbytes=%d, fObjLen=%d, fKeylen=%d",
                  (nout+fKeylen-buflen),buflen,fNbytes,fObjlen,fKeylen);
            }
            goto WriteFile;
         }
         bufcur += nout;
         noutot += nout;
         objbuf += kMAXZIPBUF;
         nzip   += kMAXZIPBUF;
      }
      nout = noutot;
      printf("before streamer4, fNbytes=%d\n",fNbytes);
      fKeylen += compoffkey; // before creating TKey, we need to reserve enough space for compression entry offsets.
      Create(noutot,file);
      fKeylen -= compoffkey; // reduce fKeylen back to the original length (without compression entry offsets).
      fBufferRef->SetBufferOffset(0);
      printf("streamer4,fNbytes=%d\n",fNbytes);
      Bool_t random = fRandomAccessCompression;
      if (random) {
         fRandomAccessCompression = 0;// make sure compressed entry offsets are not streamed into uncompressed buffer
         Streamer(*fBufferRef);         //write key itself again
         printf("between streamer\n");
         memcpy(fBuffer,fBufferRef->Buffer(),fKeylen);
         fRandomAccessCompression = 1;// streaming compressed entry offsets into compressed buffer
         printf("before fCompressedBufferRef, fLast=%d\n",fLast);
         fLast += compoffkey; // fLast is different between fBufferRef and fCompressedBufferRef. It needs to reserve compressed entry offsets for fCompressedBufferRef.
         Streamer(*fCompressedBufferRef);
      } else {
         Streamer(*fBufferRef);         //write key itself again
         memcpy(fBuffer,fBufferRef->Buffer(),fKeylen);
      }         
   } else {
      fBuffer = fBufferRef->Buffer();
      Create(fObjlen,file);
      fBufferRef->SetBufferOffset(0);
      printf("streamer5\n");
      Streamer(*fBufferRef);         //write key itself again
      nout = fObjlen;
   }

WriteFile:
   printf("WriteFile:fKeylen=%d,fObjlen=%d,fNbytes=%d,fLast=%d,fBufferRef->Length()=%d\n",fKeylen,fObjlen,fNbytes,fLast,fBufferRef->Length());
   Int_t nBytes = WriteFileKeepBuffer();
   printf("nBytes=%d\n",nBytes);
   fHeaderOnly = kFALSE;
   return nBytes>0 ? fKeylen+nout : -1;
}

