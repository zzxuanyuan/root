// @(#)root/test:$Id$
// Author: Rene Brun   19/08/96

#include "RVersion.h"
#include "TRandom.h"
#include "TDirectory.h"
#include "TProcessID.h"

#include "Localcompression.h"


ClassImp(TLarge)
ClassImp(TSmall)
ClassImp(TInt)

////////////////////////////////////////////////////////////////////////////////
/// Create an TLarge.
TLarge::TLarge(Int_t size)
{
   fSize = size;
   fLarge = new Float_t[fSize];
   for(int i=0;i<fSize;++i) {
      if (i%60==0) fLarge[i] = gRandom->Rndm(1);
      else fLarge[i] = fLarge[i-1];
   }
}

////////////////////////////////////////////////////////////////////////////////
/// Create an TLarge.
TLarge::TLarge(const TLarge& large) : TObject(large)
{
   Float_t *intermediate = large.GetLarge();
   Int_t size = large.GetSize();
   fLarge = new Float_t[size];
   for(int i=0;i<size;++i)
      fLarge[i] = intermediate[i];
}

////////////////////////////////////////////////////////////////////////////////

TLarge::~TLarge()
{
   Clear();
   delete fLarge;
   fSize = 0;
}

////////////////////////////////////////////////////////////////////////////////

void TLarge::Clear(Option_t * /*option*/)
{
   TObject::Clear();
   for(int i=0;i<fSize;++i)
      fLarge[i] = 0;
}

/////////////////////////////////////////////////////////////////////////////////

void TLarge::Build()
{
   for(int i=0;i<fSize;++i) {
      if (i%6==0) fLarge[i] = gRandom->Rndm(1);
      else fLarge[i] = fLarge[i-1];
   }
}

///////////////////////////////////////////////////////////////////////////////
/// Create an TSmall.
TSmall::TSmall(Int_t size)
{
   fSize  = size;
   fSmall = new Float_t[fSize];
   for(int i=0;i<fSize;++i) {
      if (i%6==0) fSmall[i] = gRandom->Rndm(1);
      fSmall[i] = fSmall[i-1];
   }
}

////////////////////////////////////////////////////////////////////////////////
/// Create an TSmall.
TSmall::TSmall(const TSmall& small) : TObject(small)
{
   Float_t *intermediate = small.GetSmall();
   Int_t size = small.GetSize();
   fSmall = new Float_t[size];
   for(int i=0;i<size;++i)
      fSmall[i] = intermediate[i];
}

////////////////////////////////////////////////////////////////////////////////

TSmall::~TSmall()
{
   Clear();
   delete fSmall;
   fSize = 0;
}

//////////////////////////////////////////////////////////////////////////////////

void TSmall::Build()
{
   for(int i=0;i<fSize;++i) {
      if (i%6==0) fSmall[i] = gRandom->Rndm(1);
      else fSmall[i] = fSmall[i-1];
   }
}

///////////////////////////////////////////////////////////////////////////////

void TSmall::Clear(Option_t * /*option*/)
{
   TObject::Clear();
   for(int i=0;i<fSize;++i)
      fSmall[i] = 0;
}

///////////////////////////////////////////////////////////////////////////////
/// Create an TFloat.
TInt::TInt(Int_t size)
{
   fSize  = size;
   fInt = new Int_t[fSize];
   fInt[0] = Int_t(gRandom->Rndm(1));
   for(int i=1;i<fSize;++i) {
      fInt[i] = fInt[0];
   }
}

////////////////////////////////////////////////////////////////////////////////
/// Create an TSmall.
TInt::TInt(const TInt& aint) : TObject(aint)
{
   Int_t *intermediate = aint.GetInt();
   Int_t size = aint.GetSize();
   fInt = new Int_t[size];
   for(int i=0;i<size;++i)
      fInt[i] = intermediate[i];
}

////////////////////////////////////////////////////////////////////////////////

TInt::~TInt()
{
   Clear();
   delete fInt;
   fSize = 0;
}

//////////////////////////////////////////////////////////////////////////////////

void TInt::Build()
{
   fInt[0] = Int_t(gRandom->Rndm(1));
   for(int i=1;i<fSize;++i) {
      fInt[i] = fInt[0];
   }
}

///////////////////////////////////////////////////////////////////////////////

void TInt::Clear(Option_t * /*option*/)
{
   TObject::Clear();
   for(int i=0;i<fSize;++i)
      fInt[i] = 0;
}

