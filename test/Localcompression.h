#ifndef ROOT_Localcompression
#define ROOT_Localcompression

//////////////////////////////////////////////////////////////////////////
//                                                                      //
// Local Compression                                                    //
//                                                                      //
// Description of the event and track parameters                        //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

#include "TObject.h"
#include "TClonesArray.h"
#include "TRefArray.h"
#include "TRef.h"
#include "TH1.h"
#include "TBits.h"
#include "TMath.h"

#define LARGESIZE 5000000
#define SMALLSIZE 100
#define FLOATSIZE 6

class TLarge : public TObject {

private:
   Int_t         fSize;
   Float_t      *fLarge; //[fSize]

public:
   TLarge(Int_t size = LARGESIZE); 
   TLarge(const TLarge& large);
   virtual ~TLarge();
   TLarge &operator=(const TLarge &large);

   void          Clear(Option_t *option ="");
   void          Build();
   Int_t         GetSize() const { return fSize; }
   Float_t      *GetLarge() const { return fLarge; }

   ClassDef(TLarge,1)
};

class TSmall : public TObject {

private:
   Int_t         fSize;
   Float_t      *fSmall; //[fSize]

public:
   TSmall(Int_t size = SMALLSIZE); 
   TSmall(const TSmall& small);
   virtual ~TSmall();
   TSmall &operator=(const TSmall &small);

   void          Clear(Option_t *option ="");
   void          Build();
   Int_t         GetSize() const { return fSize; }
   Float_t      *GetSmall() const { return fSmall; }

   ClassDef(TSmall,1)
};

class TFloat : public TObject {

private:
   Int_t         fSize;
   Float_t      *fFloat; //[fSize]

public:
   TFloat(Int_t size = FLOATSIZE); 
   TFloat(const TFloat& aint);
   virtual ~TFloat();
   TFloat &operator=(const TFloat &afloat);

   void          Clear(Option_t *option ="");
   void          Build();
   Int_t         GetSize() const { return fSize; }
   Float_t      *GetFloat() const { return fFloat; }

   ClassDef(TFloat,1)
};

class TDummy : public TObject {

private:
   TLarge       *fl;
   TSmall       *fs;
   TFloat       *ff;

public:
   TDummy(); 
   TDummy(const TDummy& dummy);
   virtual ~TDummy();
   TDummy &operator=(const TDummy &dummy);

   void          Clear(Option_t *option ="");
   void          Build();
   TLarge       *GetLarge() const { return fl; }
   TSmall       *GetSmall() const { return fs; }
   TFloat       *GetFloat() const { return ff; }

   ClassDef(TDummy,1)
};


#endif
