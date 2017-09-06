// Author: Philippe Canal, 2017

/*************************************************************************
 * Copyright (C) 1995-2017, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_TRWMutexImp
#define ROOT_TRWMutexImp

#include "TVirtualRWMutex.h"
#include "ROOT/TSpinMutex.hxx"
#include "ROOT/TReentrantRWLock.hxx"

#include "TBuffer.h" // Needed by ClassDEfInlineOverride

template <typename MutexT, typename RecurseCountsT = ROOT::Internal::RecurseCounts>
class TRWMutexImp : public TVirtualRWMutex {
   ROOT::TReentrantRWLock<MutexT, RecurseCountsT> fMutexImp;

public:
   void ReadLock() override;
   void ReadUnLock() override;
   void WriteLock() override;
   void WriteUnLock() override;

   TVirtualRWMutex *Factory(Bool_t /*recursive*/ = kFALSE) override;
   std::unique_ptr<TVirtualMutex::State> Reset() override;
   void Restore(std::unique_ptr<TVirtualMutex::State> &&) override;

   ClassDefInlineOverride(TRWMutexImp,0)  // Concrete RW mutex lock class
};

#endif