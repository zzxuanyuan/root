// @(#)root/tree:$Id$
// Author: Rene Brun   14/01/2001

/*************************************************************************
 * Copyright (C) 1995-2000, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

/** \class TLeafElement
\ingroup tree

A TLeaf for the general case when using the branches created via
a TStreamerInfo (i.e. using TBranchElement).
*/

#include "TLeafElement.h"
//#include "TMethodCall.h"

#include "TVirtualStreamerInfo.h"
#include "Bytes.h"

ClassImp(TLeafElement);

////////////////////////////////////////////////////////////////////////////////
/// Default constructor for LeafObject.

TLeafElement::TLeafElement(): TLeaf()
{
   fAbsAddress = 0;
   fID   = -1;
   fType = -1;
}

////////////////////////////////////////////////////////////////////////////////
/// Create a LeafObject.

TLeafElement::TLeafElement(TBranch *parent, const char *name, Int_t id, Int_t type)
   : TLeaf(parent, name,name)
{
   fLenType    = 0;
   fAbsAddress = 0;
   fID         = id;
   fType       = type;
   if (type < TVirtualStreamerInfo::kObject) {
      Int_t bareType = type;
      if (bareType > TVirtualStreamerInfo::kOffsetP)
         bareType -= TVirtualStreamerInfo::kOffsetP;
      else if (bareType > TVirtualStreamerInfo::kOffsetL)
         bareType -= TVirtualStreamerInfo::kOffsetL;

      if ((bareType >= TVirtualStreamerInfo::kUChar && bareType <= TVirtualStreamerInfo::kULong)
          || bareType == TVirtualStreamerInfo::kULong64)
      {
         SetUnsigned();
      }

      auto bareTypeCopy = static_cast<EDataType>(bareType);
      switch (bareTypeCopy) {
         case kChar_t: // fall-through
         case kUChar_t: // fall-through
         case kchar: // fall-through
         case kBool_t:
            fLenType = 1;
            break;
         case kShort_t: // fall-through
         case kUShort_t: // fall-through
         case kFloat16_t:
            fLenType = 2;
            break;
         case kFloat_t: // fall-through
         case kDouble32_t: // fall-through
         case kInt_t: // fall-through
         case kUInt_t:
            fLenType = 4;
            break;
         case kLong_t: // fall-through
         case kULong_t: // fall-through
         case kLong64_t: // fall-through
         case kULong64_t: // fall-through
         case kDouble_t:
            fLenType = 8;
            break;
         // All cases I don't know how to handle.
         case kOther_t: // fall-through
         case kNoType_t: // fall-through
         case kCounter: // fall-through
         case kCharStar: // fall-through
         case kBits: // fall-through
         case kVoid_t: // fall-through
         case kDataTypeAliasUnsigned_t: // fall-through
         case kDataTypeAliasSignedChar_t: // fall-through
         case kNumDataTypes: // fall-through
            fLenType = 0;
      };
   }
}

////////////////////////////////////////////////////////////////////////////////
/// Default destructor for a LeafObject.

TLeafElement::~TLeafElement()
{
}

////////////////////////////////////////////////////////////////////////////////
/// Determine if this TLeafElement supports bulk IO
TLeaf::DeserializeType
TLeafElement::GetDeserializeType() const
{
   if (R__likely(fDeserializeTypeCache.load(std::memory_order_relaxed) != kInvalid))
      return fDeserializeTypeCache;

   TClass *clptr = nullptr;
   EDataType type = EDataType::kOther_t;
   if (fBranch->GetExpectedType(clptr, type)) {  // Returns non-zero in case of failure
      fDeserializeTypeCache.store(kDestructive, std::memory_order_relaxed);
      return kDestructive;  // I don't know what it is, but we aren't going to use bulk IO.
   }
   fDataTypeCache.store(type, std::memory_order_release);
   if (clptr) {  // Something that requires a dictionary to read; skip.
      fDeserializeTypeCache.store(kDestructive, std::memory_order_relaxed);
      return kDestructive;
   }

   if ((fType == EDataType::kChar_t) || fType == EDataType::kUChar_t || type == EDataType::kBool_t) {
      fDeserializeTypeCache.store(kZeroCopy, std::memory_order_relaxed);
      return kZeroCopy;
   } else if ((type == EDataType::kFloat_t) || (type == EDataType::kDouble_t) ||
              (type == EDataType::kInt_t) || (type == EDataType::kUInt_t) ||
              (type == EDataType::kLong64_t) || (type == EDataType::kULong64_t)) {
      fDeserializeTypeCache.store(kInPlace, std::memory_order_relaxed);
      return kInPlace;
   }

   fDeserializeTypeCache.store(kDestructive, std::memory_order_relaxed);
   return kDestructive;
}

////////////////////////////////////////////////////////////////////////////////
/// Deserialize N events from an input buffer.
Bool_t TLeafElement::ReadBasketFast(TBuffer &input_buf, Long64_t N) {

   EDataType type = fDataTypeCache.load(std::memory_order_consume);

   if ((type == EDataType::kFloat_t) || (type == EDataType::kInt_t) || (type == EDataType::kUInt_t)) {
      Float_t *buf __attribute__((aligned(8))) = reinterpret_cast<Float_t *>(input_buf.GetCurrent());
      for (int idx=0; idx<fLen*N; idx++) {
         Float_t tmp = *reinterpret_cast<Float_t*>(buf + idx); // Makes a copy of the values; frombuf can't handle aliasing.
         char *tmp_ptr = reinterpret_cast<char *>(&tmp);
         frombuf(tmp_ptr, buf + idx);
      }
   } else if ((type == EDataType::kDouble_t) || (type == EDataType::kLong64_t) || (type == EDataType::kULong64_t)) {
      Double_t *buf __attribute__((aligned(8))) = reinterpret_cast<Double_t*>(input_buf.GetCurrent());
      for (int idx=0; idx<fLen*N; idx++) {
         Double_t tmp = *reinterpret_cast<Double_t*>(buf + idx); // Makes a copy of the values; frombuf can't handle aliasing.
         char *tmp_ptr = reinterpret_cast<char *>(&tmp);
         frombuf(tmp_ptr, buf + idx);
      }
   } else {
      return false;
   }

   return true;
}

////////////////////////////////////////////////////////////////////////////////
/// Returns pointer to method corresponding to name name is a string
/// with the general form "method(list of params)" If list of params is
/// omitted, () is assumed;

TMethodCall *TLeafElement::GetMethodCall(const char * /*name*/)
{
   return 0;
}


////////////////////////////////////////////////////////////////////////////////
/// Copy/set fMinimum and fMaximum to include/be wide than those of the parameter

Bool_t TLeafElement::IncludeRange(TLeaf *input)
{
    if (input) {
        if (input->GetMaximum() > this->GetMaximum())
            ((TBranchElement*)fBranch)->fMaximum = input->GetMaximum();
        return kTRUE;
    } else {
        return kFALSE;
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Return true if this leaf is does not have any sub-branch/leaf.

Bool_t TLeafElement::IsOnTerminalBranch() const
{
   if (fBranch->GetListOfBranches()->GetEntriesFast()) return kFALSE;
   return kTRUE;
}
