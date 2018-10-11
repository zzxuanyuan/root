// Author: Brian Bockelman CERN  10/2018

/*************************************************************************
 * Copyright (C) 1995-2018, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RROOTBULKDS
#define ROOT_RROOTBULKDS

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RDataSource.hxx"
#include <TChain.h>

#include <memory>

namespace ROOT {

namespace Internal {
namespace RDF {
class TBulkBufferMgr;
} // ns RDF
} // ns Internal

namespace RDF {

namespace Experimental {

class RRootBulkDS final : public ROOT::RDF::RDataSource {
private:
   unsigned int fNSlots{0U};
   std::string fTreeName;
   std::string fFileNameGlob;
   mutable TChain fModelChain; // Mutable needed for getting the column type name
   std::vector<std::string> fListOfBranches;
   std::vector<std::pair<ULong64_t, ULong64_t>> fEntryRanges;
   std::vector<std::unique_ptr<ROOT::Internal::RDF::TBulkBufferMgr>> fBufferMgrs; // One buffer manager per slot.
   std::vector<std::unique_ptr<TChain>> fChains;

   std::vector<void *> GetColumnReadersImpl(std::string_view, const std::type_info &);
   void InitialiseSlot(unsigned int slot);

protected:
   std::string AsString() { return "ROOT bulk API data source"; };

public:
   RRootBulkDS(std::string_view treeName, std::string_view fileNameGlob);
   ~RRootBulkDS();
   std::string GetTypeName(std::string_view colName) const;
   const std::vector<std::string> &GetColumnNames() const;
   bool HasColumn(std::string_view colName) const;
   void InitSlot(unsigned int slot, ULong64_t firstEntry);
   void FinaliseSlot(unsigned int /*slot*/) {}
   std::vector<std::pair<ULong64_t, ULong64_t>> GetEntryRanges();
   bool SetEntry(unsigned int slot, ULong64_t entry);
   void SetNSlots(unsigned int nSlots);
   std::string GetDataSourceType();
};

RDataFrame MakeRootBulkDataFrame(std::string_view treeName, std::string_view fileNameGlob);

} // ns Experimental 

} // ns RDF

} // ns ROOT

#endif
