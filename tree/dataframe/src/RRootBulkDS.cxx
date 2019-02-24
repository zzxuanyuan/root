#include <ROOT/RDF/Utils.hxx>
#include <ROOT/RRootBulkDS.hxx>
#include <ROOT/TSeq.hxx>
#include <TBufferFile.h>
#include <TClass.h>
#include <TDataType.h>
#include <TError.h>
#include <TROOT.h>         // For the gROOTMutex
#include <TVirtualMutex.h> // For the R__LOCKGUARD
#include <ROOT/RMakeUnique.hxx>
#include <ROOT/TBulkBranchRead.hxx>

#include <algorithm>
#include <vector>

namespace ROOT {

namespace Internal {

namespace RDF {

/// A struct to handle the state of each column in a given slot.
//
//  We assume that the TBulkBufferMgr is accessed from a single thread and optimize
//  access for scanning monotonically, one-at-a-time through the corresponding entries.
class TBulkBufferMgr
{
private:
   // This is the mapping from col to void* address.
   std::vector<void*> fBranchAddresses;

   // This is the mapping from col to buffer.
   std::unordered_map<TBranch*, std::unique_ptr<TBufferFile>> fBufferMap;

   // These are all the buffers that must be advanced by n bytes for each event.
   // 1 byte for bool/char, 2 bytes for short, 4 bytes for int/float and 8 bytes for double/long64
   std::vector<TBufferFile*> fNByteBuffers;
   std::vector<TBufferFile*> fFixArrayBuffers;
   std::vector<TBufferFile*> fVarArrayBuffers;

   // This is the mapping from array branch to 
   // 1. size of the array if the branch is fixed size array
   // 2. 1 if the branch is variable size array
   std::unordered_map<TBufferFile*, int> fFixArrayLenMap;
   std::unordered_map<TBufferFile*, std::pair<void*, int>> fVarArrayLenMap;
   // The mapping from buffer to type len
   std::unordered_map<TBufferFile*, int> fTypeLenMap;
   // This is the mapping from array branch to entry offset
   std::unordered_map<TBufferFile*, ULong64_t> fEntryMap;
   // This is the mapping from buffer to leaf values
   std::unordered_map<TBufferFile*, std::unique_ptr<char>> fValueMap;

   // The current absolute entry in the TTree.
   ULong64_t fCurAbsEntry{0};
   // The current entry relative to the cluster beginning.
   ULong64_t fCurRelEntry{0};

   // The current TTree* being processed by the buffer manager.
   TTree *fCurTree{nullptr};

public:
   TBulkBufferMgr(TTree *curTree)
    : fCurTree(curTree)
   {
      const TObjArray *branchList = curTree->GetListOfBranches();
      Int_t branchCount = branchList->GetEntriesFast();
      // collect variable size array branches and process later
      std::vector<TBranch*> bv;
      for (auto idx : ROOT::TSeqU(branchCount)) {
         fBranchAddresses.push_back(nullptr);
         auto br = dynamic_cast<TBranch*>((*branchList)[idx]);
         if (!br || !br->SupportsBulkRead()) {
            printf("Skipping branch %s as it does not support bulk reads.\n", br->GetName());
            continue;
         }
         EDataType dt;
         TClass *cls = nullptr;
         if (br->GetExpectedType(cls, dt)) {
            printf("Skipping branch %s as we failed to retrieve the expected type info.\n", br->GetName());
            continue;
         }

         // get branch leaf
         auto leaf = (TLeaf*)br->GetListOfLeaves()->At(0);
         // initialize branch buffer map
         fBufferMap[br].reset(new TBufferFile(TBuffer::kWrite, 32*1024));
         TBufferFile *bf = fBufferMap[br].get();
         // initialize branch type size map
         int leafLenType = leaf->GetLenType();
         fTypeLenMap[bf] = leafLenType;
         // initialize branch entry map
         fEntryMap[bf] = 0;

         // initialize value map, array information
         auto leafLenStatic = leaf->GetLenStatic();
         auto leafCount = leaf->GetLeafCount();
         if (leafLenStatic > 1) {
            fFixArrayLenMap[bf] = leafLenStatic;
            fFixArrayBuffers.push_back(bf);
            fValueMap[bf].reset(new char[1024]);
         } else if (leafCount) {
            fVarArrayBuffers.push_back(bf);
            fValueMap[bf].reset(new char[1024]);
         } else {
            fTypeLenMap[bf] = leafLenType;
            fNByteBuffers.push_back(bf);
            fValueMap[bf].reset(new char[leafLenType]);
         }
         fBranchAddresses.back() = fValueMap[bf].get();
      }
      // now let us process variable size arrays since their information depend on count leafs
      for (UInt_t i = 0; i < bv.size(); ++i) {
         // find count leaf branch
         TBranch *countBranch = ((TLeaf*)bv[i]->GetListOfLeaves()->At(0))->GetLeafCount()->GetBranch();
         TBufferFile *countBuffer = fBufferMap[countBranch].get();
         // store count leaf information to this branch mapping
         TBufferFile *branchBuffer = fBufferMap[bv[i]].get();
         fVarArrayLenMap[branchBuffer] = std::make_pair(fValueMap[countBuffer].get(), fTypeLenMap[countBuffer]);
      }
   }

   ~TBulkBufferMgr() {
   }

   void *getColumnTargetPtr(size_t idx) {
      return &fBranchAddresses[idx];
   }

   bool SetEntry(ULong64_t entry)
   {
      // TODO: handle random skips.
      if (R__unlikely(fCurAbsEntry != entry)) {
          return false;
      }

      for (UInt_t idx = 0; idx < fNByteBuffers.size(); ++idx) {
         TBufferFile *bf = fNByteBuffers[idx];
         Long64_t offset = fEntryMap[bf];
         char *raw_buffer = bf->GetCurrent();
         if (fTypeLenMap[bf] == 1) {
            Char_t tmp = *reinterpret_cast<Char_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            frombuf(tmp_ptr, (Char_t *)fValueMap[bf].get());
            offset += 1;
         } else if (fTypeLenMap[bf] == 2) {
            Short_t tmp = *reinterpret_cast<Short_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            frombuf(tmp_ptr, (Short_t *)fValueMap[bf].get());
            offset += 2;
         } else if (fTypeLenMap[bf] == 4) {
            Int_t tmp = *reinterpret_cast<Int_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            frombuf(tmp_ptr, (Int_t *)fValueMap[bf].get());
            offset += 4;
         } else if (fTypeLenMap[bf] == 8) {
            Long64_t tmp = *reinterpret_cast<Long64_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            frombuf(tmp_ptr, (Long64_t *)fValueMap[bf].get());
            offset += 8;
         } else {
            return false;
         }
         fEntryMap[bf] = offset;
      }
      for (UInt_t idx = 0; idx < fFixArrayBuffers.size(); ++idx) {
         printf("entry fix array\n");//##
         TBufferFile *bf = fFixArrayBuffers[idx];
         Long64_t offset = fEntryMap[bf];
         char *raw_buffer = bf->GetCurrent();
         Long64_t arrayLen = fFixArrayLenMap[bf];
         if (fTypeLenMap[bf] == 1) {
            Char_t tmp = *reinterpret_cast<Char_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            Char_t *value = (Char_t *)fValueMap[bf].get();
            for (int i = 0; i < arrayLen; ++i) {
               frombuf(tmp_ptr, &value[i]);
            }
            offset += arrayLen * 1;
         } else if (fTypeLenMap[bf] == 2) {
            Short_t tmp = *reinterpret_cast<Short_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            Short_t *value = (Short_t *)fValueMap[bf].get();
            for (int i = 0; i < arrayLen; ++i) {
               frombuf(tmp_ptr, &value[i]);
            }
            offset += arrayLen * 2;
         } else if (fTypeLenMap[bf] == 4) {
            Int_t tmp = *reinterpret_cast<Int_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            Int_t *value = (Int_t *)fValueMap[bf].get();
            for (int i = 0; i < arrayLen; ++i) {
               frombuf(tmp_ptr, &value[i]);
            }
            offset += arrayLen * 4;
         } else if (fTypeLenMap[bf] == 8) {
            Long64_t tmp = *reinterpret_cast<Long64_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            Long64_t *value = (Long64_t *)fValueMap[bf].get();
            for (int i = 0; i < arrayLen; ++i) {
               frombuf(tmp_ptr, &value[i]);
            }
            offset += arrayLen * 8;
         } else {
            return false;
         }
      }
      for (UInt_t idx = 0; idx < fVarArrayBuffers.size(); ++idx) {
         printf("entry var array\n");//##
         TBufferFile *bf = fVarArrayBuffers[idx];
         Long64_t offset = fEntryMap[bf];
         char *raw_buffer = bf->GetCurrent();
         std::pair<void*, int> p = fVarArrayLenMap[bf];
         Long64_t arrayLen = -1;
         if (p.second == 1) {
            Char_t len = *((Char_t *)p.first);
            arrayLen = len;
         } else if (p.second == 2) {
            Short_t len = *((Short_t *)p.first);
            arrayLen = len;
         } else if (p.second == 4) {
            Int_t len = *((Int_t *)p.first);
            arrayLen = len;
         } else if (p.second == 8) {
            Long64_t len = *((Long64_t *)p.first);
            arrayLen = len;
         } else {
            return false;
         }
         if (arrayLen < 0) { return false; }
         if (fTypeLenMap[bf] == 1) {
            Char_t tmp = *reinterpret_cast<Char_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            Char_t *value = (Char_t *)fValueMap[bf].get();
            for (int i = 0; i < arrayLen; ++i) {
               frombuf(tmp_ptr, &value[i]);
            }
            offset += arrayLen * 1;
         } else if (fTypeLenMap[bf] == 2) {
            Short_t tmp = *reinterpret_cast<Short_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            Short_t *value = (Short_t *)fValueMap[bf].get();
            for (int i = 0; i < arrayLen; ++i) {
               frombuf(tmp_ptr, &value[i]);
            }
            offset += arrayLen * 2;
         } else if (fTypeLenMap[bf] == 4) {
            Int_t tmp = *reinterpret_cast<Int_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            Int_t *value = (Int_t *)fValueMap[bf].get();
            for (int i = 0; i < arrayLen; ++i) {
               frombuf(tmp_ptr, &value[i]);
            }
            offset += arrayLen * 4;
         } else if (fTypeLenMap[bf] == 8) {
            Long64_t tmp = *reinterpret_cast<Long64_t*>(&raw_buffer[offset]);
            char *tmp_ptr = reinterpret_cast<char *>(&tmp);
            Long64_t *value = (Long64_t *)fValueMap[bf].get();
            for (int i = 0; i < arrayLen; ++i) {
               frombuf(tmp_ptr, &value[i]);
            }
            offset += arrayLen * 8;
         } else {
            return false;
         }
      }
      fCurAbsEntry++;
      fCurRelEntry++;
      return true;
   }

   // Initialize a cluster range for processing.
   // Returns true if successful
   // On success, sets entry count to the number of available entries.
   bool SetEntryRange(ULong64_t firstEntry, Int_t &entryCount)
   {
      const TObjArray *branchList = fCurTree->GetListOfBranches();
      Int_t branchCount = branchList->GetEntriesFast();
      Int_t count = -1;
      for (auto idx : ROOT::TSeqU(branchCount)) {
         auto br = static_cast<TBranch*>((*branchList)[idx]);
         TBufferFile *bf = fBufferMap[br].get();
         auto leaf = static_cast<TLeaf*>(br->GetListOfLeaves()->At(0));
         auto leafCount = leaf->GetLeafCount();
         fEntryMap[bf] = 0;
         auto result = -1;
         if(leafCount) {
            // search for leaf count TBufferFile
            auto countBranch = leafCount->GetBranch();
            TBufferFile *countBuffer = fBufferMap[countBranch].get();
            result = br->GetBulkRead().GetEntriesSerialized(firstEntry, *bf, countBuffer);
         } else {
            result = br->GetBulkRead().GetEntriesSerialized(firstEntry, *bf);
         }
         // TODO: this fails if all the baskets in the cluster do not have the same size.
         if (result < 0) return false;
         else if ((count >= 0) && (result != count)) return false;
         else count = result;
      }
      fCurAbsEntry = firstEntry;
      fCurRelEntry = 0;
      entryCount = count;
      return true;
   }
};

} // ns RDF

} // ns Internal

namespace RDF {

namespace Experimental {

// Return a list of type-erased pointers, one per slot.
// The target of the pointer is the location where we will deserialize the column's value for
// the given slot.
std::vector<void *> RRootBulkDS::GetColumnReadersImpl(std::string_view name, const std::type_info &id)
{
   const auto colTypeName = GetTypeName(name);
   const auto &colTypeId = ROOT::Internal::RDF::TypeName2TypeID(colTypeName);
   if (id != colTypeId) {
      std::string err = "The type of column \"";
      err += name;
      err += "\" is ";
      err += colTypeName;
      err += " but a different one has been selected.";
      throw std::runtime_error(err);
   }

   const auto index =
      std::distance(fListOfBranches.begin(), std::find(fListOfBranches.begin(), fListOfBranches.end(), name));

   std::vector<void *> ret(fNSlots);
   for (auto slot : ROOT::TSeqU(fNSlots)) {
      ret[slot] = static_cast<void *>(fBufferMgrs[slot]->getColumnTargetPtr(index));
   }
   return ret;
}

RRootBulkDS::RRootBulkDS(std::string_view treeName, std::string_view fileNameGlob)
   : fTreeName(treeName), fFileNameGlob(fileNameGlob), fModelChain(std::string(treeName).c_str())
{
   fModelChain.Add(fFileNameGlob.c_str());

   const TObjArray &lob = *fModelChain.GetListOfBranches();
   fListOfBranches.resize(lob.GetEntries());

   TIterCategory<TObjArray> iter(&lob);
   std::transform(iter.Begin(), iter.End(), fListOfBranches.begin(), [](TObject *o) { return o->GetName(); });
}

RRootBulkDS::~RRootBulkDS()
{
}

std::string RRootBulkDS::GetTypeName(std::string_view colName) const
{
   if (!HasColumn(colName)) {
      std::string e = "The dataset does not have column ";
      e += colName;
      throw std::runtime_error(e);
   }
   // TODO: we need to factor out the routine for the branch alone...
   // Maybe a cache for the names?
   auto typeName =
      ROOT::Internal::RDF::ColumnName2ColumnTypeName(std::string(colName), /*nsID=*/0, &fModelChain, /*ds=*/nullptr,
                                                     /*isCustomCol=*/false);
   // We may not have yet loaded the library where the dictionary of this type is
   TClass::GetClass(typeName.c_str());
   return typeName;
}

const std::vector<std::string> &RRootBulkDS::GetColumnNames() const
{
   return fListOfBranches;
}

bool RRootBulkDS::HasColumn(std::string_view colName) const
{
   if (!fListOfBranches.empty())
      GetColumnNames();
   return fListOfBranches.end() != std::find(fListOfBranches.begin(), fListOfBranches.end(), colName);
}

void RRootBulkDS::InitSlot(unsigned int slot, ULong64_t firstEntry)
{
   Int_t eventCount;
   if (!fBufferMgrs[slot]->SetEntryRange(firstEntry, eventCount)) {
      throw std::runtime_error("Failed to initialize slot");
   }
   // TODO: compare eventCount against calculated event ranges -- should be the same sizes!
}


void RRootBulkDS::InitialiseSlot(unsigned int slot)
{
   auto chain = new TChain(fTreeName.c_str());
   chain->ResetBit(kMustCleanup);
   chain->Add(fFileNameGlob.c_str());
   chain->GetEntry(0);
   fBufferMgrs[slot].reset(new ROOT::Internal::RDF::TBulkBufferMgr(chain->GetTree()));
   fChains[slot].reset(chain);
}


std::vector<std::pair<ULong64_t, ULong64_t>> RRootBulkDS::GetEntryRanges()
{
   if (fReturnedFirstRange) return {};
   // TODO: Improve this to allow for multi-file chains.

   Long64_t clusterStart = 0;
   fModelChain.GetEntry(0);
   auto tree = fModelChain.GetTree();
   auto clusterIter = tree->GetClusterIterator(clusterStart);
   std::vector<std::pair<ULong64_t, ULong64_t>> entryRanges;

   while ( (clusterStart = clusterIter()) < tree->GetEntries() ) {
      entryRanges.emplace_back(clusterStart, clusterIter.GetNextEntry());
   }

   fReturnedFirstRange = true;
   return entryRanges;
}


bool RRootBulkDS::SetEntry(unsigned int slot, ULong64_t entry)
{
   //printf("RRootBulkDS::SetEntry slot %u, entry %llu\n", slot, entry);
   bool result = fBufferMgrs[slot]->SetEntry(entry);
   //printf("Value: %d\n", **static_cast<int**>(fBufferMgrs[slot]->getColumnTargetPtr(0)));
   return result;
}


void RRootBulkDS::SetNSlots(unsigned int nSlots)
{
   R__ASSERT(0U == fNSlots && "Setting the number of slots even if the number of slots is different from zero.");

   //printf("Setting parallelism to %u\n", nSlots);
   fNSlots = nSlots;
   fChains.resize(fNSlots);
   fBufferMgrs.resize(fNSlots);

   for (auto slot : ROOT::TSeqU(fNSlots)) {
      InitialiseSlot(slot);
   }
}

std::string RRootBulkDS::GetDataSourceType()
{
   return "RootBulk";
}

RDataFrame MakeRootBulkDataFrame(std::string_view treeName, std::string_view fileNameGlob)
{
   ROOT::RDataFrame tdf(std::make_unique<RRootBulkDS>(treeName, fileNameGlob));
   return tdf;
}

} // ns Experimental

} // ns RDF

} // ns ROOT
