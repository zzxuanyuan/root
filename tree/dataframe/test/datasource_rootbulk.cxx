#include <TBufferFile.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RRootBulkDS.hxx>
#include <ROOT/TTreeReaderFast.hxx>
#include <ROOT/TTreeReaderValueFast.hxx>
#include <ROOT/TBulkBranchRead.hxx>
#include <ROOT/TSeq.hxx>

#include <gtest/gtest.h>

#include <algorithm> // std::accumulate
#include <iostream>

using namespace ROOT;
using namespace ROOT::RDF;
using namespace ROOT::RDF::Experimental;

auto fileName0 = "RRootBulkDS_input_0.root";
auto fileGlob = "RRootBulkDS_input_*.root";
auto fileNameBig = "RRootBulkDS_input_big.root";
auto treeName = "t";

auto eventCount = 10;
auto bigEventCount = 100e6;
auto bigFlushSize = 50e3;
const auto kSlots = 4U;

TEST(RRootBulkDS, GenerateData)
{
   Char_t c = 'a';
   Bool_t b = true;
   Short_t s = 0;
   UShort_t us = 0;
   Int_t i = 0;
   UInt_t ui = 0;
   Long64_t l = 0;
   ULong64_t ul = 0;
   Float_t f = 0.0;
   Double_t d = 0.0;
   
   // TODO: re-introduce multiple files so we can test globbing.
   for (auto &&fileName : {fileName0}) {
      RDataFrame tdf(eventCount);
      tdf.Define("i", [&i]() { return i++; })
         .Define("ui", [&ui]() { return ui++; })
         .Define("s", [&s]() { return s++; })
         .Define("us", [&us]() { return us++; })
         .Define("l", [&l]() { return l++; })
         .Define("ul", [&ul]() { return ul++; })
         .Define("f", [&f]() { return f++; })
         .Define("d", [&d]() { return d++; })
         .Define("c", [&c]() { return c++; })
         .Define("b", [&b]() { b = !b; return !b; })
         .Snapshot(treeName, fileName);
   }

   // Manually create a larger file, allowing us to closely control its formatting.
   auto hfile = new TFile(fileNameBig, "RECREATE", "TTree float micro benchmark ROOT file");
   hfile->SetCompressionLevel(0); // No compression at all.

   // Otherwise, we keep with the current ROOT defaults.
   auto tree = new TTree("t", "A ROOT tree of floats.");
   tree->SetAutoFlush(bigFlushSize);
   tree->SetBit(TTree::kOnlyFlushAtCluster);
   Int_t i2 = 2;
   UInt_t ui2 = 2;
   Short_t s2 = 2;
   UShort_t us2 = 2;
   Long64_t l2 = 2;
   ULong64_t ul2 = 2;
   Float_t f2 = 2.0;
   Double_t d2 = 2.0;
   Char_t c2 = 'a';
   Bool_t b2 = true;
   tree->Branch("myInt", &i2, 320000, 1);
   tree->Branch("myUInt", &ui2, 320000, 1);
   tree->Branch("myShort", &s2, 320000, 1);
   tree->Branch("myUShort", &us2, 320000, 1);
   tree->Branch("myLLong", &l2, 320000, 1);
   tree->Branch("myULLong", &ul2, 320000, 1);
   tree->Branch("myFloat", &f2, 320000, 1);
   tree->Branch("myDouble", &d2, 320000, 1);
   tree->Branch("myChar", &c2, 320000, 1);
   tree->Branch("myBool", &b2, 320000, 1);
   for (Long64_t ev = 0; ev < bigEventCount; ev++) {
      tree->Fill();
      i2++;
      ui2++;
      s2++;
      us2++;
      l2++;
      ul2++;
      f2++;
      d2++;
      c2++;
      b2 = !b2;
   }
   hfile = tree->GetCurrentFile();
   hfile->Write();
   tree->Print();
   //printf("Successful write of all events.\n");
   hfile->Close();
   delete hfile;
}

// Test that the bulk APIs can actually read the data generated by RDF.
TEST(RRootBulkDS, DirectBulkRead)
{
   auto hfile = TFile::Open(fileName0);
   printf("Starting read of file %s.\n", fileName0);
   TStopwatch sw;

   printf("Using inline bulk read APIs.\n");
   TBufferFile branchbufI(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufUI(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufS(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufUS(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufL(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufUL(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufF(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufD(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufC(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufB(TBuffer::kWrite, 32*1024);
   TTree *tree = dynamic_cast<TTree*>(hfile->Get("t"));
   ASSERT_TRUE(tree);

   TBranch *branchI = tree->GetBranch("i");
   TBranch *branchUI = tree->GetBranch("ui");
   TBranch *branchS = tree->GetBranch("s");
   TBranch *branchUS = tree->GetBranch("us");
   TBranch *branchL = tree->GetBranch("l");
   TBranch *branchUL = tree->GetBranch("ul");
   TBranch *branchF = tree->GetBranch("f");
   TBranch *branchD = tree->GetBranch("d");
   TBranch *branchC = tree->GetBranch("c");
   TBranch *branchB = tree->GetBranch("b");

   ASSERT_TRUE(branchI);
   ASSERT_TRUE(branchUI);
   ASSERT_TRUE(branchS);
   ASSERT_TRUE(branchUS);
   ASSERT_TRUE(branchL);
   ASSERT_TRUE(branchUL);
   ASSERT_TRUE(branchF);
   ASSERT_TRUE(branchD);
   ASSERT_TRUE(branchC);
   ASSERT_TRUE(branchB);

   Int_t events = eventCount;
   Long64_t evt_idx = 0;
   while (events) {
      auto countC = branchC->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufC);
      ASSERT_EQ(countC, eventCount);
      auto countB = branchB->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufB);
      ASSERT_EQ(countB, eventCount);
      auto countI = branchI->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufI);
      ASSERT_EQ(countI, eventCount);
      auto countUI = branchUI->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufUI);
      ASSERT_EQ(countUI, eventCount);
      auto countS = branchS->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufS);
      ASSERT_EQ(countS, eventCount);
      auto countUS = branchUS->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufUS);
      ASSERT_EQ(countUS, eventCount);
      auto countL = branchL->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufL);
      ASSERT_EQ(countL, eventCount);
      auto countUL = branchUL->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufUL);
      ASSERT_EQ(countUL, eventCount);
      auto countF = branchF->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufF);
      ASSERT_EQ(countF, eventCount);
      auto countD = branchD->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufD);
      ASSERT_EQ(countD, eventCount);
      events = events > countI ? (events - countI) : 0;

      int *entryI  = reinterpret_cast<int *>(branchbufI.GetCurrent());
      unsigned int *entryUI = reinterpret_cast<unsigned int *>(branchbufUI.GetCurrent());
      short *entryS  = reinterpret_cast<short *>(branchbufS.GetCurrent());
      unsigned short *entryUS = reinterpret_cast<unsigned short *>(branchbufUS.GetCurrent());
      long long int *entryL  = reinterpret_cast<long long int *>(branchbufL.GetCurrent());
      unsigned long long int *entryUL = reinterpret_cast<unsigned long long int *>(branchbufUL.GetCurrent());
      float *entryF  = reinterpret_cast<float *>(branchbufF.GetCurrent());
      double *entryD  = reinterpret_cast<double *>(branchbufD.GetCurrent());
      char *entryC  = reinterpret_cast<char *>(branchbufC.GetCurrent());
      bool *entryB  = reinterpret_cast<bool *>(branchbufB.GetCurrent());
      Bool_t evtB = true;
      for (Int_t idx=0; idx < countB; idx++) {
         Bool_t tmpB = *reinterpret_cast<Bool_t*>(&entryB[idx]);
         char *tmpB_ptr = reinterpret_cast<char *>(&tmpB);
         bool valB;
         frombuf(tmpB_ptr, &valB);
         EXPECT_EQ(valB, evtB);
         evtB = !evtB;
      }
      Int_t evtI = 0;
      for (Int_t idx=0; idx < countI; idx++) {
         Int_t tmpI = *reinterpret_cast<Int_t*>(&entryI[idx]);
         char *tmpI_ptr = reinterpret_cast<char *>(&tmpI);
         int valI;
         frombuf(tmpI_ptr, &valI);
         EXPECT_EQ(valI, evtI);
         evtI++;
         evt_idx++;
      }
      UInt_t evtUI = 0;
      for (Int_t idx=0; idx < countUI; idx++) {
         UInt_t tmpUI = *reinterpret_cast<UInt_t*>(&entryUI[idx]);
         char *tmpUI_ptr = reinterpret_cast<char *>(&tmpUI);
         unsigned int valUI;
         frombuf(tmpUI_ptr, &valUI);
         EXPECT_EQ(valUI, evtUI);
         evtUI++;
      }
      Short_t evtS = 0;
      for (Int_t idx=0; idx < countS; idx++) {
         Short_t tmpS = *reinterpret_cast<Short_t*>(&entryS[idx]);
         char *tmpS_ptr = reinterpret_cast<char *>(&tmpS);
         short valS;
         frombuf(tmpS_ptr, &valS);
         EXPECT_EQ(valS, evtS);
         evtS++;
      }
      UShort_t evtUS = 0;
      for (Int_t idx=0; idx < countUS; idx++) {
         UShort_t tmpUS = *reinterpret_cast<UShort_t*>(&entryUS[idx]);
         char *tmpUS_ptr = reinterpret_cast<char *>(&tmpUS);
         unsigned short valUS;
         frombuf(tmpUS_ptr, &valUS);
         EXPECT_EQ(valUS, evtUS);
         evtUS++;
      }
      Long64_t evtL = 0;
      for (Int_t idx=0; idx < countL; idx++) {
         Long64_t tmpL = *reinterpret_cast<Long64_t*>(&entryL[idx]);
         char *tmpL_ptr = reinterpret_cast<char *>(&tmpL);
         long long int valL;
         frombuf(tmpL_ptr, &valL);
         EXPECT_EQ(valL, evtL);
         evtL++;
      }
      ULong64_t evtUL = 0;
      for (Int_t idx=0; idx < countUL; idx++) {
         ULong64_t tmpUL = *reinterpret_cast<ULong64_t*>(&entryUL[idx]);
         char *tmpUL_ptr = reinterpret_cast<char *>(&tmpUL);
         unsigned long long int valUL;
         frombuf(tmpUL_ptr, &valUL);
         EXPECT_EQ(valUL, evtUL);
         evtUL++;
      }
      Float_t evtF = 0;
      for (Int_t idx=0; idx < countF; idx++) {
         Float_t tmpF = *reinterpret_cast<Float_t*>(&entryF[idx]);
         char *tmpF_ptr = reinterpret_cast<char *>(&tmpF);
         float valF;
         frombuf(tmpF_ptr, &valF);
         EXPECT_EQ(valF, evtF);
         evtF++;
      }
      Double_t evtD = 0;
      for (Int_t idx=0; idx < countD; idx++) {
         Double_t tmpD = *reinterpret_cast<Double_t*>(&entryD[idx]);
         char *tmpD_ptr = reinterpret_cast<char *>(&tmpD);
         double valD;
         frombuf(tmpD_ptr, &valD);
         EXPECT_EQ(valD, evtD);
         evtD++;
      }
      Char_t evtC = 'a';
      for (Int_t idx=0; idx < countC; idx++) {
         Char_t tmpC = *reinterpret_cast<Char_t*>(&entryC[idx]);
         char *tmpC_ptr = reinterpret_cast<char *>(&tmpC);
         char valC;
         frombuf(tmpC_ptr, &valC);
         EXPECT_EQ(valC, evtC);
         evtC++;
      }
   }
   sw.Stop();
   printf("GetEntriesSerialized: Successful read of all %lld events.\n", evt_idx);
   printf("GetEntriesSerialized: Total elapsed time (seconds) for bulk APIs: %.2f\n", sw.RealTime());
}

TEST(RRootBulkDS, ColTypeNames)
{
   RRootBulkDS tds(treeName, fileGlob);
   tds.SetNSlots(1);

   auto colNames = tds.GetColumnNames();

   EXPECT_TRUE(tds.HasColumn("i"));
   EXPECT_TRUE(tds.HasColumn("ui"));
   EXPECT_TRUE(tds.HasColumn("l"));
   EXPECT_TRUE(tds.HasColumn("ul"));
   EXPECT_TRUE(tds.HasColumn("s"));
   EXPECT_TRUE(tds.HasColumn("us"));
   EXPECT_TRUE(tds.HasColumn("c"));
   EXPECT_TRUE(tds.HasColumn("b"));
   EXPECT_TRUE(tds.HasColumn("d"));
   EXPECT_TRUE(tds.HasColumn("f"));
   EXPECT_FALSE(tds.HasColumn("bla"));

   EXPECT_STREQ("i", colNames[0].c_str());
   EXPECT_STREQ("Int_t", tds.GetTypeName("i").c_str());
   EXPECT_STREQ("ui", colNames[1].c_str());
   EXPECT_STREQ("UInt_t", tds.GetTypeName("ui").c_str());
   EXPECT_STREQ("s", colNames[2].c_str());
   EXPECT_STREQ("Short_t", tds.GetTypeName("s").c_str());
   EXPECT_STREQ("us", colNames[3].c_str());
   EXPECT_STREQ("UShort_t", tds.GetTypeName("us").c_str());
   EXPECT_STREQ("l", colNames[4].c_str());
   EXPECT_STREQ("Long64_t", tds.GetTypeName("l").c_str());
   EXPECT_STREQ("ul", colNames[5].c_str());
   EXPECT_STREQ("ULong64_t", tds.GetTypeName("ul").c_str());
   EXPECT_STREQ("f", colNames[6].c_str());
   EXPECT_STREQ("Float_t", tds.GetTypeName("f").c_str());
   EXPECT_STREQ("d", colNames[7].c_str());
   EXPECT_STREQ("Double_t", tds.GetTypeName("d").c_str());
   EXPECT_STREQ("c", colNames[8].c_str());
   EXPECT_STREQ("Char_t", tds.GetTypeName("c").c_str());
   EXPECT_STREQ("b", colNames[9].c_str());
   EXPECT_STREQ("Bool_t", tds.GetTypeName("b").c_str());
}

TEST(RRootBulkDS, EntryRanges)
{
   RRootBulkDS tds(treeName, fileGlob);
   tds.SetNSlots(3U);
   tds.Initialise();

   auto ranges = tds.GetEntryRanges();

   EXPECT_EQ(1U, ranges.size());
   EXPECT_EQ(0U, ranges[0].first);
   EXPECT_EQ(10U, ranges[0].second);
}

TEST(RRootBulkDS, ColumnReaders)
{
   RRootBulkDS tds(treeName, fileGlob);
   tds.SetNSlots(1);
   
   auto valsI  = tds.GetColumnReaders<int>("i");
   auto valsUI = tds.GetColumnReaders<unsigned int>("ui");
   auto valsS  = tds.GetColumnReaders<short>("s");
   auto valsUS = tds.GetColumnReaders<unsigned short>("us");
   auto valsL  = tds.GetColumnReaders<long long int>("l");
   auto valsUL = tds.GetColumnReaders<unsigned long long int>("ul");
   auto valsF  = tds.GetColumnReaders<float>("f");
   auto valsD  = tds.GetColumnReaders<double>("d");
   auto valsC  = tds.GetColumnReaders<char>("c");
   auto valsB  = tds.GetColumnReaders<bool>("b");
   tds.Initialise();
   auto ranges = tds.GetEntryRanges();
   auto slot = 0U;
   int testI = 0;
   unsigned int testUI = 0;
   short testS = 0;
   unsigned short testUS = 0;
   long long int testL = 0;
   unsigned long long int testUL = 0;
   float testF = 0.0;
   double testD = 0.0;
   char testC = 'a';
   bool testB = true;
   for (auto &&range : ranges) {
      tds.InitSlot(slot, range.first);
      for (auto i : ROOT::TSeq<long long int>(range.first, range.second)) {
         tds.SetEntry(slot, i);
         auto valI = **valsI[slot];
         ASSERT_EQ(testI, valI);
         testI++;
         auto valUI = **valsUI[slot];
         ASSERT_EQ(testUI, valUI);
         testUI++;
         auto valS = **valsS[slot];
         ASSERT_EQ(testS, valS);
         testS++;
         auto valUS = **valsUS[slot];
         ASSERT_EQ(testUS, valUS);
         testUS++;
         auto valL = **valsL[slot];
         ASSERT_EQ(testL, valL);
         testL++;
         auto valUL = **valsUL[slot];
         ASSERT_EQ(testUL, valUL);
         testUL++;
         auto valF = **valsF[slot];
         ASSERT_EQ(testF, valF);
         testF++;
         auto valD = **valsD[slot];
         ASSERT_EQ(testD, valD);
         testD++;
         auto valC = **valsC[slot];
         ASSERT_EQ(testC, valC);
         testC += 1;
         auto valB = **valsB[slot];
         ASSERT_EQ(testB, valB);
         testB = !testB;
      }
      slot++;
   }
}

TEST(RRootBulkDS, ColumnReadersWrongType)
{
   RRootBulkDS tds(treeName, fileGlob);
   tds.SetNSlots(1);
   int res = 1;
   try {
      auto vals = tds.GetColumnReaders<char *>("i");
   } catch (const std::runtime_error &e) {
      EXPECT_STREQ("The type of column \"i\" is Int_t but a different one has been selected.", e.what());
      res = 0;
   }
   EXPECT_EQ(0, res);
}

#ifndef NDEBUG

TEST(RRootBulkDS, SetNSlotsTwice)
{
   auto theTest = []() {
      RRootBulkDS tds(treeName, fileGlob);
      tds.SetNSlots(1);
      tds.SetNSlots(1);
   };
   ASSERT_DEATH(theTest(), "Setting the number of slots even if the number of slots is different from zero.");
}
#endif

#ifdef R__B64


TEST(RRootBulkDS, FromARDFWithJitting)
{
   std::unique_ptr<RDataSource> tds(new RRootBulkDS(treeName, fileGlob));
   RDataFrame tdf(std::move(tds));
   auto max = tdf.Filter("i<6").Max("i");
   auto min = tdf.Define("j", "i").Filter("j>4").Min("j");

   EXPECT_DOUBLE_EQ(5., *max);
   EXPECT_DOUBLE_EQ(5., *min);
}


TEST(RRootBulkDS, FromARDF)
{
   std::unique_ptr<RDataSource> tds(new RRootBulkDS(treeName, fileGlob));
   RDataFrame tdf(std::move(tds));
   auto max = tdf.Max<int>("i");
   auto min = tdf.Min<int>("i");
   auto c = tdf.Count();

   EXPECT_EQ(10U, *c);
   EXPECT_DOUBLE_EQ(9., *max);
   EXPECT_DOUBLE_EQ(0., *min);
}


// NOW MT!-------------
#ifdef R__USE_IMT

TEST(RootBulkDS, DefineSlotMT)
{
   ROOT::EnableImplicitMT(kSlots);

   std::vector<unsigned int> ids(kSlots, 0u);
   std::unique_ptr<RDataSource> tds(new RRootBulkDS(treeName, fileGlob));
   RDataFrame d(std::move(tds));
   auto m = d.DefineSlot("x", [&](unsigned int slot) {
                ids[slot] = 1u;
                return 1;
             }).Max("x");
   EXPECT_EQ(1, *m); // just in case

   const auto nUsedSlots = std::accumulate(ids.begin(), ids.end(), 0u);
   EXPECT_GT(nUsedSlots, 0u);
   EXPECT_LE(nUsedSlots, kSlots);
}

TEST(RRootBulkDS, FromARDFMT)
{
   ROOT::EnableImplicitMT(kSlots);

   std::unique_ptr<RDataSource> tds(new RRootBulkDS(treeName, fileGlob));
   RDataFrame tdf(std::move(tds));
   auto max = tdf.Max<int>("i");
   auto min = tdf.Min<int>("i");
   auto c = tdf.Count();

   EXPECT_EQ(10U, *c);
   EXPECT_DOUBLE_EQ(9., *max);
   EXPECT_DOUBLE_EQ(0., *min);
}

TEST(RRootBulkDS, FromARDFWithJittingMT)
{
   ROOT::EnableImplicitMT(kSlots);

   std::unique_ptr<RDataSource> tds(new RRootBulkDS(treeName, fileGlob));
   RDataFrame tdf(std::move(tds));
   auto max = tdf.Filter("i<6").Max("i");
   auto min = tdf.Define("j", "i").Filter("j>4").Min("j");

   EXPECT_DOUBLE_EQ(5., *max);
   EXPECT_DOUBLE_EQ(5., *min);
}

// Simple speed test
TEST(RRootBulkDS, BenchmarkBulkDS)
{
   auto hfile = TFile::Open(fileNameBig);
   printf("Starting read of file %s.\n", fileNameBig);
   TStopwatch sw;

   printf("Using inline bulk read APIs.\n");
   TBufferFile branchbuf(TBuffer::kWrite, 32*1024);
   TTree *tree = dynamic_cast<TTree*>(hfile->Get("t"));
   ASSERT_TRUE(tree);

   TBranch *branchI = tree->GetBranch("myInt");
   ASSERT_TRUE(branchI);

   sw.Start();
   Int_t events = bigEventCount;
   Long64_t evt_idx = 0;
   Int_t max_bulk = 0;
   while (events) {
      auto count = branchI->GetBulkRead().GetEntriesSerialized(evt_idx, branchbuf);
      ASSERT_EQ(count, bigFlushSize);
      events = events > count ? (events - count) : 0;

      int *entry = reinterpret_cast<int *>(branchbuf.GetCurrent());
      for (Int_t idx=0; idx < count; idx++) {
         Int_t tmp = *reinterpret_cast<Int_t*>(&entry[idx]);
         char *tmp_ptr = reinterpret_cast<char *>(&tmp);
         int val;
         frombuf(tmp_ptr, &val);

         //EXPECT_EQ(val, evt_idx+2);
         //evt_idx++;
         if (val > max_bulk) {max_bulk = val;}
      }
      evt_idx += count;
   }
   sw.Stop();
   EXPECT_EQ(max_bulk, bigEventCount+1);
   printf("GetEntriesSerialized: Successful read of all %lld events.\n", evt_idx);
   printf("GetEntriesSerialized: Total elapsed time (seconds) for bulk APIs: %.2f\n", sw.RealTime());

   printf("Using TTreeReaderFast.\n");
   hfile = TFile::Open(fileNameBig);
   printf("Starting read of file %s.\n", fileNameBig);

   ROOT::Experimental::TTreeReaderFast myReader(treeName, hfile);
   ROOT::Experimental::TTreeReaderValueFast<int> myInt(myReader, "myInt");
   myReader.SetEntry(0);
   if (ROOT::Internal::TTreeReaderValueBase::kSetupMatch != myInt.GetSetupStatus()) {
      printf("TTreeReaderValueFast<float> failed to initialize.  Status code: %d\n", myInt.GetSetupStatus());
      ASSERT_TRUE(false);
   }
   if (myReader.GetEntryStatus() != TTreeReader::kEntryValid) {
      printf("TTreeReaderFast failed to initialize.  Entry status: %d\n", myReader.GetEntryStatus());
      ASSERT_TRUE(false);
   }

   max_bulk = 0;
   sw.Reset();
   sw.Start();
   for (auto reader_idx : myReader) {
      (void)reader_idx;
      int val = *myInt;
      if (max_bulk < val) max_bulk = val;
   }
   sw.Stop();
   EXPECT_EQ(max_bulk, bigEventCount+1);
   printf("TTreeReaderFast: Successful read of all events.\n");
   printf("TTreeReaderFast: Total elapsed time (seconds): %.2f\n", sw.RealTime());

   RDataFrame tdf(treeName, fileNameBig, {"myInt"});

   printf("Using standard RDF APIs.\n");
   sw.Reset();
   sw.Start();
   auto max = tdf.Max<int>("myInt");
   //auto c = tdf.Count();

   EXPECT_EQ(bigEventCount+1, *max);
   sw.Stop();
   printf("Standard RDF APIs: Total elapsed time (seconds): %.2f\n", sw.RealTime());

   std::unique_ptr<RDataSource> rds2(new RRootBulkDS(treeName, fileNameBig));
   RDataFrame rdf2(std::move(rds2));

   printf("Using bulk RDS APIs.\n");
   sw.Reset();
   sw.Start();
   auto max2 = rdf2.Max<int>("myInt");
   //auto c = tdf.Count();

   EXPECT_EQ(bigEventCount+1, *max2);
   sw.Stop();
   printf("Using bulk RDS APIs: Total elapsed time (seconds): %.2f\n", sw.RealTime());

   printf("Using bulk data source directly.\n");
   RRootBulkDS tds(treeName, fileNameBig);
   tds.SetNSlots(1);
   auto vals = tds.GetColumnReaders<int>("myInt");
   tds.Initialise();
   auto ranges = tds.GetEntryRanges();
   auto slot = 0U;
   Int_t max3 = 0;
   sw.Reset();
   sw.Start();
   for (auto &&range : ranges) {
      tds.InitSlot(slot, range.first);
      for (auto i = range.first; i < range.second; i++) {
         tds.SetEntry(slot, i);
         auto val = **vals[slot];
         if (val > max3) {max3 = val;}
      }
   }
   sw.Stop();
   //EXPECT_EQ(bigEventCount+1, max3);
   printf("Using bulk RDS directly: Total elapsed time (seconds): %.2f\n", sw.RealTime());
}

// Test different primitives.

TEST(RRootBulkDS, Primitives)
{
   auto hfile = TFile::Open(fileName0);
   TBufferFile branchbufI(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufUI(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufS(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufUS(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufL(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufUL(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufF(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufD(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufC(TBuffer::kWrite, 32*1024);
   TBufferFile branchbufB(TBuffer::kWrite, 32*1024);
   TTree *tree = dynamic_cast<TTree*>(hfile->Get("t"));
   ASSERT_TRUE(tree);

   TBranch *branchI = tree->GetBranch("i");
   ASSERT_TRUE(branchI);
   TBranch *branchUI = tree->GetBranch("ui");
   ASSERT_TRUE(branchUI);
   TBranch *branchS = tree->GetBranch("s");
   ASSERT_TRUE(branchS);
   TBranch *branchUS = tree->GetBranch("us");
   ASSERT_TRUE(branchUS);
   TBranch *branchL = tree->GetBranch("l");
   ASSERT_TRUE(branchL);
   TBranch *branchUL = tree->GetBranch("ul");
   ASSERT_TRUE(branchUL);
   TBranch *branchF = tree->GetBranch("f");
   ASSERT_TRUE(branchF);
   TBranch *branchD = tree->GetBranch("d");
   ASSERT_TRUE(branchD);
   TBranch *branchC = tree->GetBranch("c");
   ASSERT_TRUE(branchC);
   TBranch *branchB = tree->GetBranch("b");
   ASSERT_TRUE(branchB);

   Int_t events = eventCount;
   Long64_t evt_idx = 0;
   Int_t expectedI = 0;
   UInt_t expectedUI = 0;
   Short_t expectedS = 0;
   UShort_t expectedUS = 0;
   Long64_t expectedL = 0;
   ULong64_t expectedUL = 0;
   Float_t expectedF = 0.0;
   Double_t expectedD = 0.0;
   Char_t expectedC = 'a';
   Bool_t expectedB = true;
   while (events) {
      auto countI = branchI->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufI);
      ASSERT_EQ(countI, eventCount);
      auto countUI = branchUI->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufUI);
      ASSERT_EQ(countUI, eventCount);
      auto countS = branchS->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufS);
      ASSERT_EQ(countS, eventCount);
      auto countUS = branchUS->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufUS);
      ASSERT_EQ(countUS, eventCount);
      auto countL = branchL->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufL);
      ASSERT_EQ(countL, eventCount);
      auto countUL = branchUL->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufUL);
      ASSERT_EQ(countUL, eventCount);
      auto countF = branchF->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufF);
      ASSERT_EQ(countF, eventCount);
      auto countD = branchD->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufD);
      ASSERT_EQ(countD, eventCount);
      auto countC = branchC->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufC);
      ASSERT_EQ(countC, eventCount);
      auto countB = branchB->GetBulkRead().GetEntriesSerialized(evt_idx, branchbufB);
      ASSERT_EQ(countB, eventCount);
      events = events > eventCount ? (events - eventCount) : 0;

      int *entryI = reinterpret_cast<int *>(branchbufI.GetCurrent());
      unsigned int *entryUI = reinterpret_cast<unsigned int *>(branchbufUI.GetCurrent());
      short *entryS = reinterpret_cast<short *>(branchbufS.GetCurrent());
      unsigned short *entryUS = reinterpret_cast<unsigned short *>(branchbufUS.GetCurrent());
      long long int *entryL = reinterpret_cast<long long int *>(branchbufL.GetCurrent());
      unsigned long long int *entryUL = reinterpret_cast<unsigned long long int *>(branchbufUL.GetCurrent());
      float *entryF = reinterpret_cast<float *>(branchbufF.GetCurrent());
      double *entryD = reinterpret_cast<double *>(branchbufD.GetCurrent());
      char *entryC = reinterpret_cast<char *>(branchbufC.GetCurrent());
      bool *entryB = reinterpret_cast<bool *>(branchbufB.GetCurrent());
      for (Int_t idx=0; idx < eventCount; idx++) {
         Int_t tmpI = *reinterpret_cast<Int_t*>(&entryI[idx]);
         UInt_t tmpUI = *reinterpret_cast<UInt_t*>(&entryUI[idx]);
         Short_t tmpS = *reinterpret_cast<Short_t*>(&entryS[idx]);
         UShort_t tmpUS = *reinterpret_cast<UShort_t*>(&entryUS[idx]);
         Long64_t tmpL = *reinterpret_cast<Long64_t*>(&entryL[idx]);
         ULong64_t tmpUL = *reinterpret_cast<ULong64_t*>(&entryUL[idx]);
         float_t tmpF = *reinterpret_cast<Float_t*>(&entryF[idx]);
         Double_t tmpD = *reinterpret_cast<Double_t*>(&entryD[idx]);
         Char_t tmpC = *reinterpret_cast<Char_t*>(&entryC[idx]);
         Bool_t tmpB = *reinterpret_cast<Bool_t*>(&entryB[idx]);
         char *tmp_ptrI = reinterpret_cast<char *>(&tmpI);
         char *tmp_ptrUI = reinterpret_cast<char *>(&tmpUI);
         char *tmp_ptrS = reinterpret_cast<char *>(&tmpS);
         char *tmp_ptrUS = reinterpret_cast<char *>(&tmpUS);
         char *tmp_ptrL = reinterpret_cast<char *>(&tmpL);
         char *tmp_ptrUL = reinterpret_cast<char *>(&tmpUL);
         char *tmp_ptrF = reinterpret_cast<char *>(&tmpF);
         char *tmp_ptrD = reinterpret_cast<char *>(&tmpD);
         char *tmp_ptrC = reinterpret_cast<char *>(&tmpC);
         char *tmp_ptrB = reinterpret_cast<char *>(&tmpB);
         int valI;
         unsigned int valUI;
         short valS;
         unsigned short valUS;
         long long int valL;
         unsigned long long int valUL;
         float valF;
         double valD;
         char valC;
         bool valB;
         frombuf(tmp_ptrI, &valI);
         EXPECT_EQ(valI, expectedI);
         frombuf(tmp_ptrUI, &valUI);
         EXPECT_EQ(valUI, expectedUI);
         frombuf(tmp_ptrS, &valS);
         EXPECT_EQ(valS, expectedS);
         frombuf(tmp_ptrUS, &valUS);
         EXPECT_EQ(valUS, expectedUS);
         frombuf(tmp_ptrL, &valL);
         EXPECT_EQ(valL, expectedL);
         frombuf(tmp_ptrUL, &valUL);
         EXPECT_EQ(valUL, expectedUL);
         frombuf(tmp_ptrF, &valF);
         EXPECT_EQ(valF, expectedF);
         frombuf(tmp_ptrD, &valD);
         EXPECT_EQ(valD, expectedD);
         frombuf(tmp_ptrC, &valC);
         EXPECT_EQ(valC, expectedC);
         frombuf(tmp_ptrB, &valB);
         EXPECT_EQ(valB, expectedB);
         expectedI++;
         expectedUI++;
         expectedS++;
         expectedUS++;
         expectedL++;
         expectedUL++;
         expectedF++;
         expectedD++;
         expectedC++;
         expectedB = !expectedB;
         evt_idx++;
      }
   }
}

#endif // R__USE_IMT

#endif // R__B64
