#include <TGraph.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RRootBulkDS.hxx>
#include <ROOT/TSeq.hxx>

#include <gtest/gtest.h>

#include <algorithm> // std::accumulate
#include <iostream>

using namespace ROOT;
using namespace ROOT::RDF;
using namespace ROOT::RDF::Experimental;

auto fileName0 = "RRootBulkDS_input_0.root";
auto fileGlob = "RRootBulkDS_input_*.root";
auto treeName = "t";

TEST(RRootBulkDS, GenerateData)
{
   int i = 0;
   // TODO: re-introduce multiple files so we can test globbing.
   for (auto &&fileName : {fileName0}) {
      RDataFrame tdf(10);
      tdf.Define("i", [&i]() { return i++; })
         .Snapshot<int32_t>(treeName, fileName, {"i"});
   }
}

TEST(RRootBulkDS, ColTypeNames)
{
   RRootBulkDS tds(treeName, fileGlob);
   tds.SetNSlots(1);

   auto colNames = tds.GetColumnNames();

   EXPECT_TRUE(tds.HasColumn("i"));
   EXPECT_FALSE(tds.HasColumn("bla"));

   EXPECT_STREQ("i", colNames[0].c_str());

   EXPECT_STREQ("Int_t", tds.GetTypeName("i").c_str());
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
   const auto nSlots = 1U;
   tds.SetNSlots(nSlots);
   tds.Initialise();
   auto vals = tds.GetColumnReaders<int>("i");
   tds.Initialise();
   auto ranges = tds.GetEntryRanges();
   auto slot = 0U;
   for (auto &&range : ranges) {
      tds.InitSlot(slot, range.first);
      for (auto i : ROOT::TSeq<int>(range.first, range.second)) {
         tds.SetEntry(slot, i);
         auto val = **vals[slot];
         EXPECT_EQ(i, val);
      }
      slot++;
   }
}

TEST(RRootBulkDS, ColumnReadersWrongType)
{
   RRootBulkDS tds(treeName, fileGlob);
   const auto nSlots = 1U;
   tds.SetNSlots(nSlots);
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

TEST(RRootBulkDS, FromARDFWithJitting)
{
   std::unique_ptr<RDataSource> tds(new RRootBulkDS(treeName, fileGlob));
   RDataFrame tdf(std::move(tds));
   auto max = tdf.Filter("i<6").Max("i");
   auto min = tdf.Define("j", "i").Filter("j>4").Min("j");

   EXPECT_DOUBLE_EQ(5., *max);
   EXPECT_DOUBLE_EQ(5., *min);
}

// NOW MT!-------------
#ifdef R__USE_IMT

TEST(RootBulkDS, DefineSlotMT)
{
   const auto nSlots = 4U;
   ROOT::EnableImplicitMT(nSlots);

   std::vector<unsigned int> ids(nSlots, 0u);
   std::unique_ptr<RDataSource> tds(new RRootBulkDS(treeName, fileGlob));
   RDataFrame d(std::move(tds));
   auto m = d.DefineSlot("x", [&](unsigned int slot) {
                ids[slot] = 1u;
                return 1;
             }).Max("x");
   EXPECT_EQ(1, *m); // just in case

   const auto nUsedSlots = std::accumulate(ids.begin(), ids.end(), 0u);
   EXPECT_GT(nUsedSlots, 0u);
   EXPECT_LE(nUsedSlots, nSlots);
}

TEST(RRootBulkDS, FromARDFMT)
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

TEST(TRootTDS, FromARDFWithJittingMT)
{
   std::unique_ptr<RDataSource> tds(new RRootBulkDS(treeName, fileGlob));
   RDataFrame tdf(std::move(tds));
   auto max = tdf.Filter("i<6").Max("i");
   auto min = tdf.Define("j", "i").Filter("j>4").Min("j");

   EXPECT_DOUBLE_EQ(5., *max);
   EXPECT_DOUBLE_EQ(5., *min);
}

#endif // R__USE_IMT

#endif // R__B64
