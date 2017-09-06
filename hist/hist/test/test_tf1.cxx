#include "TF1.h"
#include "TObjString.h"
// #include "TObject.h"

#include "gtest/gtest.h"

#include <iostream>

using namespace std;

Float_t delta = 0.00000000001;

void coeffNamesGeneric(TString &formula, TObjArray *coeffNames)
{
   TF1 cn0("cn0", formula, 0, 1);
   ASSERT_EQ(cn0.GetNpar(), coeffNames->GetEntries());
   for (int i = 0; i < coeffNames->GetEntries(); i++) {
      TObjString *coeffObj = (TObjString *)coeffNames->At(i);
      TString coeffName = coeffObj->GetString();
      EXPECT_EQ(coeffName, TString(cn0.GetParName(i)));
   }
}

// Test that the NSUM names are copied correctly
void test_nsumCoeffNames()
{
   TObjArray *coeffNames = new TObjArray();
   coeffNames->SetOwner(kTRUE);
   coeffNames->Add(new TObjString("sg"));
   coeffNames->Add(new TObjString("bg"));
   coeffNames->Add(new TObjString("Mean"));
   coeffNames->Add(new TObjString("Sigma"));
   coeffNames->Add(new TObjString("Slope"));
   TString formula("NSUM([sg] * gaus, [bg] * expo)");

   coeffNamesGeneric(formula, coeffNames);

   delete coeffNames;
}

// Test that the NSUM is normalized as we'd expect
void test_normalization() {
   double xmin = -5;
   double xmax = 5;
   TF1 n0("n0", "NSUM(.5 * gaus, .5 * (x+[0])**2)", xmin, xmax);
   EXPECT_NEAR(n0.Integral(xmin, xmax), 1, delta);
   n0.SetParameter(4, 1); // should not affect integral
   EXPECT_NEAR(n0.Integral(xmin, xmax), 1, delta);
   n0.SetParameter(0, 0);
   EXPECT_NEAR(n0.Integral(xmin, xmax), .5, delta);

   TF1 n1("n1", "NSUM([sg] * gaus, [bg] * (x+[0])**2)", xmin, xmax);
   n1.SetParameter(0, .5);
   n1.SetParameter(1, .5);
   EXPECT_NEAR(n1.Integral(xmin, xmax), 1, delta);
   n0.SetParameter(0, 0);
   EXPECT_NEAR(n0.Integral(xmin, xmax), .5, delta);

   TF1 n2("n2", "NSUM([sg] * gaus, -0.5 * (x+[0])**2)", xmin, xmax);
   n2.SetParameter(0, .5);
   EXPECT_NEAR(n2.GetParameter(1), -.5, delta);
   EXPECT_NEAR(n2.Integral(xmin, xmax), 0, delta);
   n2.SetParameter(0, 0);
   EXPECT_NEAR(n2.Integral(xmin, xmax), -.5, delta);
}

void voigtHelper(double sigma, double lg)
{
   TF1 lor("lor", "breitwigner", -20, 20);
   lor.SetParameters(1, 0, lg);
   TF1 mygausn("mygausn", "gausn", -20, 20);
   mygausn.SetParameters(1, 0, sigma);

   TF1 conv("conv", "CONV(lor, mygausn)", -20, 20);

   // Voigt should just be the convolution of the gaussian and lorentzian
   TF1 myvoigt("myvoigt", "TMath::Voigt(x, [0], [1])", -20, 20);
   myvoigt.SetParameters(sigma, lg);

   for (double x = -19.5; x < 20; x += .5)
      EXPECT_NEAR(conv.Eval(x), myvoigt.Eval(x), .01 * conv.Eval(x));
}

// Test that the voigt can be expressed as a convolution of a gaussian and lorentzian
// Check that the values match to within 1%
void test_convVoigt()
{
   voigtHelper(.1, 1);
   voigtHelper(1, .1);
   voigtHelper(1, 1);
}

TEST(TF1, NsumCoeffNames)
{
   test_nsumCoeffNames();
}

TEST(TF1, Normalization)
{
   test_normalization();
}

TEST(TF1, ConvVoigt)
{
   test_convVoigt();
}
