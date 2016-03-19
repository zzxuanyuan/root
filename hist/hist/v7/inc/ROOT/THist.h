/// \file ROOT/THist.h
/// \ingroup Hist ROOT7
/// \author Axel Naumann <axel@cern.ch>
/// \date 2015-03-23
/// \warning This is part of the ROOT 7 prototype! It will change without notice. It might trigger earthquakes. Feedback is welcome!

/*************************************************************************
 * Copyright (C) 1995-2015, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT7_THist
#define ROOT7_THist

#include "ROOT/RArrayView.h"
#include "ROOT/TAxis.h"
#include "ROOT/TDrawable.h"
#include "ROOT/THistBinIter.h"
#include "ROOT/THistDrawable.h"
#include "ROOT/THistImpl.h"
#include "ROOT/THistStats.h"
#include <initializer_list>

namespace ROOT {
namespace Experimental {

// fwd declare for fwd declare for friend declaration in THist...
template<int DIMENSIONS, class PRECISION, class STATISTICS>
class THist;

// fwd declare for friend declaration in THist.
template<int DIMENSIONS, class PRECISION, class STATISTICS>
class THist<DIMENSIONS, PRECISION, STATISTICS>
  HistFromImpl(
  std::unique_ptr<typename THist<DIMENSIONS, PRECISION, STATISTICS>::ImplBase_t> pHistImpl);

template<int DIMENSIONS, class PRECISION, class STATISTICS>
void swap(THist<DIMENSIONS, PRECISION, STATISTICS> &a,
          THist<DIMENSIONS, PRECISION, STATISTICS> &b) noexcept;


/**
 \class THist
 Histogram class for histograms with `DIMENSIONS` dimensions, where each bin
 count is stored by a value of type `PRECISION`. STATISTICS stores statistical
 data of the entries filled into the histogram (uncertainties etc). It defaults
 to THistStatEntries for integer PRECISION and THistStatUncertainty for others,
 under the assumption that integer bin content is rarely filles with weights.

 A histogram counts occurrences of values or n-dimensional combinations thereof.
 Contrary to for instance a `TTree`, a histogram combines adjacent values. The
 resolution of this combination is defined by the binning, see e.g.
 http://www.wikiwand.com/en/Histogram
 */

template<int DIMENSIONS, class PRECISION,
         class STATISTICS
         = typename std::conditional<std::is_integral<PRECISION>::value,
           THistStatEntries<DIMENSIONS, PRECISION>,
           THistStatUncertainty<DIMENSIONS, PRECISION>>::type>
class THist {
public:
  /// The type of the `Detail::THistImplBase` of this histogram.
  using ImplBase_t = Detail::THistImplBase<DIMENSIONS, PRECISION, STATISTICS>;
  /// The coordinates type: a `DIMENSIONS`-dimensional `std::array` of `double`.
  using Coord_t = typename ImplBase_t::Coord_t;
  /// The type of weights
  using Weight_t = PRECISION;
  /// Statistics type
  using Stat_t = STATISTICS;
  /// Pointer type to `HistImpl_t::Fill`, for faster access.
  using FillFunc_t = typename ImplBase_t::FillFunc_t;

  using const_iterator = THistBinIter<ImplBase_t>;

  THist() = default;
  THist(THist&&) = default;

  /// Create a histogram from an `array` of axes (`TAxisConfig`s) and possibly
  /// an initial `STATISTICS` object. The latter is usually just fine when
  /// not passed (i.e. default-constructed). Example code:
  ///
  /// Construct a 1-dimensional histogram that can be filled with `floats`s.
  /// The axis has 10 bins between 0. and 1. The two outermost sets of curly
  /// braces are to reach the initialization of the `std::array` elements; the
  /// inner one is for the initialization of a `TAxisCoordinate`.
  ///
  ///     THist<1,float> h1f({{ {10, 0., 1.} }});
  ///
  /// Construct a 2-dimensional histogram, with the first axis as before, and
  /// the second axis having non-uniform ("irregular") binning, where all bin-
  /// edges are specified. As this is itself an array it must be enclosed by
  /// double curlies.
  ///
  ///     THist<2,int> h2i({{ {10, 0., 1.}, {{-1., 0., 1., 10., 100.}} }});
  explicit THist(std::array<TAxisConfig, DIMENSIONS> axes);

  /// Constructor overload taking the histogram title
  THist(std::string_view histTitle, std::array<TAxisConfig, DIMENSIONS> axes);

  /// Constructor overload that's only available for DIMENSIONS == 1.
  template<class = typename std::enable_if<DIMENSIONS == 1>>
  THist(const TAxisConfig &xaxis):
    THist(std::array<TAxisConfig, 1>{{xaxis}}) { }

  /// Constructor overload that's only available for DIMENSIONS == 1, also
  /// passing the histogram title.
  template<class = typename std::enable_if<DIMENSIONS == 1>>
  THist(std::string_view histTitle, const TAxisConfig &xaxis):
    THist(histTitle, std::array<TAxisConfig, 1>{{xaxis}}) { }

  /// Constructor overload that's only available for DIMENSIONS == 2.
  template<class = typename std::enable_if<DIMENSIONS == 2>>
  THist(const TAxisConfig &xaxis, const TAxisConfig &yaxis):
    THist(std::array<TAxisConfig, 2>{{xaxis, yaxis}}) { }

  /// Constructor overload that's only available for DIMENSIONS == 2, also
  /// passing histogram and axis titles.
  template<class = typename std::enable_if<DIMENSIONS == 2>>
  THist(std::string_view histTitle, const TAxisConfig &xaxis, const TAxisConfig &yaxis):
    THist(histTitle, std::array<TAxisConfig, 2>{{xaxis, yaxis}}) { }

  /// Constructor overload that's only available for DIMENSIONS == 3.
  template<class = typename std::enable_if<DIMENSIONS == 3>>
  THist(const TAxisConfig &xaxis, const TAxisConfig &yaxis,
        const TAxisConfig &zaxis):
    THist(std::array<TAxisConfig, 3>{{xaxis, yaxis, zaxis}}) { }


  template<class = typename std::enable_if<DIMENSIONS == 3>>
  THist(std::string_view histTitle,
        const TAxisConfig &xaxis, const TAxisConfig &yaxis,
        const TAxisConfig &zaxis):
    THist(histTitle, std::array<TAxisConfig, 3>{{xaxis, yaxis, zaxis}}) { }


  /// Number of dimensions of the coordinates
  static constexpr int GetNDim() noexcept { return DIMENSIONS; }

  /// Access the ImplBase_t this THist points to.
  ImplBase_t *GetImpl() const noexcept { return fImpl.get(); }

  /// Add `weight` to the bin containing coordinate `x`.
  void Fill(const Coord_t &x, Weight_t weight = (Weight_t) 1) noexcept {
    (fImpl.get()->*fFillFunc)(x, weight);
  }

  /// For each coordinate in `xN`, add `weightN[i]` to the bin at coordinate
  /// `xN[i]`. The sizes of `xN` and `weightN` must be the same. This is more
  /// efficient than many separate calls to `Fill()`.
  void FillN(const std::array_view <Coord_t> xN,
             const std::array_view <Weight_t> weightN) noexcept {
    fImpl->FillN(xN, weightN);
  }

  /// Convenience overload: `FillN()` with weight 1.
  void FillN(const std::array_view <Coord_t> xN) noexcept {
    fImpl->FillN(xN);
  }

  /// Get the number of entries this histogram was filled with.
  int64_t GetEntries() const noexcept { return fImpl->GetStat().GetEntries(); }

  /// Get the content of the bin at `x`.
  Weight_t GetBinContent(const Coord_t &x) const {
    return fImpl->GetBinContent(x);
  }

  /// Get the uncertainty on the content of the bin at `x`.
  Weight_t GetBinUncertainty(const Coord_t &x) const {
    return fImpl->GetBinUncertainty(x);
  }

  const_iterator begin() const { return const_iterator(*fImpl); }

  const_iterator end() const { return const_iterator(*fImpl, fImpl->GetNBins()); }

private:
  std::unique_ptr<ImplBase_t> fImpl; ///< The actual histogram implementation
  FillFunc_t fFillFunc = nullptr; ///< Pinter to THistImpl::Fill() member function

  friend THist HistFromImpl<>(std::unique_ptr<ImplBase_t>);
  friend void swap<>(THist<DIMENSIONS, PRECISION> &a,
                     THist<DIMENSIONS, PRECISION> &b) noexcept;

};

/// Swap two histograms.
///
/// Very efficient; swaps the `fImpl` pointers.
template<int DIMENSIONS, class PRECISION, class STATISTICS>
void swap(THist<DIMENSIONS, PRECISION, STATISTICS> &a,
          THist<DIMENSIONS, PRECISION, STATISTICS> &b) noexcept {
  std::swap(a.fImpl, b.fImpl);
  std::swap(a.fFillFunc, b.fFillFunc);
};


/// Adopt an external, stand-alone THistImpl. The THist will take ownership.
template<int DIMENSIONS, class PRECISION, class STATISTICS>
THist<DIMENSIONS, PRECISION, STATISTICS>
HistFromImpl(
  std::unique_ptr<typename THist<DIMENSIONS, PRECISION, STATISTICS>::ImplBase_t> pHistImpl) {
  THist<DIMENSIONS, PRECISION, STATISTICS> ret;
  ret.fFillFunc = pHistImpl->GetFillFunc();
  std::swap(ret.fImpl, pHistImpl);
  return ret;
};


namespace Internal {
/**
 Generate THist::fImpl from THist constructor arguments.
 */
template<int DIMENSIONS, int IDIM, class PRECISION, class STATISTICS,
  class... PROCESSEDAXISCONFIG>
struct HistImplGen_t {
  /// Select the template argument for the next axis type, and "recurse" into
  /// HistImplGen_t for the next axis.
  template<TAxisConfig::EKind KIND>
  std::unique_ptr<Detail::THistImplBase<DIMENSIONS, PRECISION, STATISTICS>>
  MakeNextAxis(std::string_view title,
               const std::array<TAxisConfig, DIMENSIONS> &axes,
               PROCESSEDAXISCONFIG... processedAxisArgs) {
    typename AxisConfigToType<KIND>::Axis_t nextAxis
      = AxisConfigToType<KIND>()(axes[IDIM]);
    return HistImplGen_t<DIMENSIONS, IDIM + 1, PRECISION, STATISTICS,
      PROCESSEDAXISCONFIG..., typename AxisConfigToType<KIND>::Axis_t>()
      (title, axes, processedAxisArgs..., nextAxis);
  }

  /// Make a THistImpl-derived object reflecting the TAxisConfig array.
  ///
  /// Delegate to the appropriate MakeNextAxis instantiation, depending on the
  /// axis type selected in the TAxisConfig.
  /// \param axes - `TAxisConfig` objects describing the axis of the resulting
  ///   THistImpl.
  /// \param statConfig - the statConfig parameter to be passed to the THistImpl
  /// \param processedAxisArgs - the TAxisBase-derived axis objects describing the
  ///   axes of the resulting THistImpl. There are `IDIM` of those; in the end
  /// (`IDIM` == `DIMENSIONS`), all `axes` have been converted to
  /// `processedAxisArgs` and the THistImpl constructor can be invoked, passing
  /// the `processedAxisArgs`.
  std::unique_ptr<Detail::THistImplBase<DIMENSIONS, PRECISION, STATISTICS>>
  operator()(std::string_view title,
             const std::array <TAxisConfig, DIMENSIONS> &axes,
             PROCESSEDAXISCONFIG... processedAxisArgs) {
    switch (axes[IDIM].GetKind()) {
      case TAxisConfig::kEquidistant:
        return MakeNextAxis<TAxisConfig::kEquidistant>(title, axes,
                                                       processedAxisArgs...);
      case TAxisConfig::kGrow:
        return MakeNextAxis<TAxisConfig::kGrow>(title, axes,
                                                processedAxisArgs...);
      case TAxisConfig::kIrregular:
        return MakeNextAxis<TAxisConfig::kIrregular>(title, axes,
                                                     processedAxisArgs...);
      default:
        R__ERROR_HERE("HIST") << "Unhandled axis kind";
    }
    return nullptr;
  }
};

/// Generate THist::fImpl from constructor arguments; recursion end.
template<int DIMENSIONS, class PRECISION, class STATISTICS,
  class... PROCESSEDAXISCONFIG>
/// Create the histogram, now that all axis types and initializier objects are
/// determined.
struct HistImplGen_t<DIMENSIONS, DIMENSIONS, PRECISION, STATISTICS,
  PROCESSEDAXISCONFIG...> {
  std::unique_ptr <Detail::THistImplBase<DIMENSIONS, PRECISION, STATISTICS>>
  operator()(std::string_view title, const std::array<TAxisConfig, DIMENSIONS> &,
             PROCESSEDAXISCONFIG... axisArgs) {
    using HistImplt_t
    = Detail::THistImpl<DIMENSIONS, PRECISION, STATISTICS, PROCESSEDAXISCONFIG...>;
    return std::make_unique<HistImplt_t>(title, axisArgs...);
  }
};
} // namespace Internal


template<int DIMENSIONS, class PRECISION, class STATISTICS>
THist<DIMENSIONS, PRECISION, STATISTICS>
::THist(std::array<TAxisConfig, DIMENSIONS> axes):
  fImpl{std::move(
    Internal::HistImplGen_t<DIMENSIONS, 0, PRECISION, STATISTICS>()("", axes))},
  fFillFunc{} {
  fFillFunc = fImpl->GetFillFunc();
}

template<int DIMENSIONS, class PRECISION, class STATISTICS>
THist<DIMENSIONS, PRECISION, STATISTICS>
::THist(std::string_view title, std::array<TAxisConfig, DIMENSIONS> axes):
  fImpl{std::move(
    Internal::HistImplGen_t<DIMENSIONS, 0, PRECISION, STATISTICS>()(title, axes))},
  fFillFunc{} {
  fFillFunc = fImpl->GetFillFunc();
}

/// \name THist Typedefs
///\{ Convenience typedefs (ROOT6-compatible type names)

// Keep them as typedefs, to make sure old-style documentation tools can
// understand them.
typedef THist<1, double> TH1D;
typedef THist<1, float> TH1F;
typedef THist<1, char> TH1C;
typedef THist<1, int> TH1I;
typedef THist<1, int64_t> TH1LL;

typedef THist<2, double> TH2D;
typedef THist<2, float> TH2F;
typedef THist<2, char> TH2C;
typedef THist<2, int> TH2I;
typedef THist<2, int64_t> TH2LL;

typedef THist<3, double> TH3D;
typedef THist<3, float> TH3F;
typedef THist<3, char> TH3C;
typedef THist<3, int> TH3I;
typedef THist<3, int64_t> TH3LL;
///\}


template<int DIMENSION, class PRECISIONA, class PRECISIONB>
void Add(THist<DIMENSION, PRECISIONA> &to, THist<DIMENSION, PRECISIONB> &from) {
  using ImplTo_t = typename THist<DIMENSION, PRECISIONA>::ImplBase_t;
  using ImplFrom_t = typename THist<DIMENSION, PRECISIONB>::ImplBase_t;
  ImplTo_t *implTo = to.GetImpl();
  ImplFrom_t *implFrom = from.GetImpl();
  // TODO: move into THistImpl; the loop iteration should not go through virt interfaces!
  for (auto &&bin: from) {
    to.Fill(implFrom->GetBinCenter(*bin), implFrom->GetBinContent(*bin));
  }
};

template<int DIMENSION, class PRECISION, class STATISTICS>
std::unique_ptr <Internal::TDrawable>
GetDrawable(std::shared_ptr<THist<DIMENSION, PRECISION, STATISTICS>> hist,
            THistDrawOptions<DIMENSION> opts = {}) {
  return std::make_unique<Internal::THistDrawable<
    DIMENSION, PRECISION, STATISTICS>>(hist, opts);
}

template<int DIMENSION, class PRECISION, class STATISTICS>
std::unique_ptr <Internal::TDrawable>
GetDrawable(std::unique_ptr<THist<DIMENSION, PRECISION, STATISTICS>> hist,
            THistDrawOptions<DIMENSION> opts = {}) {
  return std::make_unique<Internal::THistDrawable<
    DIMENSION, PRECISION, STATISTICS>>(hist, opts);
}

} // namespace Experimental
} // namespace ROOT

#endif
