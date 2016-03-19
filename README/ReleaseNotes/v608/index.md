% ROOT Version 6.08 Release Notes
% 2015-11-12
<a name="TopOfPage"></a>

## Introduction

ROOT version 6.08/00 is scheduled for release in May, 2016.

For more information, see:

[http://root.cern.ch](http://root.cern.ch)

The following people have contributed to this new version:

 David Abdurachmanov, CERN, CMS,\
 Bertrand Bellenot, CERN/SFT,\
 Rene Brun, CERN/SFT,\
 Philippe Canal, Fermilab,\
 Cristina Cristescu, CERN/SFT,\
 Olivier Couet, CERN/SFT,\
 Gerri Ganis, CERN/SFT,\
 Andrei Gheata, CERN/SFT,\
 Christopher Jones, Fermilab, CMS,\
 Wim Lavrijsen, LBNL, PyRoot,\
 Sergey Linev, GSI, http,\
 Pere Mato, CERN/SFT,\
 Lorenzo Moneta, CERN/SFT,\
 Axel Naumann, CERN/SFT,\
 Danilo Piparo, CERN/SFT,\
 Timur Pocheptsov, CERN/SFT,\
 Fons Rademakers, CERN/IT,\
 Paul Russo, Fermilab,\
 Enric Tejedor Saavedra, CERN/SFT,\
 Liza Sakellari, CERN/SFT,\
 Manuel Tobias Schiller,\
 David Smith, CERN/IT,\
 Matevz Tadel, UCSD/CMS, Eve,\
 Vassil Vassilev, Fermilab/CMS,\
 Wouter Verkerke, NIKHEF/Atlas, RooFit

<a name="core-libs"></a>

## Core Libraries

ROOT prepares for [cxx modules](http://clang.llvm.org/docs/Modules.html). One of
the first requirements is its header files to be self-contained (section "Missing
Includes"). ROOT header files were cleaned up from extra includes and the missing
includes were added.

This could be considered as backward incompatibility (for good however). User
code may need to add extra includes, which were previously resolved indirectly
by including a ROOT header. For example:

  * TBuffer.h - TObject.h doesn't include TBuffer.h anymore. Third party code,
    replying on the definition of TBufer will need to include TBuffer.h, along
    with TObject.h.
  * TSystem.h - for some uses of gSystem.
  * GeneticMinimizer.h
  * ...

Other improvements, which may cause compilation errors in third party code:
  * If you get std::type_info from Rtypeinfo.h, type_info should be spelled
    `std::type_info`.

Also:
  * `TPluginManager` was made thread-safe [ROOT-7927].
  * On MacOSX, backtraces are now generated without external tools [ROOT-6667].

### Containers

* A pseudo-container (generator) was created, ROOT::TSeq<T>. This template is inspired by the xrange built-in function of Python. See the example [here](https://root.cern.ch/doc/master/cnt001__basictseq_8C.html).

### Meta Library

Add a new mode for `TClass::SetCanSplit` (2) which indicates that this class and any derived class should not be split.  This included a rework the mechanism checking the base classes.  Instead of using `InheritsFrom`, which lead in some cases, including the case where the class derived from an STL collection, to spurrious autoparsing (to look at the base class of the collection!), we use a custom walk through the tree of base classes that checks their value of `fCanSplit`.  This also has the side-effect of allowing the extension of the concept 'base class that prevent its derived class from being split' to any user class.  This fixes [ROOT-7972].


### Dictionaries

* Fix ROOT-7760: Fully allow the usage of the dylib extension on OSx.
* Fix ROOT-7879: Prevent LinkDef files to be listed in a rootmap file and use (as the user actually expects) the header files #included in the linkdef file, if any, as the top level headers.
* Add the *noIncludePaths* switch both for rootcling and genreflex to allow to loose track of the include paths in input to the dictionary generator.

### Interpreter Library

* Exceptions are now caught in the interactive ROOT session, instead of terminating ROOT.
* A ValuePrinter for tuple and pair has been added to visualise the content of these entities at the prompt.

## Parallelisation

* Three methods have been added to manage implicit multi-threading in ROOT: `ROOT::EnableImplicitMT(numthreads)`, `ROOT::DisableImplicitMT` and `ROOT::IsImplicitMTEnabled`. They can be used to enable, disable and check the status of the global implicit multi-threading in ROOT, respectively.
* Even if the default reduce function specified in the invocation of the `MapReduce` method of `TProcPool` returns a pointer to a `TObject`, the return value of `MapReduce` is properly casted to the type returned by the map function.
* Add a new class named `TThreadedObject` which helps making objects thread private and merging them.
* Add tutorial showing how to fill randomly histograms using the `TProcPool` class.
* Add tutorial showing how to fill randomly histograms from multiple threads.

## I/O Libraries

* Custom streamers need to #include TBuffer.h explicitly (see [section Core Libraries](#core-libs))
* Check and flag short reads as errors in the xroot plugins. This fixes [ROOT-3341].
* Added support for AWS temporary security credentials to TS3WebFile by allowing the security token to be given.
* Resolve an issue when space is freed in a large `ROOT` file and a TDirectory is updated and stored the lower (less than 2GB) freed portion of the file [ROOT-8055].


## TTree Libraries

* Do not automatically setup read cache during TTree::Fill(). This fixes [ROOT-8031].

### Fast Cloning

We added a cache specifically for the fast option of the TTreeCloner to significantly reduce the run-time when fast-cloning remote files to address [ROOT-5078].  It can be controlled from the `TTreeCloner`, `TTree::CopyEntries` or `hadd` interfaces.  The new cache is enabled by default, to update the size of the cache or disable it from `TTreeCloner` use: `TTreeCloner::SetCacheSize`.  To do the same from `TTree::CopyEntries` add to the option string "cachesize=SIZE".  To update the size of the cache or disable it from `hadd`, use the command line option `-cachesize SIZE`.  `SIZE` shouyld be given in number bytes and can be expressed in 'human readable form' (number followed by size unit like MB, MiB, GB or GiB, etc. or SIZE  can be set zero to disable the cache.

### Other Changes

* Update `TChain::LoadTree` so that the user call back routine is actually called for each input file even those containing `TTree` objects with no entries.
* Repair setting the branch address of a leaflist style branch taking directly the address of the struct.  (Note that leaflist is nonetheless still deprecated and declaring the struct to the interpreter and passing the object directly to create the branch is much better).
* Provide an implicitly parallel implementation of `TTree::GetEntry`. The approach is based on creating a task per top-level branch in order to do the reading, unzipping and deserialisation in parallel. In addition, a getter and a setter methods are provided to check the status and enable/disable implicit multi-threading for that tree (see Parallelisation section for more information about implicit multi-threading).
* Properly support std::cin (and other stream that can not be rewound) in `TTree::ReadStream`. This fixes [ROOT-7588].
* Prevent `TTreeCloner::CopyStreamerInfos()` from causing an autoparse on an abstract base class.

## Histogram Libraries

* TH2Poly has a functional Merge method.
* Implemented the `TGraphAsymmErrors` constructor directly from an ASCII file.

## Math Libraries


## RooFit Libraries


## 2D Graphics Libraries

* In `TColor::SetPalette`, make sure the high quality palettes are defined
  only ones taking care of transparency. Also `CreateGradientColorTable` has been
  simplified.
* New fast constructor for `TColor` avoiding to call `gROOT->GetColor()`. The
  normal constructor generated a big slow down when creating a Palette with
  `CreateGradientColorTable`.
* In `CreateGradientColorTable` we do not need anymore to compute the highest
  color index.
* In `TGraphPainter`, when graphs are painted with lines, they are split into
  chunks of length `fgMaxPointsPerLine`. This allows to paint line with an "infinite"
  number of points. In some case this "chunks painting" technic may create artefacts
  at the chunk's boundaries. For instance when zooming deeply in a PDF file. To avoid
  this effect it might be necessary to increase the chunks' size using the new function:
  `TGraphPainter::SetMaxPointsPerLine(20000)`.
* When using line styles different from 1 (continuous line), the behavior of TArrow
  was suboptimal. The problem was that the line style is also applied to the arrow
  head, which is usually not what one wants.
  The arrow tip is now drawn using a continuous line.
* It is now possible to select an histogram on a canvas by clicking on the vertical
  lines of the bins boundaries.
  This problem was reported [here](https://sft.its.cern.ch/jira/browse/ROOT-6649).
* When using time format in axis, `TGaxis::PaintAxis()` may in some cases call
  `strftime()` with invalid parameter causing a crash.
  This problem was reported [here](https://sft.its.cern.ch/jira/browse/ROOT-7689).
* Having "X11.UseXft: yes" activated in .rootrc file and running
  [this](https://sft.its.cern.ch/jira/browse/ROOT-7985) little program,
  resulted in a crash.
* Ease the setting of the appearance of joining lines for PostScript and PDF
  output. [TPostScript::SetLineJoin](https://root.cern.ch/doc/master/classTPostScript.html#ae4917bab9cc6b11fdc88478be03367d1)
  allowed to set the line joining style for PostScript files. But the setting this
  parameter implied to create a `TPostScript` object. Now a `TStyle` setting has been
  implemented and it is enough to do:
  ~~~ {.cpp}
  gStyle->SetLineJoinPS(2);
  ~~~
  Also this setting is now active for PDF output.
  This enhancement was triggered by [this forum question](https://root.cern.ch/phpBB3/viewtopic.php?f=3&t=21077).
* Make sure the palette axis title is correct after a histogram cloning. This
  problem was mentioned [here](https://sft.its.cern.ch/jira/browse/ROOT-8007).
* `TASImage` When the first or last point of a wide line is exactly on the
  window limit the line is drawn vertically or horizontally.
  This problem was mentioned [here](https://sft.its.cern.ch/jira/browse/ROOT-8021)
* Make sure that `TLatex` text strings containing "\" (ie: rendered using `TMathText`)
  produce an output in PDF et SVG files.

## 3D Graphics Libraries

* When painting a `TH3` as 3D boxes, `TMarker3DBox` ignored the max and min values
  specified by `SetMaximum()` and `SetMinimum()`.

## New histogram drawing options

* COL2 is a new rendering technique providing potential performance improvements
  compared to the standard COL option. The performance comparison of the COL2 to
  the COL option depends on the histogram and the size of the rendering region in
  the current pad. In general, a small (approx. less than 100 bins per axis),
  sparsely populated TH2 will render faster with the COL option.

  However, for larger histograms (approx. more than 100 bins per axis) that are
  not sparse, the COL2 option will provide up to 20 times performance improvements.
  For example, a 1000x1000 bin TH2 that is not sparse will render an order of
  magnitude faster with the COL2 option.

  The COL2 option will also scale its performance based on the size of the pixmap
  the histogram image is being rendered into. It also is much better optimized for
  sessions where the user is forwarding X11 windows through an `ssh` connection.

  For the most part, the COL2 and COLZ2 options are a drop in replacement to the COL
  and COLZ options. There is one major difference and that concerns the treatment of
  bins with zero content. The COL2 and COLZ2 options color these bins the color of zero.

  This has been implemented by Jeromy Tompkins <Tompkins@nscl.msu.edu>

## Geometry Libraries


## Database Libraries

* Fix `TPgSQLStatement::SetBinary` to actually handle binary data (previous limited to ascii).

## Networking Libraries


## GUI Libraries


## Montecarlo Libraries


## PROOF Libraries


## Language Bindings

### PyROOT
  * Added a new configuration option to disable processing of the rootlogon[.py|C] macro in addition
    ro the -n option in the command arguments. To disable processing the rootlogon do the following
    before any other command that will trigger initialization:
    ```
    >>> import ROOT
    >>> ROOT.PyConfig.DisableRootLogon = True
    >>> ...
    ```

### Notebook integration

  * Refactoring of the Jupyter integration layer into the new package JupyROOT.
  * Added ROOT [Jupyter Kernel for ROOT](https://root.cern.ch/root-has-its-jupyter-kernel)
    * Magics are now invoked with standard syntax "%%", for example "%%cpp".
    * The methods "toCpp" and "toPython" have been removed.
  * Factorise output capturing and execution in an accelerator library and use ctypes to invoke functions.
  * When the ROOT kernel is used, the output is consumed progressively
  * Capture unlimited output also when using an IPython Kernel (fixes ROOT-7960)

## JavaScript ROOT


## Tutorials


## Class Reference Guide

## Build, Configuration and Testing Infrastructure
* Added new 'builtin_vc' option to bundle a version of Vc within ROOT.
  The default is OFF, however if the Vc package is not found in the system the option is switched to
  ON if the option 'vc' option is ON.



