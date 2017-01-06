// @(#)root/test:$Id$
// Author: Rene Brun   19/01/97

////////////////////////////////////////////////////////////////////////
//
//             A simple example with a ROOT tree
//             =================================
//
//  This program creates :
//    - a ROOT file
//    - a tree
//  Additional arguments can be passed to the program to control the flow
//  of execution. (see comments describing the arguments in the code).
//      Event  nevent comp split fill
//  All arguments are optional. Default is:
//      Event  400      1    1     1
//
//  In this example, the tree consists of one single "super branch"
//  The statement ***tree->Branch("event", &event, 64000,split);*** below
//  will parse the structure described in Event.h and will make
//  a new branch for each data member of the class if split is set to 1.
//    - 9 branches corresponding to the basic types fType, fNtrack,fNseg,
//           fNvertex,fFlag,fTemperature,fMeasures,fMatrix,fClosesDistance.
//    - 3 branches corresponding to the members of the subobject EventHeader.
//    - one branch for each data member of the class Track of TClonesArray.
//    - one branch for the TRefArray of high Pt tracks
//    - one branch for the TRefArray of muon tracks
//    - one branch for the reference pointer to the last track
//    - one branch for the object fH (histogram of class TH1F).
//
//  if split = 0 only one single branch is created and the complete event
//  is serialized in one single buffer.
//  if split = -2 the event is split using the old TBranchObject mechanism
//  if split = -1 the event is streamed using the old TBranchObject mechanism
//  if split > 0  the event is split using the new TBranchElement mechanism.
//
//  if comp = 0 no compression at all.
//  if comp = 1 event is compressed.
//  if comp = 2 same as 1. In addition branches with floats in the TClonesArray
//                         are also compressed.
//  The 4th argument fill can be set to 0 if one wants to time
//     the percentage of time spent in creating the event structure and
//     not write the event in the file.
//  In this example, one loops over nevent events.
//  The branch "event" is created at the first event.
//  The branch address is set for all other events.
//  For each event, the event header is filled and ntrack tracks
//  are generated and added to the TClonesArray list.
//  For each event the event histogram is saved as well as the list
//  of all tracks.
//
//  The two TRefArray contain only references to the original tracks owned by
//  the TClonesArray fTracks.
//
//  The number of events can be given as the first argument to the program.
//  By default 400 events are generated.
//  The compression option can be activated/deactivated via the second argument.
//
//   ---Running/Linking instructions----
//  This program consists of the following files and procedures.
//    - Event.h event class description
//    - Event.C event class implementation
//    - MainEvent.C the main program to demo this class might be used (this file)
//    - EventCint.C  the CINT dictionary for the event and Track classes
//        this file is automatically generated by rootcint (see Makefile),
//        when the class definition in Event.h is modified.
//
//   ---Analyzing the Event.root file with the interactive root
//        example of a simple session
//   Root > TFile f("Event.root")
//   Root > T.Draw("fNtrack")   //histogram the number of tracks per event
//   Root > T.Draw("fPx")       //histogram fPx for all tracks in all events
//   Root > T.Draw("fXfirst:fYfirst","fNtrack>600")
//                              //scatter-plot for x versus y of first point of each track
//   Root > T.Draw("fH.GetRMS()")  //histogram of the RMS of the event histogram
//
//   Look also in the same directory at the following macros:
//     - eventa.C  an example how to read the tree
//     - eventb.C  how to read events conditionally
//
////////////////////////////////////////////////////////////////////////

#include <stdlib.h>

#include "Riostream.h"
#include "TROOT.h"
#include "TFile.h"
#include "TNetFile.h"
#include "TRandom.h"
#include "TTree.h"
#include "TBranch.h"
#include "TClonesArray.h"
#include "TStopwatch.h"

#include "Event.h"

using namespace std;

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char **argv)
{
//   ROOT::EnableImplicitMT(16);
   Int_t nevent = 400;     // by default create 400 events
   Int_t comp   = 1;       // by default file is compressed
   Int_t split  = 1;       // by default, split Event in sub branches
   Int_t write  = 1;       // by default the tree is filled
   Int_t hfill  = 0;       // by default histograms are not filled
   Int_t read   = 0;
   Int_t arg4   = 1;
   Int_t arg5   = 600;     //default number of tracks per event
   Int_t netf   = 0;
   Int_t punzip = 0;

   if (argc > 1)  nevent = atoi(argv[1]);
   if (argc > 2)  comp   = atoi(argv[2]);
   if (argc > 3)  split  = atoi(argv[3]);
   if (argc > 4)  arg4   = atoi(argv[4]);
   if (argc > 5)  arg5   = atoi(argv[5]);
   if (arg4 ==  0) { write = 0; hfill = 0; read = 1;}
   if (arg4 ==  1) { write = 1; hfill = 0;}
   if (arg4 ==  2) { write = 0; hfill = 0;}
   if (arg4 == 10) { write = 0; hfill = 1;}
   if (arg4 == 11) { write = 1; hfill = 1;}
   if (arg4 == 20) { write = 0; read  = 1;}  //read sequential
   if (arg4 == 21) { write = 0; read  = 1;  punzip = 1;}  //read sequential + parallel unzipping
   if (arg4 == 25) { write = 0; read  = 2;}  //read random
   if (arg4 >= 30) { netf  = 1; }            //use TNetFile
   if (arg4 == 30) { write = 0; read  = 1;}  //netfile + read sequential
   if (arg4 == 35) { write = 0; read  = 2;}  //netfile + read random
   if (arg4 == 36) { write = 1; }            //netfile + write sequential
   Int_t branchStyle = 1; //new style by default
   if (split < 0) {branchStyle = 0; split = -1-split;}

   TFile *hfile;
   TTree *tree;
   Event *event1 = 0;
   Event *event2 = 0;
   Event *event3 = 0;
   Event *event4 = 0;
   Event *event5 = 0;
   Event *event6 = 0;
   Event *event7 = 0;
   Event *event8 = 0;
   Event *event9 = 0;
   Event *event10 = 0;
   Event *event11 = 0;
   Event *event12 = 0;
   Event *event13 = 0;
   Event *event14 = 0;
   Event *event15 = 0;
   Event *event16 = 0;

   // Fill event, header and tracks with some random numbers
   //   Create a timer object to benchmark this loop
   TStopwatch timer;
   timer.Start();
   Long64_t nb = 0;
   Int_t ev;
   Int_t bufsize;
   Double_t told = 0;
   Double_t tnew = 0;
   Int_t printev = 100;
   if (arg5 < 100) printev = 1000;
   if (arg5 < 10)  printev = 10000;

//         Read case
   if (read) {
      if (netf) {
         hfile = new TNetFile("root://localhost/root/test/EventNet.root");
      } else
         hfile = new TFile("Event.root");
      tree = (TTree*)hfile->Get("T");
      TBranch *branch1 = tree->GetBranch("event1");
      branch1->SetAddress(&event1);
      TBranch *branch2 = tree->GetBranch("event2");
      branch2->SetAddress(&event2);
      TBranch *branch3 = tree->GetBranch("event3");
      branch3->SetAddress(&event3);
      TBranch *branch4 = tree->GetBranch("event4");
      branch4->SetAddress(&event4);
      TBranch *branch5 = tree->GetBranch("event5");
      branch5->SetAddress(&event5);
      TBranch *branch6 = tree->GetBranch("event6");
      branch6->SetAddress(&event6);
      TBranch *branch7 = tree->GetBranch("event7");
      branch7->SetAddress(&event7);
      TBranch *branch8 = tree->GetBranch("event8");
      branch8->SetAddress(&event8);
      TBranch *branch9 = tree->GetBranch("event9");
      branch9->SetAddress(&event9);
      TBranch *branch10 = tree->GetBranch("event10");
      branch10->SetAddress(&event10);
      TBranch *branch11 = tree->GetBranch("event11");
      branch11->SetAddress(&event11);
      TBranch *branch12 = tree->GetBranch("event12");
      branch12->SetAddress(&event12);
      TBranch *branch13 = tree->GetBranch("event13");
      branch13->SetAddress(&event13);
      TBranch *branch14 = tree->GetBranch("event14");
      branch14->SetAddress(&event14);
      TBranch *branch15 = tree->GetBranch("event15");
      branch15->SetAddress(&event15);
      TBranch *branch16 = tree->GetBranch("event16");
      branch16->SetAddress(&event16);
      Int_t nentries = (Int_t)tree->GetEntries();
      nevent = TMath::Min(nevent,nentries);
      if (read == 1) {  //read sequential
         //by setting the read cache to -1 we set it to the AutoFlush value when writing
         Int_t cachesize = -1;
         if (punzip) tree->SetParallelUnzip();
         tree->SetCacheSize(cachesize);
         tree->SetCacheLearnEntries(1); //one entry is sufficient to learn
         tree->SetCacheEntryRange(0,nevent);
         for (ev = 0; ev < nevent; ev++) {
            tree->LoadTree(ev);  //this call is required when using the cache
            if (ev%printev == 0) {
               tnew = timer.RealTime();
               printf("event:%d, rtime=%f s\n",ev,tnew-told);
               told=tnew;
               timer.Continue();
            }
            nb += tree->GetEntry(ev);        //read complete event in memory
         }
      } else {    //read random
         Int_t evrandom;
         for (ev = 0; ev < nevent; ev++) {
            if (ev%printev == 0) std::cout<<"event="<<ev<<std::endl;
            evrandom = Int_t(nevent*gRandom->Rndm());
            nb += tree->GetEntry(evrandom);  //read complete event in memory
         }
      }
   } else {
//         Write case
      // Create a new ROOT binary machine independent file.
      // Note that this file may contain any kind of ROOT objects, histograms,
      // pictures, graphics objects, detector geometries, tracks, events, etc..
      // This file is now becoming the current directory.
      if (netf) {
         hfile = new TNetFile("root://localhost/root/test/EventNet.root","RECREATE","TTree benchmark ROOT file");
      } else
         hfile = new TFile("Event.root","RECREATE","TTree benchmark ROOT file");
      hfile->SetCompressionLevel(comp);

     // Create histogram to show write_time in function of time
     Float_t curtime = -0.5;
     Int_t ntime = nevent/printev;
     TH1F *htime = new TH1F("htime","Real-Time to write versus time",ntime,0,ntime);
     HistogramManager *hm = 0;
     if (hfill) {
        TDirectory *hdir = new TDirectory("histograms", "all histograms");
        hm = new HistogramManager(hdir);
     }

     // Create a ROOT Tree and one superbranch
      tree = new TTree("T","An example of a ROOT tree");
      tree->SetAutoSave(1000000000); // autosave when 1 Gbyte written
      tree->SetCacheSize(10000000);  // set a 10 MBytes cache (useless when writing local files)
      bufsize = 64000;
      if (split)  bufsize /= 4;
      event1 = new Event();           // By setting the value, we own the pointer and must delete it.
      event2 = new Event();           // By setting the value, we own the pointer and must delete it.
      event3 = new Event();           // By setting the value, we own the pointer and must delete it.
      event4 = new Event();           // By setting the value, we own the pointer and must delete it.
      event5 = new Event();           // By setting the value, we own the pointer and must delete it.
      event6 = new Event();           // By setting the value, we own the pointer and must delete it.
      event7 = new Event();           // By setting the value, we own the pointer and must delete it.
      event8 = new Event();           // By setting the value, we own the pointer and must delete it.
      event9 = new Event();           // By setting the value, we own the pointer and must delete it.
      event10 = new Event();           // By setting the value, we own the pointer and must delete it.
      event11 = new Event();           // By setting the value, we own the pointer and must delete it.
      event12 = new Event();           // By setting the value, we own the pointer and must delete it.
      event13 = new Event();           // By setting the value, we own the pointer and must delete it.
      event14 = new Event();           // By setting the value, we own the pointer and must delete it.
      event15 = new Event();           // By setting the value, we own the pointer and must delete it.
      event16 = new Event();           // By setting the value, we own the pointer and must delete it.
      TTree::SetBranchStyle(branchStyle);
      TBranch *branch1 = tree->Branch("event1", &event1, bufsize,split);
      branch1->SetAutoDelete(kFALSE);
      TBranch *branch2 = tree->Branch("event2", &event2, bufsize,split);
      branch2->SetAutoDelete(kFALSE);
      TBranch *branch3 = tree->Branch("event3", &event3, bufsize,split);
      branch3->SetAutoDelete(kFALSE);
      TBranch *branch4 = tree->Branch("event4", &event4, bufsize,split);
      branch4->SetAutoDelete(kFALSE);
      TBranch *branch5 = tree->Branch("event5", &event5, bufsize,split);
      branch5->SetAutoDelete(kFALSE);
      TBranch *branch6 = tree->Branch("event6", &event6, bufsize,split);
      branch6->SetAutoDelete(kFALSE);
      TBranch *branch7 = tree->Branch("event7", &event7, bufsize,split);
      branch7->SetAutoDelete(kFALSE);
      TBranch *branch8 = tree->Branch("event8", &event8, bufsize,split);
      branch8->SetAutoDelete(kFALSE);
      TBranch *branch9 = tree->Branch("event9", &event9, bufsize,split);
      branch9->SetAutoDelete(kFALSE);
      TBranch *branch10 = tree->Branch("event10", &event10, bufsize,split);
      branch10->SetAutoDelete(kFALSE);
      TBranch *branch11 = tree->Branch("event11", &event11, bufsize,split);
      branch11->SetAutoDelete(kFALSE);
      TBranch *branch12 = tree->Branch("event12", &event12, bufsize,split);
      branch12->SetAutoDelete(kFALSE);
      TBranch *branch13 = tree->Branch("event13", &event13, bufsize,split);
      branch13->SetAutoDelete(kFALSE);
      TBranch *branch14 = tree->Branch("event14", &event14, bufsize,split);
      branch14->SetAutoDelete(kFALSE);
      TBranch *branch15 = tree->Branch("event15", &event15, bufsize,split);
      branch15->SetAutoDelete(kFALSE);
      TBranch *branch16 = tree->Branch("event16", &event16, bufsize,split);
      branch16->SetAutoDelete(kFALSE);
      if(split >= 0 && branchStyle) tree->BranchRef();
      Float_t ptmin = 1;

      for (ev = 0; ev < nevent; ev++) {
         if (ev%printev == 0) {
            tnew = timer.RealTime();
            printf("event:%d, rtime=%f s\n",ev,tnew-told);
            htime->Fill(curtime,tnew-told);
            curtime += 1;
            told=tnew;
            timer.Continue();
         }

         event1->Build(ev, arg5, ptmin);
         event2->Build(ev, arg5, ptmin);
         event3->Build(ev, arg5, ptmin);
         event4->Build(ev, arg5, ptmin);
         event5->Build(ev, arg5, ptmin);
         event6->Build(ev, arg5, ptmin);
         event7->Build(ev, arg5, ptmin);
         event8->Build(ev, arg5, ptmin);
         event9->Build(ev, arg5, ptmin);
         event10->Build(ev, arg5, ptmin);
         event11->Build(ev, arg5, ptmin);
         event12->Build(ev, arg5, ptmin);
         event13->Build(ev, arg5, ptmin);
         event14->Build(ev, arg5, ptmin);
         event15->Build(ev, arg5, ptmin);
         event16->Build(ev, arg5, ptmin);

         if (write) nb += tree->Fill();  //fill the tree

         if (hm) hm->Hfill(event1);      //fill histograms
         if (hm) hm->Hfill(event2);      //fill histograms
         if (hm) hm->Hfill(event3);      //fill histograms
         if (hm) hm->Hfill(event4);      //fill histograms
         if (hm) hm->Hfill(event5);      //fill histograms
         if (hm) hm->Hfill(event6);      //fill histograms
         if (hm) hm->Hfill(event7);      //fill histograms
         if (hm) hm->Hfill(event8);      //fill histograms
         if (hm) hm->Hfill(event9);      //fill histograms
         if (hm) hm->Hfill(event10);      //fill histograms
         if (hm) hm->Hfill(event11);      //fill histograms
         if (hm) hm->Hfill(event12);      //fill histograms
         if (hm) hm->Hfill(event13);      //fill histograms
         if (hm) hm->Hfill(event14);      //fill histograms
         if (hm) hm->Hfill(event15);      //fill histograms
         if (hm) hm->Hfill(event16);      //fill histograms
      }
      if (write) {
         hfile = tree->GetCurrentFile(); //just in case we switched to a new file
         hfile->Write();
      }
   }
   // We own the event (since we set the branch address explicitly), we need to delete it.
   delete event1;  event1 = 0;

   //  Stop timer and print results
   timer.Stop();
   Float_t mbytes = 0.000001*nb;
   Double_t rtime = timer.RealTime();
   Double_t ctime = timer.CpuTime();


   printf("\n%d events and %lld bytes processed.\n",nevent,nb);
   printf("RealTime=%f seconds, CpuTime=%f seconds\n",rtime,ctime);
   if (read) {
      tree->PrintCacheStats();
      printf("You read %f Mbytes/Realtime seconds\n",mbytes/rtime);
      printf("You read %f Mbytes/Cputime seconds\n",mbytes/ctime);
   } else {
      printf("compression level=%d, split=%d, arg4=%d\n",comp,split,arg4);
      printf("You write %f Mbytes/Realtime seconds\n",mbytes/rtime);
      printf("You write %f Mbytes/Cputime seconds\n",mbytes/ctime);
      //printf("file compression factor = %f\n",hfile.GetCompressionFactor());
   }
   hfile->Close();
   return 0;
}
