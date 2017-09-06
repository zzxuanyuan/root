// Author: Enrico Guiraud, Danilo Piparo CERN  03/2017

/*************************************************************************
 * Copyright (C) 1995-2016, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#include "RConfigure.h"      // R__USE_IMT
#include "ROOT/TDFNodes.hxx" // ColumnName2ColumnTypeName -> TCustomColumnBase, FindUnknownColumns -> TLoopManager
#include "ROOT/TDFUtils.hxx"
#include "TBranch.h"
#include "TBranchElement.h"
#include "TClassRef.h"
#include "TROOT.h" // IsImplicitMTEnabled, GetImplicitMTPoolSize

#include <stdexcept>
#include <string>
class TTree;
using namespace ROOT::Detail::TDF;

namespace ROOT {
namespace Internal {
namespace TDF {

/// Return a string containing the type of the given branch. Works both with real TTree branches and with temporary
/// column created by Define.
std::string ColumnName2ColumnTypeName(const std::string &colName, TTree *tree, TCustomColumnBase *tmpBranch)
{
   TBranch *branch = nullptr;
   if (tree)
      branch = tree->GetBranch(colName.c_str());
   if (!branch and !tmpBranch) {
      throw std::runtime_error("Column \"" + colName + "\" is not in a file and has not been defined.");
   }
   if (branch) {
      // this must be a real TTree branch
      static const TClassRef tbranchelRef("TBranchElement");
      if (branch->InheritsFrom(tbranchelRef)) {
         return static_cast<TBranchElement *>(branch)->GetClassName();
      } else { // Try the fundamental type
         auto title = branch->GetTitle();
         auto typeCode = title[strlen(title) - 1];
         if (typeCode == 'B')
            return "char";
         else if (typeCode == 'b')
            return "unsigned char";
         else if (typeCode == 'I')
            return "int";
         else if (typeCode == 'i')
            return "unsigned int";
         else if (typeCode == 'S')
            return "short";
         else if (typeCode == 's')
            return "unsigned short";
         else if (typeCode == 'D')
            return "double";
         else if (typeCode == 'F')
            return "float";
         else if (typeCode == 'L')
            return "Long64_t";
         else if (typeCode == 'l')
            return "ULong64_t";
         else if (typeCode == 'O')
            return "bool";
      }
   } else {
      // this must be a temporary branch
      const auto &type_id = tmpBranch->GetTypeId();
      if (auto c = TClass::GetClass(type_id)) {
         return c->GetName();
      } else if (type_id == typeid(char))
         return "char";
      else if (type_id == typeid(unsigned char))
         return "unsigned char";
      else if (type_id == typeid(int))
         return "int";
      else if (type_id == typeid(unsigned int))
         return "unsigned int";
      else if (type_id == typeid(short))
         return "short";
      else if (type_id == typeid(unsigned short))
         return "unsigned short";
      else if (type_id == typeid(long))
         return "long";
      else if (type_id == typeid(unsigned long))
         return "unsigned long";
      else if (type_id == typeid(double))
         return "double";
      else if (type_id == typeid(float))
         return "float";
      else if (type_id == typeid(Long64_t))
         return "Long64_t";
      else if (type_id == typeid(ULong64_t))
         return "ULong64_t";
      else if (type_id == typeid(bool))
         return "bool";
      else {
         std::string msg("Cannot deduce type of temporary column ");
         msg += colName.c_str();
         msg += ". The typename is ";
         msg += tmpBranch->GetTypeId().name();
         msg += ".";
         throw std::runtime_error(msg);
      }
   }

   std::string msg("Cannot deduce type of column ");
   msg += colName.c_str();
   msg += ".";
   throw std::runtime_error(msg);
}

const char *ToConstCharPtr(const char *s)
{
   return s;
}

const char *ToConstCharPtr(const std::string s)
{
   return s.c_str();
}

unsigned int GetNSlots()
{
   unsigned int nSlots = 1;
#ifdef R__USE_IMT
   if (ROOT::IsImplicitMTEnabled())
      nSlots = ROOT::GetImplicitMTPoolSize();
#endif // R__USE_IMT
   return nSlots;
}

void CheckCustomColumn(std::string_view definedCol, TTree *treePtr, const ColumnNames_t &customCols)
{
   const std::string definedColStr(definedCol);
   if (treePtr != nullptr) {
      // check if definedCol is already present in TTree
      const auto branch = treePtr->GetBranch(definedColStr.c_str());
      if (branch != nullptr) {
         const auto msg = "branch \"" + definedColStr + "\" already present in TTree";
         throw std::runtime_error(msg);
      }
   }
   // check if definedCol has already been `Define`d in the functional graph
   if (std::find(customCols.begin(), customCols.end(), definedCol) != customCols.end()) {
      const auto msg = "Redefinition of column \"" + definedColStr + "\"";
      throw std::runtime_error(msg);
   }
}

void CheckSnapshot(unsigned int nTemplateParams, unsigned int nColumnNames)
{
   if (nTemplateParams != nColumnNames) {
      std::string err_msg = "The number of template parameters specified for the snapshot is ";
      err_msg += std::to_string(nTemplateParams);
      err_msg += " while ";
      err_msg += std::to_string(nColumnNames);
      err_msg += " columns have been specified.";
      throw std::runtime_error(err_msg);
   }
}

/// Choose between local column names or default column names, throw in case of errors.
const ColumnNames_t SelectColumns(unsigned int nRequiredNames, const ColumnNames_t &names,
                                  const ColumnNames_t &defaultNames)
{
   if (names.empty()) {
      // use default column names
      if (defaultNames.size() < nRequiredNames)
         throw std::runtime_error(
            std::to_string(nRequiredNames) + " column name" + (nRequiredNames == 1 ? " is" : "s are") +
            " required but none were provided and the default list has size " + std::to_string(defaultNames.size()));
      // return first nRequiredNames default column names
      return ColumnNames_t(defaultNames.begin(), defaultNames.begin() + nRequiredNames);
   } else {
      // use column names provided by the user to this particular transformation/action
      if (names.size() != nRequiredNames)
         throw std::runtime_error(std::to_string(nRequiredNames) + " column name" +
                                  (nRequiredNames == 1 ? " is" : "s are") + " required but " +
                                  std::to_string(names.size()) + (names.size() == 1 ? " was" : " were") + " provided.");
      return names;
   }
}

ColumnNames_t FindUnknownColumns(const ColumnNames_t &requiredCols, TTree *tree, const ColumnNames_t &definedCols)
{
   ColumnNames_t unknownColumns;
   for (auto &column : requiredCols) {
      const auto isTreeBranch = (tree != nullptr && tree->GetBranch(column.c_str()) != nullptr);
      if (isTreeBranch)
         continue;
      const auto isCustomColumn = std::find(definedCols.begin(), definedCols.end(), column) != definedCols.end();
      if (isCustomColumn)
         continue;
      unknownColumns.emplace_back(column);
   }
   return unknownColumns;
}

} // end NS TDF
} // end NS Internal
} // end NS ROOT
