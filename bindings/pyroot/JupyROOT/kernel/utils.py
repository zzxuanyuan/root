# -*- coding:utf-8 -*-
#-----------------------------------------------------------------------------
#  Copyright (c) 2015, ROOT Team.
#  Authors: Omar Zapata <Omar.Zapata@cern.ch> http://oproject.org
#           Danilo Piparo <Danilo.Piparo@cern.ch> CERN
#           Enric Tejedor enric.tejedor.saavedra@cern.ch> CERN
#  website: http://oproject.org/ROOT+Jupyter+Kernel (information only for ROOT kernel)
#  Distributed under the terms of the Modified LGPLv3 License.
#
#  The full license is in the file COPYING.rst, distributed with this software.
#-----------------------------------------------------------------------------
import os
from glob import glob

import importlib

from JupyROOT.handlers import IOHandler, JupyROOTDeclarer, JupyROOTExecutor

import ROOT

_ioHandler = None
_Executor  = None
_Declarer  = None

def GetIOHandler():
    global _ioHandler
    if not _ioHandler:
        _ioHandler = IOHandler()
    return _ioHandler

def GetExecutor():
    global _Executor
    if not _Executor:
        _Executor = JupyROOTExecutor()
    return _Executor

def GetDeclarer():
    global _Declarer
    if not _Declarer:
        _Declarer = JupyROOTDeclarer()
    return _Declarer

class MagicLoader(object):
    '''Class to load JupyROOT Magics'''
    def __init__(self,kernel):
         magics_path = os.path.dirname(__file__)+"/magics/*.py"
         for file in glob(magics_path):
              if file != magics_path.replace("*.py","__init__.py"):
                  module_path="JupyROOT.kernel.magics."+file.split("/")[-1].replace(".py","")
                  try:
                      module = importlib.import_module(module_path)
                      module.register_magics(kernel)
                  except ImportError:
                      raise Exception("Error importing Magic: %s"%module_path)




