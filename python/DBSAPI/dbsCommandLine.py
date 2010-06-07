#!/usr/bin/env python
#
# Revision: $"
# Id: $"
#
#
###################################################
#
#  Developed by M. Anzar Afaq @ FNAL (March 2007)
#
#  Disclaimer:
#        Any modifications to original 
#        code will be considered Users responsibility 
#        and will not be supported)
###################################################
#
#
# system modules
#
import os, sys, string, stat, re, time
import traceback
import optparse
import StringIO
import signal, os
import pprint 
import threading
#
# Determine if DBS API is Available
dbsAvailable=True
#
try:
  # DBS specific modules
  from DBSAPI.dbsApi import DbsApi
  from DBSAPI.dbsException    import DbsException
  from DBSAPI.dbsApiException import *
  from DBSAPI.TermUtilities import TerminalController
  from DBSAPI.TermUtilities import ProgressBar
except Exception, ex:
  #print "\nUnable to setup DBS environment, will only be running as a query tool\n"
  dbsAvailable=False
  
try:
        import httplib

except Exception, ex:
        if ex.__str__() == "'Random' object has no attribute 'getrandbits'":
                exmsg ="\n\nDBS has detected a serious issue with your PYTHONPATH"
                exmsg+="\nTHIS IS A KNOWN ISSUE with using gLite UI and CMSSW"
                exmsg+="\nPlease remove lib-tk, and lib-dynload from PYTHONPATH and try again\n\n"
                raise DbsToolError(args=exmsg, code="9999")

import urllib, urllib2
import xml.sax, xml.sax.handler
from xml.sax.saxutils import escape, unescape
from xml.sax import SAXParseException

# Importing a dynamically generated module
def importCode(code,name,add_to_sys_modules=0):
    """
    Import dynamically generated code as a module. code is the
    object containing the code (a string, a file handle or an
    actual compiled code object, same types as accepted by an
    exec statement). The name is the name to give to the module,
    and the final argument says wheter to add it to sys.modules
    or not. If it is added, a subsequent import statement using
    name will return this module. If it is not added to sys.modules
    import will try to load it in the normal fashion.

    import foo

    is equivalent to

    foofile = open("/path/to/foo.py")
    foo = importCode(foofile,"foo",1)

    Returns a newly generated module.
    """
    import sys,imp

    module = imp.new_module(name)

    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module

    return module

# Class to capture stdout
class NullStream:
        def __init__(self):
                self.txt=""
        def write(self, text):
                self.txt += text
        def read(self):
                return self.txt
	def flush(self):
		pass

class manageStdOut:
	
	def capture(self):
		self.SAVEOUT = sys.stdout
		self.NEWSTDOUT = NullStream()
		sys.stdout = self.NEWSTDOUT
	def restore(self):
		sys.stdout = self.SAVEOUT


class printDot ( threading.Thread ):
       def __init__(self):  
          threading.Thread.__init__(self)
          self.doIt = 1
          #self.write = sys.stdout.write
	  term=TerminalController()
	  self.mypb = ProgressBar(term, "API Progress")

       def run ( self ):
          last = int(str(time.time()).split('.')[0])
	  update=0.0
          while ( self.doIt != 0 ) :
            curr = int(str(time.time()).split('.')[0])
            if curr > last+1 :
               update+=0.01
               self.mypb.update(update, "working")
	       last=int(str(time.time()).split('.')[0])	
            else: 
              continue
       def mark_done(self):
	  self.doIt = 0
          self.mypb.update(1.0, "Done")
	  self.mypb.clear()

class showProgress( threading.Thread ):
	def __init__(self):
		threading.Thread.__init__(self)
		self.doIt = 1
		self.twirl=True

	def run ( self ):
		if not self.twirl:
			return
		sys.stdout.write('Processing ... ')
		chars = ('|', '/', '-', '\\')
		while (self.doIt):
		    	for char in chars:	
				sys.stdout.write(char+'\b')
				sys.stdout.flush()
				time.sleep(0.1)
	def stop(self):
		if not self.twirl:
			return
		if self.doIt == 1: print ""
		self.doIt = 0
	
#############################################################################
##Default URL for the Service
#
# If NO URL is provided, URL from dbs.config will be used
#
#############################################################################

#saved_help="out.log" 
saved_help= StringIO.StringIO()

# help related funcs
class cmd_doc_writer:

  def __init__(self):
      self.wiki_help=False

  def print_all_doc(self):
      self._help_andslist()
      print "\n"
      self._help_andsdeflist()
      print "\n" 
      self._help_primarylist()
      print "\n"        
      self._help_procdslist() 
      print "\n"        
      self._help_algolist()
      print "\n"        
      self._help_filelist()
      print "\n"        
      self._help_selist()
      print "\n"        
      self._help_blocklist()
      print "\n"       
      self._help_search()  
      print "\n"
      self._help_storequery()
      print "\n"
      self._help_templatestorequery
      print "\n"
      self._help_createpads()
      print "\n"
      self._help_myadslist()
      print "\n"
      self._help_myadsdeflist()
      print "\n"
      self._help_createads()
      print "\n"
      self._help_createcff()
      print "\n"

  def command_help(self):
      pre=""
      if self.wiki_help:
                pre = "---++"
      command_help = pre+"IMAGINE the possibilities:\n"
      command_help = "\nPossible commands are:\n"
      command_help += "\n           listPrimaryDatasets or lsp, can be qualified with --pattern"
      command_help += "\n           listProcessedDatasets lsd, can provide --path"
      command_help += "\n           listAlgorithms or lsa, can provide --path"
      command_help += "\n           listRuns or lsr, can provide --path"
      command_help += "\n           listTiers or lst, can provide --path"
      command_help += "\n           listBlocks or lsb, can provide --path and/or --blockpattern"
      command_help += "\n           listFiles or lsf, must provide --path"
      command_help += "\n           listAnalysisDatasetDefinition or lsadef"
      command_help += "\n           listAnalysisDataset or lsads"
      command_help += "\n           search --searchtype=block,files,datasets"
      if self.wiki_help: command_help += "<verbatim>"
      command_help += "\n\nSome examples:\n"
      command_help += "\npython dbsCommandLine.py --help"
      command_help += "\ndbs lsp --pattern=TestPrimary*"
      command_help += "\ndbs listPrimaryDatasets --pattern=TestPrimary*"
      command_help += "\ndbs listPrimaryDatasets --pattern=*"
      command_help += "\ndbs listPrimaryDatasets"
      command_help += "\ndbs listProcessedDatasets"
      command_help += "\ndbs lsd --p=/QCD_pt_0_15_PU_OnSel/SIM/CMSSW_1_2_0-FEVT-1168294751-unmerged"
      command_help += "\ndbs listFiles --path=/PrimaryDS_ANZAR_01/SIM/anzar-procds-01"
      command_help += "\ndbs lsf --path=/PrimaryDS_ANZAR_01/SIM/anzar-procds-01"
      command_help += "\n"
      if self.wiki_help: command_help += "</verbatim>"
      return command_help

  def command_short_help(self):
      command_help = "   Some possible commands are:"
      command_help += "\n           listProcessedDatasets or lsd, can provide --path"
      command_help += "\n           listAlgorithms or lsa, can provide --path"
      command_help += "\n           listFiles or lsf, must provide --path"
      command_help += "\n           listAnalysisDatasetDefinition or lsadef"
      command_help += "\n           listAnalysisDataset or lsads"
      command_help += "\n           myAnalysisDatasetDef or myadsdef or --myadsdef"
      command_help += "\n           myAnalysisDataset or myads"
      command_help += "\n           search or --search\n"
      command_help += "\n EXAMPLES:"
      command_help += "\n 	    python dbsCommandLine.py -c lsd --path=/*/*/RAW" 
      command_help += "\n     OR     Using ALIAS 'dbs' for 'python dbsCommandLine.py -c'\n"
      command_help += "\n            dbs lsd --path=/*/*/RAW\n"
      command_help += "\n Note: most commands can print greater details with --report"
      command_help += "\n Please use --doc for details"
      return command_help

  def _help_myadsdeflist(self):
                pre=""
                if self.wiki_help:
                        print "---+++"
                print pre+"Lists Local Analysis Dataset Definitions"
                print "   Arguments:"
                print "         -c myadsdef, or --command=myAnalysisDatasetDef or --command=myadsdef"
                print "         optional: --pattern=<Analysis_Dataset_Name_Pattern>"
                print "                   --help, displays this message"
                if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs myadsdef"
                print "         dbs myadsdef --pattern=MyStoredQuery*"
                if self.wiki_help: print "</verbatim>"

  def _help_myadslist(self):
		pre=""
                if self.wiki_help:
                        print "---+++"
                print pre+"Lists Local Analysis Datasets"
                print "   Arguments:"
                print "         -c myads, or --command=myAnalysisDataset or --command=myads"
                print "         optional: --pattern=<Analysis_Dataset_Name_Pattern>"
                print "                   --help, displays this message"
                if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs myads"
                print "         dbs myads --pattern=MyAnalysis*"
                if self.wiki_help: print "</verbatim>"


  #Analysis Datasets
  def _help_andslist(self):
                pre=""
		if self.wiki_help:
			print "---+++"
                print pre+"Lists Analysis Datasets know to this DBS"
                print "   Arguments:"
                print "         -c lsads, or --command=listAnalysisDataset or --command=lsads"
                print "         optional: --pattern=<Analysis_Dataset_Name_Pattern>"
		print "                   --path=<dataset path>"
		print "                   --adsversion=VERSION, list a specific version of ADS, default is latest"
                print "                   --help, displays this message"
		if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs lsads"
                print "         dbs lsads --pattern=MyAnalysis*"
                print "         dbs lsads --path=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-0006210/FEVT"
                print "         dbs lsads --pattern=MyAnalysisDatasets4* --path=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-0006210/FEVT"
		if self.wiki_help: print "</verbatim>"

  #Analysis Dataset Definitions
  def _help_andsdeflist(self):
                pre=""
                if self.wiki_help:
                        print "---+++"
                print pre+"Lists Analysis Dataset Definitions know to this DBS"
                print "   Arguments:"
                print "         -c lsadef, or --command=listAnalysisDatasetDefinition or --command=lsadef"
                print "         optional: --pattern=<Analysis_Dataset_Definition_Name_Pattern>"
                print "                   --help, displays this message"
		if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs lsadef"
                print "         dbs lsadef --pattern=WhatILike*"
		if self.wiki_help: print "</verbatim>"
                

  #primary
  def _help_primarylist(self):
                pre=""
                if self.wiki_help:
                        print "---+++"
                print pre+"Lists PrimaryDatasets know to this DBS"
                print "   Arguments:"
                print "         -c lsp, or --command=listPrimaryDataset or --command=lsp"
                print "         optional: --pattern=<Primary_Dataset_Name_Pattern>"
		print "                   --help, displays this message"    
		if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs lsp"
                print "         dbs lsp --pattern=mc*"
		if self.wiki_help: print "</verbatim>"

  def _help_procdslist(self):
                pre=""
                if self.wiki_help:
                        print "---+++"
                print pre+"Lists Processed Datasets (and Paths) known to this DBS"
                print "   Arguments:"
                print "         -c lsd, or --command=listProcessedDatasets or --command=lsd"
                print "         optional: "
                print "                     --algopattern=<Algorithm_Pattern>  "
                print "                     (in /ExecutableName/ApplicationVersion/ApplicationFamily/PSet-Hash form )"
                print "                     supports glob patterns"
                print "                     --path=<dataset path>"
                print "                     --report, if provided a report for each dataset is generated"
		print "                     --help, displays this message"    
		if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs lsd"
                print "         dbs lsd --report"
                print "         dbs lsd --path=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-0006210/*"
                print "         dbs lsd --path=/TAC-TIBTOB-120-DAQ-EDM/*/*"
                print "         dbs lsd --path=/TAC-TIBTOB-120-DAQ-EDM/*/*  --algopattern=/*/CMSSW_1_2_0/*/*"
		if self.wiki_help: print "</verbatim>"

  def _help_algolist(self):
                pre=""
                if self.wiki_help:
                        print "---+++"
                print pre+"List Algorithms known to this DBS"
                print "   Arguments:"
                print "         -c lsa, or --command=listAlgorithms or --command=lsa"
                print "         --algopattern=</ExecutableName/ApplicationVersion/ApplicationFamily/PSet-Hash>"
                print "                       supports glob search"
		print "         --help, displays this message"    
		if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs lsa"
                print "         dbs lsa --algopattern=/*/CMSSW_1_2_0/*/*"
                print "         dbs lsa --algopattern=/*/CMSSW_1_2_0          (equally good)"
		if self.wiki_help: print "</verbatim>"

  def _help_filelist(self):
                pre=""
                if self.wiki_help:
                        print "---+++"
                print pre+"List Files known to this DBS"
                print "   Arguments:"
                print "         -c lsf, or --command=listFiles or --command=lsf"
                print "         optional: "
                print "                     --lfnpattern=<LogicalFileName_Pattern>  "
                print "                                          supports glob patterns"
                print "                     --path=<dataset path>"
                print "                     --blockpattern=<Block_Name_Pattern> in the form /prim/proc/dt(n)#<GUID>, DOES NOT support glob patterns"
                print "                     --report, if provided a report for each file is generated"
		print "                     --help, displays this message"    
		if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs lsf --path=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-00006219/RAW"
                print "         dbs lsf --path=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-00006219/RAW --report"
                print "         dbs lsf --lfnpattern=*root* --report (don't do that please !)"
                print "         dbs lsf --blockpattern=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-00006219#1134f4e5-addd-4a45-8d28-fd491d0e6154 --report"
                print "         dbs lsf --blockpattern=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-00006219#1134f4e5-addd-4a45-8d28-fd491d0e6154"
                print "   Note: --path takes precedence over --blockpattern and --lfnpattern (if --path provided rest are ignored)"
		if self.wiki_help: print "</verbatim>"

  def _help_selist(self):
                pre=""
                if self.wiki_help:
                        print "---+++"
                print pre+"List Storage Elements known to this DBS"
                print "   Arguments:"
                print "         -c lsse, or --command=listStorageElements or --command=lsse"
                print "   optional: --sepattern=<Storage element name pattern> for glob search"
		print "             --help, displays this message"    
		if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs lsse"
                print "         dbs lsse --sepattern=*it         (All italian Storage Elements)"
                print "         dbs lsse --sepattern=*fnal*         (All FNAL Storage Elements)"
		if self.wiki_help: print "</verbatim>"

  def _help_blocklist(self):
                pre=""
                if self.wiki_help:
                        print "---+++"
                print pre+"List File Blocks known to this DBS"
                print "   Arguments:"
                print "         -c lsb, or --command=listBlocks or --command=lsb"
                print "         optional: "
                print "                     --path=<dataset path>"
                print "                     --blockpattern=<Block_Name_Pattern> in the form /prim/proc/dt(n)#<GUID>, supports glob patterns"
                print "                     --sepattern=<Storage element name pattern> for glob search"
                print "                     --report, if provided a report for each file block is generated"
		print "                     --help, displays this message"    
		if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs lsb --path=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-00006219/RAW"
                print "         dbs lsb --path=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-00006219/RAW --report"
                print "         dbs lsb --sepattern=*fnal*"
                print "         dbs lsb --blockpattern=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-00006219#1134f4e5-addd-4a45-8d28-fd491d0e6154 --report"
                print "         dbs lsb --blockpattern=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0-RAW-Run-00006219#1134f4e5-addd-4a45-8d28-fd491d0e6154"
                print "   Note: --path takes precedece over --blockpattern and --lfnpattern (if --path provided rest are ignored)"
		if self.wiki_help: print "</verbatim>"
  def _help_storequery(self):
                pre=""
                if self.wiki_help:
                        pre="---+++"
                print pre+"Store the serach Criteria in DBS Mart"
                print "   At the query storeage time or later this query could be used to create ADS"
                print "   First you run search, after you have found what you were looking for "
                print "   Add --storequery=<QUERYNAME> to the commandline and it will create an ADS Definition for you"
                print "   Arguments:"
                print "         --storequery=<QUERYNAME>"
                print "                    Where QUERYNAME is a Valid Analysis Dataset Definition Name"
                if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs search --query=\"find file, lumi where dataset=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0/RAW\" --storequery=MYTESTADSDEF"
                if self.wiki_help: print "</verbatim>"

  def _help_templatestorequery(self):
                pre=""
                if self.wiki_help:
                        pre="---+++"
                print pre+"Store the serach Criteria in DBS Mart without Specifying a Dataset Path (Expert Option)"
                print "   At the query storage time or later this query could be used to create ADS, User will need"
                print "   to provide --path option. "
		print "Such queries in DBS are called Template queries, and hence the name"
                if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs search --query=\"find lumi where run = 1234\" --storetemplatequery=MYTESTADSDEF"

                if self.wiki_help: print "</verbatim>"

  def _help_createpads(self):
                pre=""
                if self.wiki_help:
                        pre="---+++"
                print pre+"Creates Personal Analysis Dataset (Local on Disk), based on the search criteria"
		print "   This command cannot be used alone you have to use it with either 'dbs search' or --usequery="
		print "   First you run search, after you have found what you were looking for "
		print "   Add --createPADS=<PADSNAME> to the commandline and it will create a Personal ADS for you"
		print "   you will have to specify a name for your search as well --storequery=<QUERYNAME>"
		print "                    Where QUERYNAME is a Valid Analysis Dataset Definition Name, stored in your local system"
		print "   You can use --usequery=<QUERYNAME> to specify a pre-stored query from DBS Mart"
		print "   Arguments:"
		print "         --createPADS=<PADSNAME> --storequery=<QUERYNAME>"
		print "                Where PADSNAME is a Valid Analysis Dataset Name, QUERYNAME is a Valid Analysis Dataset Definition Name"
		print "                   "
		print "         --createPADS=<PADSNAME> --usequery=<QUERYNAME>"
		print "                Where PADSNAME is a Valid Analysis Dataset Name, QUERYNAME is a Valid Analysis Dataset Definition Name from DBS Mart"
                if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs search --query=\"find files where dataset=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0/RAW\" --storequery=ANZARTESTADS --createPADS=MYPADS01"
                print "         python dbsCommandLine.py --createPADS=MYPADS01 --usequery=MYTESTADSDEF"
		if self.wiki_help: print "</verbatim>"

  def _help_createcff(self):
		pre=""
		if self.wiki_help:
                        pre="---+++"
                print pre+"Creates .cff files for the search criteria"
                print "   This command cannot be used alone you have to use it with --usequery= or search"
                print "   You can use --usequery=<QUERYNAME> to specify a pre-stored query"
		print "   or perform a fresh search using --query and store the results as .cff"
                print "   First you run search (dbs search --query=\"Your Query Here\", after you have found what you were looking for "
                print "   you will either specify a name for your search and store it using --storequery=<QUERYNAME>"
                print "                    Where QUERYNAME is a Valid Analysis Dataset Definition Name, stored in your local system"
                print "   And then use then --usequery=<QUERYNAME> to specify a pre-stored query and create same ADS in DBS instance"
                print "   Arguments:"
                print "         --createCFF=<CFFNAME> --usequery=<QUERYNAME>"
                print "                Where CFFNAME is a Valid File Name, QUERYNAME is a Valid Analysis Dataset Definition Name from DBS Mart"
		print "   OR you can just specify --createCFF= and --storequery= and create .cff and store the query at the same time"
                print "         dbs search --query=\"find files where dataset=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0/RAW\" --createPADS=mycff_file (do not specify .cff (.cff is default))"
                if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         python dbsCommandLine.py --createCFF=MYCFFTest01 --usequery=MYTESTADSDEF01"
                if self.wiki_help: print "</verbatim>"

  def _help_createcfg(self):
                pre=""
                if self.wiki_help:
                        pre="---+++"
                print pre+"Creates .cff files for CRAB based on search criteria"
                print "   This command cannot be used alone you have to use it with --usequery= or search"
                print "   You can use --usequery=<QUERYNAME> to specify a pre-stored query"
                print "   or perform a fresh search using --query and store the results as crab.cfg"
                print "   First you run search (dbs search --query=\"Your Query Here\", after you have found what you were looking for "
                print "   you will either specify a name for your search and store it using --storequery=<QUERYNAME>"
                print "                    Where QUERYNAME is a Valid Analysis Dataset Definition Name, stored in your local system"
                print "   And then use then --usequery=<QUERYNAME> to specify a pre-stored query and create same ADS in DBS instance"
                print "   Arguments:"
                print "         --usequery=<QUERYNAME>"
                print "                Where QUERYNAME is a Valid Analysis Dataset Definition Name from DBS Mart"
                print "   OR you can just specify --createCFG= and --storequery= and create crab.cfg and store the query at the same time"
                if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs search --query=\"find files where dataset=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0/RAW\" --createCFG"
		if self.wiki_help: print "</verbatim>"


  def _help_createads(self):
                pre=""
                if self.wiki_help:
                        pre="---+++"
                print pre+"Creates Analysis Dataset (NOT on Local on Disk but in specified DBS instance), based on the search criteria"
                print "   This command cannot be used alone you have to use it with --usequery= or search"
                print "   You can use --usequery=<QUERYNAME> to specify a pre-stored query"
                print "   or perform a fresh search using --query and create an ADS"
                print "   First you run search (dbs search --query=\"Your Query Here\", after you have found what you were looking for "
                print "   you will either specify a name for your search and store it using --storequery=<QUERYNAME>"
                print "                    Where QUERYNAME is a Valid Analysis Dataset Definition Name, stored in your local system"
                print "   And then use then --usequery=<QUERYNAME> to specify a pre-stored query and create same ADS in DBS instance"
                print "   Arguments:"
                print "         --createADS=<ADSNAME> --storequery=<QUERYNAME>"
                print "                Where ADSNAME is a Valid Analysis Dataset Name, QUERYNAME is a Valid Analysis Dataset Definition Name"
                print "                   "
                print "         --createADS=<ADSNAME> --usequery=<QUERYNAME>"
                print "                Where ADSNAME is a Valid Analysis Dataset Name, QUERYNAME is a Valid Analysis Dataset Definition Name from DBS Mart"
                if self.wiki_help: print "<verbatim>"
                print "   examples:"
                print "         dbs search --query=\"find files where dataset=/TAC-TIBTOB-120-DAQ-EDM/CMSSW_1_2_0/RAW\" --storequery=ANZARTESTADS --createADS=MYADS01"
                print "         python dbsCommandLine.py --createADS=MYADS01 --usequery=MYTESTADSDEF"
                if self.wiki_help: print "</verbatim>"
		
  def _help_search(self):
                pre=""
                if self.wiki_help:
                        pre="---+++"
                print pre+"Search the Data known to this DBS, based on the search criteria"
                print "   Arguments:"
                print "         -c --search, or --command=--search or --command=search"
                print "         required: "
                print "                     --query=<user supplied query as supported by Data Discovery Page"
		print "         optional: "
		print "                     --help, displays this message"    
		if self.wiki_help: print "<verbatim>"
                print "   Simple Example:"
                print "         dbs search --query=\"find dataset where dataset like *Online*\""
		detail_help="""
			Query Syntax: 	FIND <keyword> WHERE <keyword> <op> <value> AND | OR <keyword> <op> <value>
					Constrain operators: <=, <, >=, >, =, not like, like, in, between
					words FIND,WHERE,AND,OR can be upper or lower case.
					Expressions can be groupped together using brackets, e.g. ((a and b) or c)

			Query Keywords: 	dataset primds , procds , tier , block , file , release , run , lumi , site. (etc.)

			Query Examples: 
				find release where release like *
				find file where release>CMSSW_1_6_7 and site=T2_UK
				find file,run where dataset=/Commissioning2008Ecal-A/Online/RAW

			Further Help: Use 'dbs help find' or 'dbs help <keyword>'
			
			'dbs search' can be used in conjuction with --storequery and --createPADS/--createADS 
					look at --doc for details
				"""
		print detail_help
                print "search can be used in conjuction with --storequery and --createPADS/--createADS operations, look at --doc for help"
		if self.wiki_help: print "</verbatim>"


###############################################################################
# Following are independent functions to re-direct and capture 
# help from optparse.OptionParser, and not part of cms_doc_writer class
#
#capture help from optparse in atemp file
def redirected_print_help(self):
	print self.print_usage()
	print print_help(self)
      
# This function just dumps the generic help text on screen
def print_help(self):
	print saved_help.getvalue()
	if dbsAvailable:
		term=TerminalController()
	helper = cmd_doc_writer()
        print helper.command_short_help()
        #print term.BLUE+helper.command_short_help()+term.NORMAL

       #print open(saved_help, 'r').read()

###############################################################################

class DbsOptionParser(optparse.OptionParser):
  """
     OptionParser is main class to parse options.
  """

  def __init__(self):
      optparse.OptionParser.__init__(self, usage="%prog --help or %prog --command [options]", version="%prog 1.0.9", conflict_handler="resolve")

      #self.add_option("--url",action="store", type="string", dest="url", default="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet",
      #     help="specify URL, e.g. --url=http://cmssrv17.fnal.gov:8989/DBS/servlet/DBSServlet, If no url is provided default url from dbs.config is attempted")

      self.add_option("--url",action="store", type="string", dest="url", default="BADURL",
           help="specify URL, e.g. --url=http://cmssrv17.fnal.gov:8989/DBS/servlet/DBSServlet, If no url is provided default url from dbs.config is attempted")

      self.add_option("--alias",action="store", type="string", dest="alias", default="NOALIAS",
           help="specify DBS instance alias, e.g. --alias=Prod_GLOBAL_R, If no alias is provided default url/alias from dbs.config is attempted")

      #self.add_option("-v","--verbose", action="store", type="string", default="ERROR", dest="level",
      #     help="specify verbose level, e.g. --verbose=DBSDEBUG, The possible values are, CRITICAL, ERROR, DBSWARNING, DBSDEBUG, DBSINFO, where DBSINFO is most verbose, and ERROR is default")

      self.add_option("--q","--quiet", action="store_true", dest="quiet", default=False,
	   help="if specified the output of certain commands will be supressed, generally used with 'search'")

      self.add_option("--p","--path", action="store", type="string", dest="path",
           help="specify dataset path, e.g. -p=/primary/tier/processed, or --path=/primary/tier/processed, supports shell glob")

      self.add_option("--pattern", action="store", type="string", dest="pattern", default="*",
           help="some commands scope could be restricted with pattern, e.g. listPrimaryDataset, supports shell glob for Primary Dataset Names, "+ \
						"Use --doc or individual commands with --help")

      self.add_option("--run", action="store", type="string", dest="run",
           help="Run number, can be used by list files or search commands")

      self.add_option("--algopattern", action="store", type="string", dest="algopattern",
           help="Algorithms can be specified as algopattern /appname/appversion/appfamily/pset_hash, supports shell glob")

      self.add_option("--blockpattern", action="store", type="string", dest="blockpattern",
           help="BlockName pattern for listBlocks and other calls in format /primary/tier/processed#guid, supports shell glob")

      self.add_option("--sepattern", action="store", type="string", dest="sepattern",
           help="Storage Element Name pattern for glob search")

      self.add_option("--lfnpattern", action="store", type="string", dest="lfnpattern",
           help="Logical File Name pattern for glob search")

      self.add_option("--storequery", action="store", type="string", dest="storequery",
           help="Store the search query as ADS Definition (must be used with --search)")

      self.add_option("--storetemplatequery", action="store", type="string", dest="storetemplatequery",
           help="Store the search query as ADS <<Template>> Definition (must be used with --search)")

      self.add_option("--query", action="store", type="string", dest="query",
           help="Search query used to perform data serach, create ADS Definitions etc ")

      self.add_option("--begin", action="store", type="string", dest="begin", default="",
           help="Used to specify the begin of the paginated query (you can ask few items from the result)")

      self.add_option("--end", action="store", type="string", dest="end", default="",
           help="Used to specify the end of the paginated query (you can ask few items from the result) ")

      self.add_option("--xml", action="store_true", dest="xml",
           help="If specified XML from DBS Server will be printed (in general used for debugging) ")

      self.add_option("--donotrunquery", action="store_true", default=False, dest="donotrunquery",
           help="If you add this option the query specified will not be run, use with caution with --storequery and --storetemplatequery, \
              ONLY IF you already know that your query yields what you want")

      self.add_option("--createPADS", action="store", type="string", dest="createPADS",
           help="Create Personal Analysis Dataset for the search query (must be used with --search, or --usequery=)")

      self.add_option("--createADS", action="store_true", dest="createADS", 
           help="Create Analysis Dataset for the search query (must be used with --search, or --usequery=)")

      self.add_option("--createCFG", action="store_true", dest="createCFG", 
           help="Creates crab.cfg for the search query (must be used with --search, or --usequery=)")

      self.add_option("--createCFF", action="store", type="string", dest="createCFF",
           help="Create .cff file for the search query (must be used with --search, or --usequery=)")

      self.add_option("--usequery", action="store", type="string", dest="usequery",
           help="Specifies the ADS Def/Query to be used for creating PADS/ADS (must be used with --createPADS= or --createADS=)")

      self.add_option("--dbsmartfile", action="store", type="string", dest="dbsmartfile",
           help="Location of the dbs mart file, absolute path or relative to ADSHOME")

      self.add_option("--useASearch", action="store_true", dest="useASearch", default=False,
           help="If supplied, ASearch (Search DBS Discovery Page) is used instead of DBS Server")

      self.add_option("--report", action="store_true", default=False, dest="report",
           help="If you add this option with some listCommands the output is generated in a detailed report format")

      self.add_option("--adsversion", action="store", default="", dest="adsversion",
           help="Use this with lsads (listing Analysis Dataset) to list a specific version")

      self.add_option("--doc", action="store_true", default=False, dest="doc",
           help="Generates a detailed documentation for reference, overrides all other cmdline options (use --wiki_help to produces help document in wiki format [dbs --doc --wiki_help])")

      self.add_option("--twirl", action="store_true", default=False, dest="twirl",
           help="If provided, tool will show 'Progressing...' Twril on screen (can be useful when running large queries)")

      self.add_option("--noheader", action="store_true", default=False, dest="noheader",
           help="If provided, tool will NOT display the header information in the result of query (Useful for scripted quries)")

      self.add_option("--production", action="store_true", dest="production", default=False,
                help="When added to --query, displays ALL datasets regardles of their validity and aviability status")

      ## Always keep this as the last option in the list
      self.add_option("-c","--command", action="store", type="string", default="notspecified", dest="command",
                         help="Command line command, e.g. -c lsp, or --command=listPrimaryDataset, "+ \
				"Also you can use --help with individual commands, e.g, -c lsp --help, you donot need to specify --command search when providing --query ")

      ## capture help
      self.capture_help()
      ## redirect print_help
      optparse.OptionParser.print_help=redirected_print_help

  def capture_help(self):

      saveout = sys.stdout
      sys.stdout = saved_help
      self.print_help()
      sys.stdout = saveout
      #saved_help.close()

  def doc(self):
      print_help(self) 
      helper = cmd_doc_writer()
      if '--wiki_help' in sys.argv:
        helper.wiki_help=True
      print helper.command_help()
      print "\n" 	
      print "More Details on individual calls:" 	
      helper.print_all_doc()
      sys.exit(0)

  def parse_args(self):
      """
      Intercepting the parser and removing help from menu, if exist
      This is to avoid STUPID handling of help by nice optparse (optp-arse!)
      """

      if '--doc' in sys.argv:
        self.doc()

      help=False
      possible_helps=['help', '--help', '-h', '-help', '--h', '?', '-?', '--?', 'h']
      for hlpcmd in possible_helps:
	if hlpcmd in sys.argv:
                sys.argv.remove(hlpcmd)
                help=True	

      if help==True:
                if len(sys.argv) < 3:
			print_help(self)
			sys.exit(0)
      		self.add_option("--want_help", action="store", type="string", dest="want_help", default="yes",
           		help="another way to ask for help")
                 
      return optparse.OptionParser.parse_args(self)	

  def getOpt(self):
      """
          Returns parse list of options
          @type  self: class object
          @param self: none
          @rtypei : none
          @return : list of options.
      """
      return self.parse_args()
  

class printReport:

  def __init__(self, report):
	"""
    	report: is a Python data structure of this format
    	report = {
        	'summary': object (dict type), all key/vals will be printed as HEADER
        	'Comments': TEXT, any text appearing here will be printed after HEADER
        	'lines' : [lineObjs], each line Object's key/value pair will be printed in each line
        	}
    	"""
	print report['summary']
	for aline in report['lines']:
		print aline
	
class Report(dict):
        def __init__(self):
                dict.__init__(self)
                self['summary']=""
                self['lines']=[]
        def addSummary(self, summary):
                self['summary'] += summary
        def addLine(self, line):
                self['lines'].append(line)

# API Call Wrapper
class ApiDispatcher:

  def makeApi(self):
    self.api = None
    #See if Twril needs to be ignored
    if dbsAvailable:
        self.api = DbsApi(opts.__dict__)
        if not self.optdict['noheader'] : self.printGREEN( "Using DBS instance at: %s" %self.optdict.get('url', self.api.url()))
        self.optdict['url']=self.api.url()

  def getApi(self):
	if self.api == None:
		raise Exception("-1111", "You need to setup DBS Client for this functionality")
	return self.api
 
  def getArgVal(self, args):
	vals=args.command.split('=')
	if len(vals) >= 2:
		return vals[1]
	else: return ""

  def __init__(self, args):
   try:

    self.helper = cmd_doc_writer()
    self.progress=showProgress()
    if dbsAvailable:
    	self.term = TerminalController()
	
    self.optdict=args.__dict__

    #If User specified JUST the query on command line....that should be handled as well, for some CRAZY reasons
    # User is DOG, can specify anything....
    apiCall = self.optdict.get('command', '')
    if apiCall in ('', 'notspecified'):
    	qu=self.optdict.get('query', '')
	if qu in (None, ''):
		findQuery=""
		for item in sys.argv:
			if item.strip().startswith("find"):
				apiCall="search"
				self.optdict['query']=item.strip()
	else: 
		if qu.strip().startswith("find"):
			apiCall="search"

    # If NO URL is provided, URL from dbs.config will be used
    if opts.__dict__['url'] == "BADURL":
        del(opts.__dict__['url']) 

    if opts.__dict__['alias'] == 'NOALIAS':
	del(opts.__dict__['alias'])

    self.makeApi()
    self.progress.twirl=self.optdict['twirl']

    if apiCall in ('', 'notspecified') and self.optdict.has_key('want_help'):
        print_help(self)
        return

    if self.optdict.has_key('want_help'):
        if (self.manageHelp()):
                return

    if apiCall in ("--version", "version", "-ver", "--ver"):
        print "DBS API Version: %s" %self.getApi().getApiVersion()
        return

    elif apiCall in ("", None):
       self.printRED ("No command specified, use --help for details")
       return

    # Following are some weired possibilities, lets deal with'em first
    elif apiCall.startswith('myAnalysisDatasetDef') or apiCall.startswith('myadsdef') or apiCall.startswith('--myadsdef') or \
		apiCall.startswith('myAnalysisDatasetDef=') or apiCall.startswith('myadsdef=') or apiCall.startswith('--myadsdef=') :
	apiCall='myadsdef'
	self.optdict['myadsdef']=self.getArgVal(args)

    elif apiCall.startswith('myAnalysisDataset=') or apiCall.startswith('myads=') or apiCall.startswith('--myads=') or \
    	 	apiCall.startswith('myAnalysisDataset') or apiCall.startswith('myads') or apiCall.startswith('--myads'):
	apiCall='myads'
        self.optdict['myads']=self.getArgVal(args)

    elif apiCall.startswith('usequery') or apiCall.startswith('--usequery') or \
                        apiCall.startswith('usequery=') or apiCall.startswith('--usequery='):
	apiCall='usequery'
	self.optdict['usequery']=self.getArgVal(args)

    # also handles storetemplatequery
    elif apiCall.startswith('storequery') or apiCall.startswith('--storequery') or \
		apiCall.startswith('--storequery=') or apiCall.startswith('storequery=') :
	apiCall='storequery'
	self.optdict['storequery']=self.getArgVal(args)

    elif apiCall.startswith('createPADS') or apiCall.startswith('--createPADS') or \
                        apiCall.startswith('createPADS=') or apiCall.startswith('--createPADS='):
        apiCall='createPADS'
        self.optdict['createPADS']=self.getArgVal(args)

    elif apiCall.startswith('createADS') or apiCall.startswith('--createADS') or \
                        apiCall.startswith('--createADS=') or apiCall.startswith('createADS='):
        apiCall='createADS'
        self.optdict['createADS']=self.getArgVal(args)

    elif apiCall.startswith('createCFG') or apiCall.startswith('--createCFG'):
        apiCall='createCFG'
	self.optdict['createCFG']=True
	self.optdict['quiet']=True

    elif apiCall.startswith('createCFF') or apiCall.startswith('--createCFF') or \
                        apiCall.startswith('--createCFF=') or apiCall.startswith('createCFF='):
        apiCall='createCFF'
	self.optdict['quiet']=True
        self.optdict['createCFF']=self.getArgVal(args)
	if self.optdict.get('createCFF') in (None, ''): self.printRED("createCFF needs an argument, use --createCFF=<CFFNAME>")

    else:
	# Lets continue with normal executioni
	# we possibly have our command right
	pass

    #Execute the proper API call
    ##listPrimaryDatasets 
    if apiCall in ('listPrimaryDatasets', 'lsp'):
	#self.optdict['query']="find primds"
	#self.handleSearchCall()
	self.handleListPrimaryDatasets()

    ##listProcessedDatasets
    elif apiCall in ('listProcessedDatasets', 'lsd'):
	#self.optdict['query']="find dataset"
	#self.handleSearchCall()
	self.handleListProcessedDatasets()

    ##listAlgorithms
    elif apiCall in ('listAlgorithms', 'lsa'):
	self.handleListAlgorithms()

    ##listFiles
    elif apiCall in ('listFiles', 'lsf'):
	self.handleListFiles() 

    ##listBlocks
    elif apiCall in ('listBlocks', 'lsb'):
	self.handleBlockCall()

    ##listStorageElements
    elif apiCall in ('listStorageElements', 'lsse'):
        self.handlelistSECall()

    elif apiCall in ('listAnalysisDatasetDefinition', 'lsadef'):
	self.handlelistANDSDefCall()

    elif apiCall in ('listAnalysisDataset', 'lsads'):
        self.handlelistANDCall()

    elif apiCall in ('myads'):
	self.handlelistMyADSCall()
	print "myads"

    elif apiCall in ('myadsdef'):
	self.handlelistMyADSDEFCall()
	print "myadsdef"

    elif apiCall in ('usequery'):
	self.handleUseQueryCall()

    elif apiCall in ('storequery'):
	self.handleStoreQueryCall()

    elif apiCall in ('createPADS'):
	self.handleCreatePADSCall()

    elif apiCall in ('createADS'):
        self.handleCreateADSCall()

    elif apiCall in ('createCFF'):
        self.handleCreateCFFCall()

    elif apiCall in ('createCFG'):
        self.handleCreateCFGCall()

    ##Search data
    elif apiCall in ('search', '--search') or self.optdict.get('search') :
	self.handleSearchCall()

    else:
       print "Unsupported API Call '%s', please use --doc or --help" %str(apiCall)

    """

   except DbsApiException, ex:
      self.progress.stop()
      self.printRED("Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() ))
      if ex.getErrorCode() not in (None, ""):
          self.printRED("DBS Exception Error Code: %s "% str(ex.getErrorCode()))

   except DbsException, ex:
      self.progress.stop()
      self.printRED( "Caught DBS Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() ))
      if ex.getErrorCode() not in (None, ""):
          self.printRED( "DBS Exception Error Code: %s " % str(ex.getErrorCode()))

   except Exception, ex:
        self.progress.stop()
        self.printRED ("Unknow Exception in user code:")
        traceback.print_exc(file=sys.stdout)

   except KeyboardInterrupt:
        self.progress.stop()
        print "Interrupted."
        sys.exit(1)

    """

   except Exception, ex:
    try:
      self.progress.stop()
      message=ex.getErrorMessage()
      code=ex.getErrorCode()
    except:
        try:
                (code, message) = ex
        except:
                code = 0
                message = ex
    print "Error: " + str(message)
    print "Error Code: "  + str(code)

  def manageHelp(self):

	cmd = self.optdict.get('command', '')

        if cmd in ("find", "FIND", "Find"):
                desc_help="""
        FIND is used with the search command to query data in DBS

        Example:   dbs search --query=\"find dataset where dataset like *Online*\""
        Syntax:    FIND <keyword> WHERE <keyword> <op> <value> AND | OR <keyword> <op> <value>
        Operators: <=, <, >=, >, =, not like, like, in, between
                        words FIND,WHERE,AND,OR can be upper or lower case.
        """
                print desc_help
                #print "Possible keywords are: \n\t", string.join( entityAttr.keys(), '  ' )
                print "For further help on keywords use:"
                print " dbs help <keyword>\n"
		print " dbs help keywords\n"
		print " dbs help examples\n"
                return True

  	entities = self.getApi().getHelp("")
	entityNames = [ x['name'] for x in entities]

        if cmd in ("keywords"):
                print "Possible keywords are: ", string.join( entityNames, '  ' )
                print "For further help and examples on each keyword use:"
                print " dbs help <keyword>\n"
                return True

	cmd_s=cmd.split('.')
	if (len(cmd_s) > 1 ) : cmd=cmd_s[0]
	if cmd in entityNames:	
		self.printGREEN( '\nENTITY\t ' + cmd )
		print 'ATTRIBUTES'
		for item in entities:
			if item['name']==cmd:
				attrList = item['attrs']
				examples = item['examples']
		for attr in attrList:
			toPrint = '\t' + cmd + '.' + attr
			self.printGREEN( toPrint )
		print 'EXAMPLES'
		for aex in examples:
			print aex['desc']
			self.printRED( "     "+aex['query'] )
		return True

	return False
		
  def setMartParams(self):

	#Loaded with MART Queries
	self.KnownQueries = {}
	self.mart_file = ""
	try :
        	#Lets see if user has provided a MART File as destination
        	self.adshome = os.path.expandvars(self.getApi().adshome())
        	if not os.path.exists(self.adshome):
                	self.printRED("WARNING: Path %s do not exist, ADSHOME (%s) parameter is not set or not a valid path" \
										% ( self.adshome,  str(self.getApi().adshome())))
			self.printRED("WARNING: Trying to create ADSHOME (%s) " %str(self.adshome))
			try:
				os.mkdir(self.adshome)
			except:
				self.printRED("ERROR: Unable to create ADSHOME (%s) " %str(self.adshome))
                		return False
	except DbsException, ex:
		try:
			self.adshome = os.path.expandvars("$PWD")
	 	except:
			pass
		if self.adshome in (None, ""):
			self.adshome=os.getcwd()

        mart_file_name = self.optdict.get('dbsmartfile') or ''
        if mart_file_name not in ('', None):
                if not os.path.exists(mart_file_name):
                        self.mart_file = os.path.join(self.adshome, mart_file_name)
                else : self.mart_file = mart_file_name
        #Else use the DEFAULT Mart file
        else :
                self.mart_file = os.path.join(self.adshome, "Default.dm")

	# LOAD the mart file if it already exits, otherwise create a new one
	if os.path.exists(self.mart_file):
		martFile=open(self.mart_file, "r")
		mart = importCode(martFile, "mart", 0)
		self.KnownQueries = mart.KnownQueries

	return

  def printRED(self, msg):
    print self.term.RED+msg+self.term.NORMAL

  def printGREEN(self, msg):
    print self.term.YELLOW+msg+self.term.NORMAL

  def printBLUE(self, msg):
    print self.term.BLUE+msg+self.term.NORMAL

  def makeTIME(self, intime):
    return time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime(long(intime)))


  def handlelistANDSDefCall(self):
        if self.optdict.has_key('want_help'):
                self.helper._help_andsdeflist()
                return
        if self.optdict.get('pattern'):
	  self.progress.start()
          apiret = self.getApi().listAnalysisDatasetDefinition(self.optdict.get('pattern'))
          self.progress.stop()
        else:
          self.progress.start()
          apiret = self.getApi().listAnalysisDatasetDefinition("*")
          self.progress.stop()
        for anObj in apiret:
		#print anObj
                self.reportAnDSDef(anObj)
        return

  def reportAnDSDef(self, anObj):
	print "\n  USING: Analysis Dataset Definition: %s" %anObj['Name']
	print "      Dataset Path: %s" %str(anObj['ProcessedDatasetPath'])
	print "         CreationDate: %s" % self.makeTIME(anObj['CreationDate'])
	print "         CreatedBy: %s" %anObj['CreatedBy']
	return	

  def handlelistMyADSDEFCall(self):
        if self.optdict.has_key('want_help'):
                self.helper._help_myadsdeflist()
                return
        print "Listing ADS DEFs from DBS MART"

        self.getALLKnownQueries()
        pp = pprint.PrettyPrinter(indent=1)
        pp.pprint(self.KnownQueries)

        print "Yet to be implemented to search for a particular ADS DEF, easy and coming!"
	print "self.optdict.get('pattern')"



  def handlelistMyADSCall(self):
	if self.optdict.has_key('want_help'):
                self.helper._help_myadslist()
                return
	print "Listing ADS from DBS MART"

	self.getALLKnownQueries()

	pp = pprint.PrettyPrinter(indent=1)
	for aQuery in self.KnownQueries.keys():
		self.printGREEN( "ADSs for ADSDEF/Query %s \n" % aQuery )
		pp.pprint(self.KnownQueries[aQuery]['MARTADS'])

	print "Yet to be implemented to search for a particular ADS, easy and coming!"
	print "self.optdict.get('pattern')"

  def getALLKnownQueries(self):
	"""
	List ALL Queries in ALL Known mart files
	Helpful for searching ADSDef/ADS etc.
	"""

	self.setMartParams()
	if self.KnownQueries!= {} : return #Never do it twice
        #adshome = os.path.expandvars(self.getApi().adshome())
        if not os.path.exists(self.adshome):
                self.printRED("ERROR: Path do not exist, ADSHOME (%s) parameter is not set or not a valid path" %str(self.getApi().adshome()))
	if not os.path.isdir(self.adshome):
		self.printRED("ERROR: Path do not exist, ADSHOME (%s) parameter is not set or not a valid path" %str(self.getApi().adshome()))

	dirList=os.listdir(self.adshome)
	for mart_file in dirList:
		if mart_file.endswith(".dm"):
    			print "Processing: %s " %mart_file
			martFile=open(os.path.join(self.adshome, mart_file), "r")
                	mart = importCode(martFile, "mart", 0)
                	self.KnownQueries=mart.KnownQueries

	return
	         
  def handlelistANDCall(self):
        if self.optdict.has_key('want_help'):
                self.helper._help_andslist()
                return
        print self.optdict.get('path') 
        self.progress.start()
	adsversion=self.optdict.get('adsversion')
	if adsversion in ("", None):
		apiret = self.getApi().listAnalysisDataset(self.optdict.get('pattern'), self.optdict.get('path'))
		#apiret = self.getApi().listAnalysisDataset()
	else:
		apiret = self.getApi().listAnalysisDataset(self.optdict.get('pattern'), self.optdict.get('path'), self.optdict.get('adsversion'))
        self.progress.stop()
        for anObj in apiret:
                #print anObj
                self.reportAnDS(anObj)
        return

  def reportAnDS(self, anObj):
        print "\n\nAnalysis Dataset: %s" %anObj['Name']
        print "         Latest Version: %s" %anObj['Version']
        print "         CreationDate: %s" % self.makeTIME(anObj['CreationDate'])
        print "         CreatedBy: %s" %anObj['CreatedBy']
        #Lets print the definition too.
        self.reportAnDSDef(anObj['Definition'])
 
  def handleListPrimaryDatasets(self):
	if self.optdict.has_key('want_help'):
		self.helper._help_primarylist()
		return
       	if self.optdict.get('pattern'):
	  self.progress.start()
          apiret = self.getApi().listPrimaryDatasets(self.optdict.get('pattern'))
	  self.progress.stop()
        else:
          self.progress.start()
          apiret = self.getApi().listPrimaryDatasets("*")
          self.progress.stop()
       	for anObj in apiret:
        	print anObj['Name']
		#print "CreationDate: %s" %self.makeTIME(anObj['CreationDate'])
                #print "LastModificationDate: %s" % self.makeTIME(anObj['LastModificationDate'])

        return

  def handleListProcessedDatasets(self):
        if self.optdict.has_key('want_help'):
		self.helper._help_procdslist()
                return

	datasetPaths = []

        if self.optdict.get('pattern') != '*':
          print "--pattern has no effect on listProcessedDataset, --path can be used for dataset patterns"
        
        paramDict = {}

        # See if Algorithm is specified for selection
        algoparam = self.getAlgoPattern()

        # See if any path is provided
        pathl = self.getPath(self.optdict.get('path'))

        if len(pathl):
		paramDict.update(pathl)
        if len(algoparam):
                paramDict.update(algoparam)

        print "Listing datasets, please wait..."
	self.progress.start()

	# Use faster call if no patter is supplied	
	if self.optdict.get('path') in (None, '', '*', '/*/*/*'):
		apiret = self.getApi().listDatasetPaths()
		print "\n"
		for aPath in apiret:
			print aPath
		if self.optdict.get('report'):
			self.printRED("\n--report has no afcet when you are not supplying --path or --algopattern")
		
		self.progress.stop()
		return
	
        apiret = self.getApi().listProcessedDatasets(**paramDict)
	self.progress.stop()

        #dot.mark_done()

        if len(apiret) < 1 :
		print "No Datasets found"
	for anObj in apiret:

	    if self.optdict.get('report'):	
		self.reportProcessedDatasets(anObj)
		#print anObj
            else:  
		  path_list = anObj['PathList']
		  if len(path_list) == 0: 
			pass
		  	#print anObj['Name'] +" No Blocks Found ?? "
		  else:	
                  	for aPath in anObj['PathList']:
                      		if aPath not in datasetPaths:
                         		datasetPaths.append(aPath)
                         		#Print on screen as well
                         		print aPath
        return

  def reportProcessedDatasets(self, anObj):
		sumry  = "\n\n\nProcessed Dataset %s " %anObj['Name']
		sumry += "\nCreationDate: %s" % self.makeTIME(anObj['CreationDate'])
		#sumry += "\nLastModificationDate: %s" % self.makeTIME(anObj['LastModificationDate'])
		
        	report = Report()
		report.addSummary(sumry)

		report.addLine("Paths in this Processed Dataset:")
		for aPath in anObj['PathList']:
			report['lines'].append("        "+aPath)
		#Print it
        	printReport(report)
                return

  def getPath(self, inpath=None):

	pathDict = {} 
	if inpath not in (None, ''):
   		if not inpath.startswith('/'):
			raise DbsException (args="Path must start with a '/'",  code="1201")
	
	else:
		if self.optdict.get('path'):
			inpath = self.optdict.get('path')

	if inpath not in (None, ''):	
		# remove the starting '/' 
   		inpath=inpath[1:]
   		if inpath.endswith('/'):
     			inpath=inpath[:-1]
   		pathl = inpath.split('/')
   		if len(pathl) < 3:
			raise DbsException (args="must provide a full qualifying --path=/?/?/?, or no --path", code="1203")
   		else:
      			pathDict['patternPrim'] = pathl[0]
      			pathDict['patternDT'] = pathl[2]
      			pathDict['patternProc'] = pathl[1]
	else :
        	pathDict['patternPrim'] = "*"
        	pathDict['patternDT'] = "*"
        	pathDict['patternProc'] = "*"

    	return pathDict

  def getAlgoPattern(self):

        algodict = {} 
        if self.optdict.get('algopattern'):
          algopat = self.optdict.get('algopattern')
          if not algopat.startswith('/'):
             raise DbsException (args="Algorithm patters starts with a '/'",  code="1200")
          algopat=algopat[1:]
          if algopat.endswith('/'):
                algopat=algopat[:-1]
          algotoks = algopat.split('/')

          if len(algotoks) >=1 : algodict['patternExe']=algotoks[0]
          if len(algotoks) >=2 : algodict['patternVer']=algotoks[1]
          if len(algotoks) >=3 : algodict['patternFam']=algotoks[2]
          if len(algotoks) >=4 : algodict['patternPS']=algotoks[3]

        return algodict
          
  def handleListAlgorithms(self):
        if self.optdict.has_key('want_help'):
		self.helper._help_algolist()
                return

        self.printBLUE( "Retrieving list of Algorithm, Please wait..." )
        algoparam = self.getAlgoPattern()
        if len(algoparam):
	     self.progress.start()
             apiret = self.getApi().listAlgorithms(**algoparam)
             self.progress.stop()
        else:
	  self.progress.start()
          apiret = self.getApi().listAlgorithms()
          self.progress.stop()

        self.printGREEN ("\nListed as:     /ExecutableName/ApplicationVersion/ApplicationFamily/PSet-Hash\n" )
        for anObj in apiret:
		#print anObj
		#print "CreationDate: %s" % self.makeTIME(anObj['CreationDate'])
		#print "LastModificationDate: %s" % self.makeTIME(anObj['CreationDate'])
                print "       /"+ anObj['ExecutableName'] \
				+ "/" + anObj['ApplicationVersion']  \
					+"/"+ anObj['ApplicationFamily'] \
						+ "/" + anObj['ParameterSetID']['Hash']

		#print "NAME:::::::", anObj['ParameterSetID']['Name']
        if (len(apiret) > 10): self.printGREEN( "\nListed as:      /ExecutableName/ApplicationVersion/ApplicationFamily/PSet-Hash\n\n" )
        return


  def handleListFiles(self):
       if self.optdict.has_key('want_help'):
		self.helper._help_filelist()
                return
       path=self.optdict.get('path') or ''
       blockpattern=self.optdict.get('blockpattern') or ''
       lfnpattern=self.optdict.get('lfnpattern') or ''
       run=self.optdict.get('run') or ''

       if path == '' and blockpattern == '' and lfnpattern=='' :
         self.printRED( "Can not list ALL files of ALL datasets, please specify a dataset path using --path= and/or --blockpattern= and/or --lfnpattern")
       else:
         self.printBLUE( "Making api call, this may take sometime depending upon size of dataset, please wait....\n")
         self.progress.start()
	 apiret = self.getApi().listFiles(path=path, blockName=blockpattern, patternLFN=lfnpattern, runNumber=run)
         self.progress.stop()
         if self.optdict.get('report') :
		for anObj in apiret:
			self.reportFile(anObj)
	 else:
                for anObj in apiret:
                	print "          %s" %anObj['LogicalFileName']
         self.printBLUE( "Total files listed: %s" %len(apiret))
         return

  def reportFile(self, anObj):
              report = Report()
              report.addSummary("                 LogicalFileName: %s" %anObj['LogicalFileName'])
              report.addLine("                    File Details:")
              report.addLine("                         Status : %s"  %anObj['Status'])
              report.addLine("                         Size : %s"  %anObj['FileSize'])
              report.addLine("                         NumberOfEvents : %s"  %anObj['NumberOfEvents'])
              report.addLine("                         Checksum : %s"  %anObj['Checksum'])
              report.addLine("                         FileType : %s"  %anObj['FileType'])
              report.addLine("                         Block : %s"  %anObj['Block']['Name'])
              report.addLine("\n")
              printReport(report)
              return

  def handlelistSECall(self):
       if self.optdict.has_key('want_help'):
		self.helper._help_selist()
                return

       sepattern=self.optdict.get('sepattern') or '*'
       self.progress.start()
       apiret = self.getApi().listStorageElements(sepattern)
       self.progress.stop()
       self.printBLUE( "Listing storage elements, please wait..." )
       for anObj in apiret:
           print anObj['Name']
       return

  def handleBlockCall(self):
       if self.optdict.has_key('want_help'):
		self.helper._help_blocklist()
                return

       path=self.optdict.get('path') or ''
       blockpattern=self.optdict.get('blockpattern') or ''
       sepattern=self.optdict.get('sepattern') or ''

       if path in ['/*/*/*', ''] and blockpattern in ['*', ''] and sepattern in ['*', '']:
         self.printRED( "Can not list ALL Blocks of ALL datasets, specify a dataset path (--path=) and/or a block name (--blockpattern=) and/or storage element (--sepattern)")
	 return
       else:
         self.printBLUE( "Listing block, please wait..." )
         self.progress.start()
         apiret = self.getApi().listBlocks(dataset=path, block_name=blockpattern, storage_element_name=sepattern)
         self.progress.stop()
         if self.optdict.get('report') :
            for anObj in apiret:
		self.reportBlock(anObj)
         else :
                for anObj in apiret:
                    print anObj['Name']
       return

  def reportBlock(self, anObj):
                sumry  = "\n     Block Name %s " %anObj['Name']
                sumry += "\n     Block Path %s" %anObj['Path']
		sumry += "\n     CreationDate: %s" % self.makeTIME(anObj['CreationDate'])
                report = Report()
                report.addSummary(sumry)
                report.addLine("     Block Details:")
                report.addLine("           BlockSize: %s" %anObj['BlockSize'])
                report.addLine("           NumberOfFiles: %s" %anObj['NumberOfFiles'])
                report.addLine("           OpenForWriting: %s" % anObj['OpenForWriting'])
                report.addLine("           This block available at:")
                for aSE in anObj['StorageElementList']:
                        report['lines'].append("                           %s" %aSE['Name'])
                #Print it
                report.addLine("\n")
                printReport(report)
                return

  def handleStoreQueryCall(self):
        if self.optdict.has_key('want_help'):
		self.helper._help_storequery()
        	return
	self.handleSearchCall()
	return


  def handleCreateADSCall(self):
        if self.optdict.get('useASearch') :
                self.printRED("You cannot use --useASearch for creating ADS")

        self.setMartParams()
        if self.optdict.has_key('want_help'):
                self.helper._help_createads()
                return

	#adsname = self.optdict.get('createADS') or ''
	#if adsname in ('', None):
	#	self.printRED("Please provide a NAME for your Analysis Dataset --createADS=<NAME>")
	#	return

	usequery=self.optdict.get('usequery') or ''
	#storequery=self.optdict.get('storequery') or ''

	if usequery in ('', None):
		self.printRED("You cannot use --createPADS without --usequery (see --doc)")
		self.printRED("Each query must be named and stored (as ADS Definition) before using it for creating ADS")
		return

	#if usequery not in ('', None) and storequery not in ('', None):
        #        self.printRED("You cannot provide BOTH --storequery and --usequery (see --doc)")
        #        return

	if usequery not in self.KnownQueries.keys():
                self.printRED("Query %s not found in the specified MART" % usequery)
                return

	self.printGREEN("Creating ADS based on query: %s " % usequery )
	martQ=self.KnownQueries[usequery]


	# See if DEF already has path, then no need to specify PATH here, else we need to pass the Path to DBS
	adspath=""
	if martQ['PATH'] in ('', None):
		adspath = self.optdict.get('path')	
		if adspath in ('', None):
			self.printRED("You must specify a dataset path for ADS, either when doing --storequery, or use --path= now")
			return

	userq=martQ['USERINPUT']
	self.printGREEN(userq)

	#if self.getApi().url != martQ['HOSTURL']:
	#	self.printRED("Query %s was created from DBS instance at %s " %(usequery, martQ['HOSTURL']) )
	#	self.printRED("You cannot use the same query for another DBS instance")
	#	return

	# Verify the validity of query
	userInput=userq.replace('where','WHERE')
        criteria=userInput.split('WHERE')
        if len(criteria) <= 0:
        	self.printRED( "Not a valid criteria for creating ADS: %s " % userInput)
        	self.printRED( "Use --help, or refer to Twiki page")
        	return

       	else: userInput="find dataset, file, lumi where "+criteria[1]
	# Craete ADS Def

	try :
		from DBSAPI.dbsAnalysisDatasetDefinition import DbsAnalysisDatasetDefinition

		adsdef = DbsAnalysisDatasetDefinition(Name=usequery,
                                         ProcessedDatasetPath=martQ['PATH'],
                                         UserInput=escape(userInput),
                                         SQLQuery=escape(martQ['QUERY']),
                                         Description="ADS DEF Created by DBS Mart from MART query %s CreatedAt %s" \
												% (usequery, martQ['CREATEDAT']),
                         )

		stdmgr=manageStdOut()
		stdmgr.capture()
		self.getApi().createAnalysisDatasetDefinition (adsdef)
		stdmgr.restore()
	except DbsApiException, ex:
		if ex.getErrorMessage().find("Already Exists") < 0:
			self.printBLUE ("Caught DBS Exception %s: %s "  % (ex.getClassName(), \
									ex.getErrorMessage() ) )
			return
		stdmgr.restore()
		self.printBLUE ("WARNING : Unable to create ADS Definition")
		self.printBLUE ("ADS Definition ALREADY EXISTS")
		self.printBLUE ("Existing Definition will be reused by the DBS instance")
		# ALL OK !. Creat ADS now
	try:
	        self.progress.start()	
		from DBSAPI.dbsAnalysisDataset import DbsAnalysisDataset


		ads=DbsAnalysisDataset(
        		Type='TEST',
                	Status='NEW',
                	PhysicsGroup='RelVal',
                	Path=adspath,
			Description="ADS Created by DBS Mart from MART query %s CreatedAt %s" \
				% (usequery, martQ['CREATEDAT']),
                )	
		print "Processing, please wait..."
                stdmgr=manageStdOut()
                stdmgr.capture()
		self.getApi().createAnalysisDataset(ads, usequery)		
		stdmgr.restore()
		self.progress.stop()
		self.printGREEN("Analysis Dataset Created")
        except DbsApiException, ex:
		stdmgr.restore()
		self.progress.stop()
                self.printRED ("Unable to create ADS")
		if ex.getErrorMessage().find("Already Exists") < 0:
                        self.printBLUE ("Caught DBS Exception %s: %s "  % (ex.getClassName(), \
                                                                        ex.getErrorMessage() ) )
                        return
		self.printBLUE ("WARNING : Unable to create ADS")
		msg=ex.getErrorMessage()
		msg=msg.split("Already Exists,")[1].split("already exists")
		self.printBLUE ("WARNING : "+str(msg[0])+" Already exists")
	return

  def handleCreatePADSCall(self, path="", files=[]):
	if self.optdict.get('useASearch') :
		self.printRED("You cannot use --useASearch for creating PADS")

	self.setMartParams()
	if self.optdict.has_key('want_help'):
		self.helper._help_createpads()
		return

	# Get the Definition name
	# see if user provide --usequery
	storequery=''
	usequery=self.optdict.get('usequery') or ''
        if usequery in ('', None):
		storequery=self.optdict.get('storequery') or ''	
		
		if storequery in ('', None):
			self.printRED("You cannot use --createPADS without --storequery or --usequery (see --doc)")
			self.printRED("Each query must be named and stored (as ADS Definition) before using it for creating PADS/ADS")
			return
		else : que = storequery
	else : 
		que = usequery

	if usequery not in ('', None) and storequery not in ('', None):
                self.printRED("You cannot provide BOTH --storequery and --usequery (see --doc)")
		return
	
	if que not in self.KnownQueries.keys():
		self.printRED("Query not found in the specified MART")
		return

	# Checks on ADS begin here
	adsname=self.optdict.get('createPADS') or ''	
	if adsname in ('', None):
		self.printRED("You must provide a valid ADS name --createPADS=<PADSNAME>")


        ads_list=self.KnownQueries[que]['MARTADS']
	if adsname in ads_list.keys():
		self.printRED("An ADS with same name already exists in the MART")
		return
			
	adspath = os.path.join(self.adshome, adsname)

	if os.path.exists(adspath):
		# That must be checked in the MART file as well
		self.printRED("ERROR: Cannot create %s, An analysis Dataset with same name already exists" %str(adspath))
		return False

	# Get Path etc from and query DBS to get the list of files
	if files in ([], None):
		# Get Path etc from and query DBS to get the list of files
		print "SEARCHING for Files based on QUERY: %s for PATH : %s " %(que, self.KnownQueries[que]['PATH'])

		userq=self.KnownQueries[que]['USERINPUT']
        	userInput=userq.replace('where','WHERE')
        	criteria=userInput.split('WHERE')
        	if len(criteria) <= 0:
                	print "Not a valid criteria for creating PADS/ADS"
                	print "Use --help, or refer to Twiki page"
                	return

        	else: userInput="find dataset, file, lumi where "+criteria[1]

    		if self.optdict.get('useASearch') :
        		results=self.getDataFromDDSearch(userInput)
    		else: results=self.getDataFromDBSServer(userInput)

	    	path=results['DATASETPATH'].strip()
    		files=results['ADSFileList']

	# OK, Let there be an ADS
	newADS={}
	newADS['PATH']=path
	newADS['EXISTS_IN_DBS']='false'
	newADS['CREATEDAT']=time.asctime()
	newADS['LASTMODIFIEDAT']=time.asctime()
        newADS['CREATEDBY']=os.environ['USER']
	newADS['LASTMODIFIEDBY']=os.environ['USER']

	self.KnownQueries[que]['MARTADS'][adsname]=newADS
	self.rewriteMart()

	# Lets write the ADS file
	ads_file=open(adspath, 'w')
	ads_file.write("<?xml version='1.0' standalone='yes'?>")
	ads_file.write("\n<!-- DBS Version 1 -->")
	ads_file.write("\n<dbs>")
	ads_file.write("\n<src url='%s' />" % self.getApi().url())
	ads_file.write("\n<dataset path='%s' />" % path)
	ads_file.write("\n<analysis_dataset_def name='%s' query='%s' />" % (que, self.KnownQueries[que]['QUERY']) )	
	for aFile in files:
		ads_file.write("\n<file lfn='%s' />" % aFile['LogicalFileName'])
	ads_file.write("\n</dbs>")
        ads_file.close()
	print "Created Personal ADS: %s" % adspath

	return

  def handleCreateCFGCall(self):
        """
        Generates CFF file
        """
        if self.optdict.has_key('want_help'):
                self.helper._help_createcfg()
                return
	#handleCreateCFFCall also does what handleCreateCFG needs
	self.handleCreateCFFCall()
	return

 
  def handleCreateCFFCall(self):
	"""
	Generates CFF file
	"""
        if self.optdict.has_key('want_help'):
                self.helper._help_createcff()
                return

	usequery=self.optdict.get('usequery') or ''

	if usequery not in ('', None):
		self.setMartParams()
        	usequery=self.optdict.get('usequery') or ''
        	#storequery=self.optdict.get('storequery') or ''

        	if usequery in ('', None):
                	self.printRED("You cannot use --createPADS without --usequery (see --doc)")
                	self.printRED("Each query must be named and stored (as ADS Definition) before using it for creating ADS")
                	return

        	if usequery not in self.KnownQueries.keys():
                	self.printRED("Query %s not found in the specified MART" % usequery)
                	return

        	#self.printGREEN("Creating ADS based on query: %s " % usequery )
        	martQ=self.KnownQueries[usequery]
        	if martQ['PATH'] in ('', None):
                	martQ['PATH'] = self.optdict.get('path')
                	if martQ['PATH'] in ('', None):
                        	self.printRED("You must specify a dataset path for ADS, either when creating query, or use --path=")
                        	return

        	userq=martQ['USERINPUT']
        	#self.printGREEN(userq)
	        # Verify the validity of query
	        userInput=userq.replace('where','WHERE')
        	criteria=userInput.split('WHERE')
        	if len(criteria) <= 0:
                	self.printRED( "Not a valid criteria for creating ADS: %s " % userInput)
                	self.printRED( "Use --help, or refer to Twiki page")
               		return

                if self.optdict.get('useASearch') :
                        results=self.getDataFromDDSearch(userInput)
                else: results=self.getDataFromDBSServer(userInput)

                files=results['ADSFileList']
		if self.optdict.get('createCFG') : 
			self.printGREEN("Creating crab.cfg based on query: %s " % usequery )
			self.generateCFG(martQ['PATH'])

		createCFF=self.optdict.get('createCFF') or ''
		if createCFF not in ('', None): 
			self.printGREEN("Creating .cff based on query: %s " % usequery )
			self.generateCFF(createCFF, files)
	else:
        	self.handleSearchCall()
        return

  def generateCFF(self, cffName, cfffileslist):

	self.setMartParams()

	if cffName.endswith(".cff"):
		cffpath=self.adshome+"/"+cffName
	else: 
		cffpath=self.adshome+"/"+cffName+".cff"

	# Lets write the CFF file
	cff_file=open(cffpath, 'w')
	cff_file.write("\n")
	cff_file.write("\nreplace PoolSource.fileNames = {\n")
	cnt=len(cfffileslist)
	for aFile in cfffileslist:
		if aFile != cfffileslist[cnt-1]:
			cff_file.write("\n'"+aFile['LogicalFileName']+"',")
		else:
			cff_file.write("\n'"+aFile['LogicalFileName']+"'")

	cff_file.write("\n}\n")
	cff_file.close()
	self.printGREEN("Generated %s in %s" % (cffpath, self.adshome) )
	return

  def generateCFG(self, path):#, cfffileslist):

	if path in ('', None):
		self.printRED("No dataset path specified in query or commandline (use --path=)")
		return

        self.setMartParams()
	crabfpath=self.adshome+"/"+"crab.cfg"
	crabf=open(crabfpath, 'w')
	crabf.write("[CRAB]")
	crabf.write("\njobtype                 = cmssw")
	crabf.write("\nscheduler               = glitecoll")

	crabf.write("\n\n[CMSSW]")
	crabf.write("\ndatasetpath             = %s" %path)
	crabf.write("\npset                    = ")
	crabf.write("\ntotal_number_of_events  = -1")
	crabf.write("\nevents_per_job          = 1000")
	crabf.write("\noutput_file             = ")

	crabf.write("\n\n[USER]")
	crabf.write("\nreturn_data             = 1")
	crabf.write("\nuse_central_bossDB      = 0")
	crabf.write("\nuse_boss_rt             = 0")

	crabf.write("\n\n[EDG]")
	crabf.write("\nrb                      = CERN ")
	crabf.write("\nproxy_server            = myproxy.cern.ch ")
	crabf.write("\nvirtual_organization    = cms")
	crabf.write("\nretry_count             = 0")
	crabf.write("\nlcg_catalog_type        = lfc")
	crabf.write("\nlfc_host                = lfc-cms-test.cern.ch")
	crabf.write("\nlfc_home                = /grid/cms")
	crabf.close()
	self.printGREEN("Generated %s/crab.cfg in %s" % (self.adshome,self.adshome) )
	return

  def handleUseQueryCall(self):
	if self.optdict.has_key('want_help'):
		self.printRED("\n--usequery Can only be used with --createPADS or --createADS call\n")
		self.printRED("\nTry --doc for more information.\n")
		return

	# Check to see if its a createADS or createPADS call (Cannot be both)
	createPADS=self.optdict.get('createPADS') or ''
	createADS=self.optdict.get('createADS') or ''
    	createCFF=self.optdict.get('createCFF') or ''
    	createCFG=self.optdict.get('createCFG') or ''

	if createPADS in ('', None) and createADS in ('', None) \
			and createCFF in ('', None) and createCFG in ('', None) :
		self.printRED("\n--usequery Can only be used with --createPADS, --createADS, --createCFG or --createCFF calls\n")
		self.printRED("\nTry --doc for more information.\n")
                return

	if createPADS not in ('', None):
		self.handleCreatePADSCall()
	if createADS not in ('', None):
		self.handleCreateADSCall()
	if createCFF not in ('', None):
		self.handleCreateCFFCall()
	if createCFG not in ('', None):
                self.handleCreateCFGCall()

  def getDBSversion(self):

	if self.optdict.get('url') in ('BADURL', None, ''):
		print "BADURL, please specify using --url="
		raise Exception("-1111", "BADURL, please specify using --url=")
		return

	if self.optdict.get('url').startswith('https:'):
		if not dbsAvailable:
			print "You are using HTTPS URL, you will need to setup DBS Client environment first"
			raise Exception("-1111", "You are using HTTPS URL, you will need to setup DBS Client environment first")
			return

        params = {'apiversion': 'DBS_2_0_4' ,'api':'getDBSServerVersion'}
        params = dict(params)
        data = urllib2.urlopen(self.optdict.get('url'), urllib.urlencode(params, doseq=True))
        res = data.read()
        for line in res.split():
            if  line.find('server_version') != -1:
                dbsver = line.split('=')[-1]
                dbsver = dbsver.replace("'","").replace('"','')
		if dbsver.find("_pre") != -1: 
			dbsver = dbsver.split("_pre")[0]
		if dbsver.find("patch") != -1:
			dbsver=dbsver.split("_patch")[0]
                return dbsver
        return None

  def getDataFromDBSServer(self, userInput, qu="exe"):
        if self.optdict['donotrunquery']:
               print "\n--donotrunquery specified, will not execute the query"
               qu="query"
	
        self.progress.start()
	if userInput.find("dataset") != -1 and not self.optdict.get('production') :
		if userInput.strip()=="find dataset" :
			userInput += " where dataset.status like VALID* "
		elif len(userInput.split()) == 2:
			if userInput.split()[1].strip()=="dataset":
				userInput += " where dataset.status like VALID* "
		else:
			if userInput.find("order") != -1 :
				userInput = userInput.replace("order", " and dataset.status like VALID* order ")	
			else:
				userInput += " and dataset.status like VALID* "
	#print userInput
	if dbsAvailable: 
		data=self.getApi().executeQuery(query=userInput, type=qu, begin=self.optdict.get('begin'), end=self.optdict.get('end'))
		self.apiversion=self.getApi().getApiVersion()
	else : 
		dbsver = self.getDBSversion()
		
		if  not dbsver or dbsver >= 'DBS_2_0_6':
			self.apiversion=dbsver
		else:
			self.apiversion='DBS_2_0_5'
		params = {'apiversion': self.apiversion ,'api':'executeQuery', 
					'begin' : self.optdict.get('begin'), 'end' : self.optdict.get('end')}
		params = dict(params)
		params['query']=userInput
		params['type']=qu
		res = urllib2.urlopen(self.optdict.get('url'), urllib.urlencode(params, doseq=True))
		data = res.read()

        self.progress.stop()
	# parse according to Server version, DBS_2_0_6 have different XML coming from server
        if not self.apiversion or self.apiversion >= 'DBS_2_0_6':
            return self.parseResults(data)
        else:
            return self.parseReultsOldStyle(data)

  def getDataFromDDSearch(self, userInput):

    print "Usage of ASearch is deprecated from DBS_2_0_6"
    sys.exit(0) 

  def parseResults(self, data):

    results={}
    results['QUERY']=''
    results['USERINPUT']=''
    results['DATASETPATH']=''
    results['ADSFileList']=[]

    if self.optdict.has_key('quiet'):
                quiet=self.optdict.get('quiet')
   
    noheader=False 
    if self.optdict['noheader']:
		noheader=True

    if self.optdict.get('xml'):
	print data
	return results

    try:
        # DbsExecutionError message would arrive in XML, if any
        class Handler (xml.sax.handler.ContentHandler):

	   def __init__(self):
		xml.sax.handler.ContentHandler.__init__(self)
	   	self.next_is_query=0
		self.next_is_userinput=0
		self.next_is_dataset=0
		self.new_file=0
		self.dataset_once=1
		self.start_print=0
		self.titleList=[]
		self.title=""
		self.printme=""
		self.first_time_result=1	
		self.do_date=0

           def startElement(self, name, attrs):
		#print name
		if name.find('moddate') != -1 :
			self.do_date=1 
		if name.find('createdate') != -1 :
			self.do_date=1
		if name == 'sql':
			self.next_is_query=1
		
		if name == 'userinput':
			self.next_is_userinput=1

           	if name == 'dataset' and self.dataset_once==1:
			self.next_is_dataset=1
			self.dataset_once=0
		if name == 'file':
			self.new_file=1
		# That sucks 
		if name == 'timeStamp':
			self.next_is_userinput=0
		if self.start_print:
			if name not in self.titleList: self.titleList.append(name)
			self.title+=str(name)+'\t'
			#print "NAME where start_print==1 %s " % str(name)
			#print self.title
		if name == 'row':
			self.start_print=1
			#cout=""
		#Add extra space to line to be printed ONLY if the TAG is one of the asked-for rows
		if name in self.titleList:
			self.printme+="   "

	   def characters(self, s):

		if str(escape(s)).strip() in ("\n", ""): return
		elif self.next_is_query==1:
			results['QUERY'] += str(escape(s))
			return

		elif self.next_is_userinput==1:
                        results['USERINPUT']+=str(escape(s))
                        return
		elif self.next_is_dataset==1:
			results['DATASETPATH']+=str(escape(s))
			#return
		elif self.new_file==1:
			aFile={}
			aFile['LogicalFileName']=str(escape(s))
			results['ADSFileList'].append(aFile)
			#return
		else: pass
		# It is worth printing
		if not quiet: 
			if self.start_print:
				#cout += str((escape(s)).strip())
				#print (escape(s)).strip()
				if self.do_date:
					self.printme+=time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime(long((escape(s)).strip())))
					self.do_date=0
				else:
					self.printme+=str((escape(s)).strip())
					

	   def endElement(self, name):
		#print name
		if name=='sql':
			self.next_is_query=0
		if name == 'userinput':
                        self.next_is_userinput=0
		if name == 'dataset':
                        self.next_is_dataset=0
                if name == 'file':
                        self.new_file=0
		if name == 'row':
			
			if self.first_time_result:
				if not noheader:
					print "-------------------------------------------------------"
					print self.title+"\n"
				self.first_time_result=0
			print "%s" % self.printme.strip()
                        self.printme=""
			self.start_print=0


        xml.sax.parseString (data, Handler ())

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server not responding as desired %s" % self.Url
      raise DbsConnectionError (args=msg, code="505")

    #print results
    return results

  def parseReultsOldStyle(self, data):

        results={}
        results['QUERY']=''
        results['USERINPUT']=''
        results['DATASETPATH']=''
        results['ADSFileList']=[]

        if self.optdict.has_key('quiet'):
                quiet=self.optdict.get('quiet')

        if self.optdict.get('xml'):
		print data
		return results
        try:
        # DbsExecutionError message would arrive in XML, if any
         class Handler (xml.sax.handler.ContentHandler):

           def __init__(self):
                xml.sax.handler.ContentHandler.__init__(self)
                self.next_is_query=0
                self.next_is_userinput=0
                self.next_is_dataset=0
                self.new_file=0
                self.dataset_once=1
                self.sql_once=1
                self.columns = True

           def startElement(self, name, attrs):
                if name == 'sql':
                        self.next_is_query=1

                if name == 'userinput':
                        self.next_is_userinput=1

                # That sucks 
                if name == 'timeStamp':
                        self.next_is_userinput=0


                if name == 'result':
                        #result FILES_LOGICALFILENAME   PATH
                        out=""
                        if self.columns:
                                self.columns = False
                                cout = ""
                                for akey in attrs.keys():
                                        cout += akey + '\t'
                                print cout
                                print '_________________________________________________________________________________'

                        for akey in attrs.keys():
                                out += str(attrs[akey]) + '\t'

                        #for aval in attrs.values():
                        #       out += "    " +str(aval)
                        if not quiet: print out
                        if  attrs.has_key('FILES_LOGICALFILENAME'):
                                aFile={}
                                aFile['LogicalFileName']=str(attrs['FILES_LOGICALFILENAME'])
                                results['ADSFileList'].append(aFile)
                        if self.dataset_once==1:
                                if  attrs.has_key('PATH'):
                                        results['DATASETPATH']+=str(escape(attrs['PATH']))
                                        self.dataset_once=0

           def characters(self, s):

                if str(escape(s)).strip() in ("\n", ""): return
                elif self.next_is_query==1 and self.sql_once==1:
                        results['QUERY'] += str(escape(s))
                        return

                elif self.next_is_userinput==1:
                        results['USERINPUT']+=str(escape(s))
                        return
                else: pass

           def endElement(self, name):
                if name=='sql':
                        self.next_is_query=0
                        self.sql_once=0
                if name == 'userinput':
                        self.next_is_userinput=0

         xml.sax.parseString (data, Handler ())

        except SAXParseException, ex:
         msg = "Unable to parse XML response from DBS Server"
         msg += "\n  Server not responding as desired %s" % self.Url
         raise DbsConnectionError (args=msg, code="505")
        #print results
        return results

  def handleSearchCall(self):

    """
    	Interface to DBS Discovery 

    """

    if self.optdict.has_key('want_help'):
         self.helper._help_search()
         return
    userInput=self.optdict.get('query') or ''

    if userInput in ('=') or userInput.startswith('='):
	print "Do you have empty spaces before and/or after --query ? that is invalid, please use --query=\"find ...\""
	return

    if userInput in ('', None):
	print "No search criteria specified, Use --query= to specify a serach query (Do you want --usequery=?)"
	return

    storequeryname=self.optdict.get('storequery') or ''
    createPADS=self.optdict.get('createPADS') or ''
    createADS=self.optdict.get('createADS') or ''
    createCFF=self.optdict.get('createCFF') or ''
    storetemplt=self.optdict.get('storetemplatequery') or ''

    if storequeryname not in ('', None) and storetemplt not in ('', None):
	self.printRED("You cannot specify both --storequery and --storetemplatequery")
	return

    if storetemplt not in ('', None):
        if userInput.find('dataset') != -1:
                self.printRED("You cannot specify dataset clause when storing a template query")
                return

    qu="exe"
    if storequeryname not in ('', None) and storetemplt not in ('', None) \
        and createCFF in ('', None) and createPADS in ('', None) and createPADS in ('', None):
        qu="query"  # Run ONLY query and do not execute

    #if createCFF not in ('', None):
    #	qu="exe"
    # ONLY do this if User needs to create an ADS
    if createPADS not in ('', None) or createADS not in ('', None):
	#qu="exe"
	userInput=userInput.replace('where','WHERE')
    	criteria=userInput.split('WHERE')
    	if len(criteria) <= 0:
		self.printRED("Please provide a valid criteria for search, use where or WHERE clause")
		self.printRED("Use --help, or refer to Twiki page")
		return

   	userInput="find dataset, file, lumi where "+criteria[1]

    # NEED to perform search to store certain information about the query
    if self.optdict.get('useASearch') :
		results=self.getDataFromDDSearch(userInput)
    else: results=self.getDataFromDBSServer(userInput, qu)

    datasetPath=results['DATASETPATH'].strip()
    if storetemplt not in ('', None):
	storequeryname=storetemplt
	#Also the datasetPath should be empty for a Template ADSDef
	datasetPath=""
    
    adsfileslist=results['ADSFileList']

    if storequeryname not in ('', None):
                #if len(datasetPaths) > 1:
                #        self.printRED("Cannot create ADS, more than one matching Paths found for query, limit to ONE Path")
                #        return
                canstore=self.storeQuery(results['DATASETPATH'], results['USERINPUT'], results['QUERY'], storequeryname)
                if (canstore) and createPADS not in ('', None):
           		if datasetPath in ('', None) or len(adsfileslist) <= 0 :
				self.printRED("Cannot create PADS, No matching Dataset (With Files and LumiSections) found")
				self.printRED("Used Query %s" % results['USERINPUT'])
				return
                       	self.handleCreatePADSCall(datasetPath, adsfileslist)
		if (canstore) and createADS not in ('', None):
			#handleCreateADSCall only knows about usequery, so we can set it here
			self.optdict['usequery']=storequeryname
			self.handleCreateADSCall()

    if createPADS not in ('', None) and storequeryname in ('', None):
                self.printRED("You cannot use --createPADS without --storequery (see --doc)")
                self.printRED("Each query must be named and stored (as ADS Definition) before using it for creating PADS")

    if createADS not in ('', None) and storequeryname in ('', None):
                self.printRED("You cannot use --createADS without --storequery in this case (see --doc)")
                self.printRED("Each query must be named and stored (as ADS Definition) before using it for creating ADS")

    # CAN Create CFF without storing query
    if createCFF not in ('', None):
		self.generateCFF(createCFF, adsfileslist)

    if self.optdict.get('createCFG') : self.generateCFG(datasetPath)

    return

  def storeQuery(self, path, userquery, sqlquery, queryname):
	"""
	Created the ADS in DBS Local ADS File format
		More info will be passed and added to this method at later stage
	"""

	self.setMartParams()
	# The fun begins
	adsdef = {}
	adsdef['PATH']=path
	adsdef['USERINPUT']=unescape(userquery)
	adsdef['QUERY']=sqlquery
	adsdef['EXISTS_IN_DBS']='false'
	adsdef['HOSTURL']=self.getApi().url()
	adsdef['CREATEDAT']=time.asctime()
	adsdef['LASTMODIFIEDAT']=time.asctime()
        adsdef['CREATEDBY']=os.environ['USER']
        adsdef['LASTMODIFIEDBY']=os.environ['USER']
	adsdef['MARTADS']={}

        if queryname in self.KnownQueries.keys():
		self.printRED("ERROR: Cannot create %s, An Analysis Dataset Definition (query) with same name already exists in the mart %s" \
									% (queryname, self.mart_file))
		if self.optdict.get('createPADS') not in ('', None):
			self.printRED(".....Trying to use this existing query to create ADS.....")
			#self.printRED("Try using --usequery=<QUERYNAME> with --createPADS")
		return True
	else:
		self.KnownQueries[queryname]=adsdef
		#print self.KnownQueries

	self.rewriteMart()
	self.printBLUE("Query %s stored in the mart %s" % (queryname, self.mart_file))
        return True

  def rewriteMart(self):

	mart = open(str(self.mart_file), "w")
	mart.write("#File Last Update %s" % str(time.asctime()))
	mart.write("\nKnownQueries = ")

	pp = pprint.PrettyPrinter(indent=1)
        #pp.pprint(self.KnownQueries)
	safestr=pp.pformat(self.KnownQueries)

	mart.write(safestr)
	mart.close()

# main
#
if __name__ == "__main__":

  opts = {}
  args = []

  #try:

  optManager  = DbsOptionParser()
  (opts,args) = optManager.getOpt()

  ApiDispatcher(opts)

  """
  except Exception, ex:
    try:
      message=ex.getErrorMessage()
      code=ex.getErrorCode()
    except:
	try:
        	(code, message) = ex
    	except:
        	code = 0
        	message = ex
    print "Error: " + str(message)
    print "Error Code: "  + str(code) 

  """
