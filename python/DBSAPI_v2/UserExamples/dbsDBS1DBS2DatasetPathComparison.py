#!/usr/bin/env python
#
#
#

import os, re, string, xml.sax, xml.sax.handler, sys
from xml.sax.saxutils import escape
from cStringIO import StringIO

# DBS specific modules
from DBSAPI.dbsApi import DbsApi 
from DBSAPIOLD.dbsCgiApi import DbsCgiApi

from DBSAPIOLD.dbsException import DbsException
import DBSAPIOLD

from DBSAPI.dbsHttpService import DbsHttpService
from DBSAPI.dbsExecService import DbsExecService

from DBSAPI.dbsException import DbsException
from DBSAPI.dbsApiException import *

from DBSAPI.dbsBaseObject import *
from DBSAPI.dbsConfig import DbsConfig
from DBSAPI.dbsOptions import DbsOptionParser

from exceptions import Exception

out = open("DBS1DBS2Compare.out", "w")
err = open("DBS1DBS2Compare.err", "w")


optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)
DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquerytest2"
args = {}
args['instance'] = 'MCGlobal/Writer'

dbs1Api = DbsCgiApi(DEFAULT_URL, args)

inputPairs=open("DBS2MigrRename_p.txt", "r").readlines()
for aline in inputPairs:
 if not aline.startswith('/'): continue # A non datasetPath line
 dbs1dbs2paths = aline.split()
 dbs1Path=dbs1dbs2paths[0]
 dbs2Path=dbs1dbs2paths[1]
 out.write("\n*****Starting processing Pair DBS1:%s and DBS2:%s*******" %(dbs1Path, dbs2Path))
 try:

  dbs1FileList=[]
  dbs2FileList=[]
 
  dbs1LFNList=[]
  dbs2LFNList=[]
  error_flag = 0

  out.write( "\nRetrieving files for DBS1 dataset..%s"% dbs1Path)
  for block in dbs1Api.getDatasetFileBlocks (dbs1Path):
	fileList=block.get('fileList')
    	dbs1FileList.extend(fileList)
  out.write("\nTotal files retrieved for DBS1 datasetPath: %s is:%s" %(dbs1Path, str(len(dbs1FileList))))

  if len(dbs1FileList) < 1:
      err.write( "\n*******ERROR dataset in DBS1: %s has no files*************" %dbs1Path)
  else:

     for dbs1file in dbs1FileList:
         out.write("\n"+ dbs1file['logicalFileName'])
         dbs1LFNList.append(dbs1file['logicalFileName']) 
  
     out.write( "\n--------------------------------------------------------------------------" )
     out.write( "\nRetrieving files for DBS2 dataset..%s"% dbs2Path )
     dbs2FileList=api.listFiles(path=dbs2Path)

     if len(dbs2FileList) < 1:
         err.write( "\n*******ERROR dataset in DBS2: %s has no files ( Is its transferred yet? )*************" %dbs2Path)
     else:
         out.write( "\nTotal files retrieved for DB2 datasetPath: %s is:%s" %(dbs2Path, str(len(dbs2FileList))))
         for dbs2file in dbs2FileList:
             out.write("\n"+ dbs2file['LogicalFileName'])
             dbs2LFNList.append(dbs2file['LogicalFileName'])

         if len(dbs1FileList) != len(dbs2FileList):
            err.write( "\n******ERROR: Number of files in DBS1 dataset:%s doesn't match Number of file in DBS2: %s (%s !=  %s) " \
			%(dbs1Path, dbs2Path, str(len(dbs1FileList)), str(len(dbs2FileList))))
         else:
            out.write( "\n******PASS 1: Number of files in DBS1 dataset:%s matches Number of file in DBS2: %s (%s ==  %s) " \
                        %(dbs1Path, dbs2Path, str(len(dbs1FileList)), str(len(dbs2FileList))))

     for dbs1lfn in dbs1LFNList:
         if dbs1lfn not in dbs2LFNList:
            error_flag = 1 
            err.write( "\n******ERROR: File %s exists in DBS1 dataset: %s But not in DBS2 dataset: %s" \
			%(dbs1lfn, dbs1Path, dbs2Path))

     if error_flag != 1: out.write("\n******PASS 2: All DBS1 dataset:%s files are same as in DBS2 dataset:%s ******" \
			%(dbs1Path, dbs2Path))
     error_flag = 0 
     for dbs2lfn in dbs2LFNList:
         if dbs2lfn not in dbs1LFNList:
            error_flag = 1
            err.write( "\n******ERROR: File %s exists in DBS1 dataset: %s But not in DBS2 dataset: %s" \
                        %(dbs2lfn, dbs1Path, dbs2Path))

     if error_flag != 1: out.write("\n******PASS 3: All DBS2 dataset:%s files are same as in DBS1 dataset:%s ******"\
			%(dbs2Path, dbs1Path))

 except Exception, ex:
  out.write( "\nException raised: %s" %ex)
  pass

out.write( "\nDone")

out.close()
err.close()


