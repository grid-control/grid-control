#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Copyright 2006 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2006

"""
DBS Web API
"""

# system modules
import os, re, string, xml.sax, xml.sax.handler
from xml.sax.saxutils import escape
from cStringIO import StringIO

# DBS specific modules
from dbsApi import *

class DbsWebApi(DbsApi):
  """
  DbsWebApi class, provides a wrapper around DbsApi for Web application, e.g.
  DBS/DLS discovery page
  """ 

  def __init__(self, Args={}):
    """ 
    Constructor. 
    """

    DbsApi.__init__(self,Args)

  #-------------------------------------------------------------------

  def listBlocks(self, dataset="*"):
    """
    Retrieve list of Blocks matching a shell glob pattern.
    Returns a list of DbsFileBlock objects.  If the pattern is
    given, it will be matched against the content as a shell glob
    pattern.

    May raise an DbsApiException.

    Please note, that it has the same logic and code as in DbsApi, but return
    type is different, it is dictionary.
    """

    path = get_path(dataset)
    data = self._server._call ({ 'api' : 'listBlocks', 'path' : path }, 'GET')

    # Parse the resulting xml output.
    try:
      result={}
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'block':
               # to be backward compatible I need the following structure
               # blocks[name]=[evts,str(attrs['status']), long(attrs['files']), long(attrs['bytes'])]
               # for time being evts=0, once schema will support them I'll update this field
               evts=int(attrs['number_of_events'])
               status=str(attrs['open_for_writing'])
               files=int(attrs['number_of_files'])
               bytes=long(attrs['size'])
               result[str(attrs['name'])]=[evts,status,files,bytes]

      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  # ------------------------------------------------------------
  def listBlocksFull(self, dataset="*",block_name="*",storage_element_name="*"):
      return super(DbsWebApi,self).listBlocks(dataset,block_name,storage_element_name)
  # ------------------------------------------------------------
  def listProcessedDatasets(self, patternPrim="*", patternDT="*", patternProc="*",   
                                  patternVer="*", patternFam="*", patternExe="*", patternPS="*"):
    """
    Retrieve list of processed datasets matching a shell glob patterns.
    Returns a list of DbsProcessedDataset objects.  If the pattern(s) are
    given, they will be matched against the dataset primary dataset, data tier, application version,  etc. as a shell glob
    pattern.

    May raise an DbsApiException.

    """
#    return super(DbsWebApi,self).listProcessedDatasets(patternPrim,patternDT,patternProc,patternVer,patternFam,patternExe,patternPS)

    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listProcessedDatasets', 
		    'primary_datatset_name_pattern' : patternPrim, 
		    'data_tier_name_pattern' : patternDT, 
		    'processed_datatset_name_pattern' : patternProc, 
		    'app_version' : patternVer, 
		    'app_family_name' : patternFam, 
		    'app_executable_name' : patternExe, 
		    'ps_hash' : patternPS }, 
		    'GET')
 
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        
	def startElement(self, name, attrs):
	  if name == 'processed_dataset':
             self.procName = str(attrs['processed_datatset_name'])
             self.primName = str(attrs['primary_datatset_name'])
          if name == 'data_tier':
             # FIXME: so far in test some datasets doesn't have tiers, I don't know what to do
             # about them since we don't decided what would be dataset name means, either
             # dataset=/prim/tier/proc or dataset=/prim/proc. The following block need to be fixed
             # once decision is made
             if str(attrs['name']):
                n = "/"+self.primName+"/"+str(attrs['name'])+"/"+self.procName
                result.append(DbsProcessedDataset(datasetPathName=n))
#             else:
#                 n = "/"+self.prim+"/"+'None'+"/"+self.proc
#                 result.append(DbsProcessedDataset(datasetPathName=n))

      xml.sax.parseString (data, Handler ())
      return result

    except DbsException, ex:
	raise DbsBadResponse(exception=ex)
    except Exception, ex:
	raise DbsBadResponse(exception=ex)

  def getDatasetDetails(self, patternPrim="*", patternDT="*", patternProc="*",   
                              patternVer="*", patternFam="*", patternExe="*", patternPS="*"):
      return super(DbsWebApi,self).listProcessedDatasets(
                         patternPrim=patternPrim,patternDT=patternDT,patternProc=patternProc,
                         patternVer=patternVer,patternExe=patternExe,patternPS=patternPS)
  # ------------------------------------------------------------
  def getLFNs(self, blockName , dataset = None):
    """
    Retrieve list of LFNs in a dataset and/or in a block
    Returs a list of lfn,size,status,type,events.

    May raise an DbsApiException.

    """
    path = get_path(dataset)
    patternLFN="*"
    # Invoke Server.
    data = self._server._call ({ 'api' : 'listFiles', 'path' : path, 'block_name' : blockName, 'pattern_lfn' : patternLFN }, 'GET')

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'file':
             lfn = str(attrs['lfn'])
             size=int(attrs['size'])
             events=int(attrs['number_of_events'])
             status=str(attrs['status'])
             type=str(attrs['type'])
             result.append((lfn,size,status,type,events))
      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)


  # ------------------------------------------------------------
  def listApplications(self, patternVer="*", patternFam="*", patternExe="*", patternPS="*"):
    """
    Retrieve list of applications/algorithms matching a shell glob pattern.
    User can base his/her search on patters for Application Version, 
    Application Family, Application Executable Name or Parameter Set.

    returns:  list of DbsApplication objects.  
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())


    # Invoke Server.
    data = self._server._call ({ 'api' : 'listAlgorithms',
		    'app_version' : patternVer, 
		    'app_family_name' : patternFam, 
		    'app_executable_name' : patternExe, 
		    'ps_hash' : patternPS }, 
		    'GET')
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):

	  if name == 'algorithm':
             app = "/%s/%s/%s"%(str(attrs['app_version']),str(attrs['app_family_name']),str(attrs['app_executable_name']))
             if not result.count(app):
                result.append(app)

      xml.sax.parseString (data, Handler ())
      return result
    except Exception, ex:
      raise DbsBadResponse(exception=ex)

