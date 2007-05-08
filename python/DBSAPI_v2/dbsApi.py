#!/usr/bin/env python
#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"
#
# DBS API class. Interfacing to Server using http/https or local
#

# system modules
import os, re, string, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

# DBS specific modules
from dbsHttpService import DbsHttpService
from dbsExecService import DbsExecService

from dbsException import DbsException
from dbsApiException import *

from dbsBaseObject import *
from dbsRun import DbsRun 
from dbsQueryableParameterSet import DbsQueryableParameterSet
from dbsProcessedDataset import DbsProcessedDataset
from dbsPrimaryDataset import DbsPrimaryDataset
from dbsLumiSection import DbsLumiSection 
from dbsFile import DbsFile
from dbsFileBlock import DbsFileBlock
from dbsDataTier import DbsDataTier 
from dbsStorageElement import DbsStorageElement 
from dbsFileBranch import DbsFileBranch 
from dbsAlgorithm import DbsAlgorithm
from dbsAnalysisDataset import DbsAnalysisDataset
from dbsAnalysisDatasetDefinition import DbsAnalysisDatasetDefinition

from dbsParent import DbsParent
from dbsConfig import DbsConfig
import urlparse
import urllib2

import logging
import inspect

from dbsLogger import *

#DBS Api version
__version__ = "v00_00_06"


# DBS Defined Log Levels
#DBSDEBUG=1
#DBSINFO=2
#DBSWARNING=3


def getInt(value = None):
	if (value == None ) :
		return 0
	if (len(value) < 1 ) :
		return 0
	return int(value)

def getLong(value = None):
	if (value == None ) :
		return 0
	if (len(value) < 1 ) :
		return 0
	return long(value)

class DbsApi(DbsConfig):
  """
  DbsApi class, provides access to DBS Server, 
  all clients must use this interface 
  """ 

  def __init__(self, Args={}):
    """ 
    Constructor. 
    Initializes the DBS Api by reading configuration 
    parameters from dbs.config file OR from the parameters 
    passed through Args as a Python dictionary.
    Parameters passed through Args take precedence 
    over parameters in dbs.config
    
    For MODE=POST (Default Mode): Creates a http proxy, to be able to talk to DBS Server
    """

    DbsConfig.__init__(self,Args)

    if not self.configDict.has_key('version'):
       self.configDict['version'] = self.setApiVersion()
 

    if self.verbose():
       print "configuration dictionary:", self.configDict
       print "using version",self.version()
       print "using mode   ",self.mode()
    #
    # Connect to the Server proxy
    #
    self._server = ""
    if not self.configDict.has_key('mode'):
	self.configDict['mode'] = "POST"  
    if self.mode() == "EXEC" :
	    self._server = DbsExecService(self.dbshome(), self.javahome(), self.version(), Args)
    else :
            self._server = DbsHttpService(self.url(), self.version(), Args)

    # Set up logging

    if not self.configDict.has_key('level'):
	self.configDict['level'] = "ERROR"
    if not self.configDict.has_key('log'):
        self.configDict['log'] = "STDOUT"

    DbsLogger(self.loglevel(), self.log()) 
    logging.log(DBSDEBUG, "DBS Api initialized")
    

  def setApiVersion(self):
    """
    Sets DBS Client Api Version
    Reading for __version__ tag

    Note: Config (dbs.config) and Constructor 
      arguments have higher presedence 
    """
    return __version__

  def getApiVersion(self):
    """
    Returns the API version of the API
    """
    return self.version()

  def _path (self, dataset):
    """
    Determine the dataset path of a dataset.  If the argument is a
    string, it's assumed to be the path and is returned.  If the 
    argument is an object, its assumed to be a processed datatset 
    and this function make a path (string) out of its  primary dataset, 
    tier and processed dataset name.

    Note: takes the FIRST Tier from the list of tiers
          A dataset can have multiple tiers, and however you contruct the path
          using any tier, still leads to same processed dataset, so picking 
          first tier doen't havm the operations. 
    """

    if dataset == None:
	    return ""
    if type(dataset) == type(''):
       return dataset

    if dataset.get('Path') not in ('', None):
	return dataset.get('Path')
 
    # Worst case fabricate a Path !
    if dataset.get('Name') not in ('', None):
         primary = dataset.get('PrimaryDataset')
         if primary != None:
            tier= dataset.get('TierList', [])
            
            if tier in (None, []):
               tier = ['NOTIER']
	    #return "/" + primary.get('Name') \
			#+ "/" + dataset.get('Name') + "/" + tier[0] 	
	    return "/" + primary.get('Name') \
			+ "/" + dataset.get('Name') + "/" + string.join(tier, "-")
               #return "/" + primary.get('Name') \
               #      + "/" + tier[0] + "/" + dataset.get('Name')

    # Anything missing (name, primary or tier) thats an error 
    raise InvalidDatasetPathName(Message="The dataset/path provided is incorrect")      

  def _name (self, obj):
    """
    A utility function, that gets "Name" from an Object.
    Not a very cool function !
    """

    if obj == None:
	    return "";
    if type(obj) == type(''):
       return obj
    name = obj.get('Name')
    if name ==  None:
	    return ""
    return name


  def _file_name (self, obj):
    """
    A utility function, that gets "Name" from an Object.
    Not a very cool function !
    """

    if obj == None:
            return "";
    if type(obj) == type(''):
       return obj
    name = obj.get('LogicalFileName')
    if name ==  None:
            return ""
    return name

  def _get_run (self, obj):
    """
    A utility function, that gets "Name" from an Object.
    Not a very cool function !
    """

    if obj == None:
            return ""; 

    if type(obj) == type(int(1)):
       return obj

    if type(obj) == type(long(1)):
       return obj

    if type(obj) == type (""):
       return obj

    num = obj.get('RunNumber')
    if num ==  None:
            return ""

    return num


  # ------------------------------------------------------------
  #  dbsApi API Implementation follows
  # ------------------------------------------------------------
  def listPrimaryDatasets(self, pattern="*"):
    """
    Retrieve list of primary datasets matching a shell glob pattern.
    Returns a list of DbsPrimaryDataset objects.  If the pattern is
    given, it will be matched against the dataset name as a shell
    glob pattern.

    
    params:
          pattern:  takes a dataset path pattern, defult value is "*"
    returns: 
          list of DbsPrimaryDataset objects  
    examples: 
          api.listPrimaryDatasets() : List ALL primary Datasets in DBS
          api.listPrimaryDatasets("*") : List ALL primary Datasets in DBS
          api.listPrimaryDatasets(pattern='MyPrimaryDataset001') : List MyPrimaryDataset001
          api.listPrimaryDatasets(pattern='MyPrimaryDataset*') : List All Primary datasets whoes names start with MyPrimaryDataset

    raise: DbsApiException, DbsBadResponse
             
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listPrimaryDatasets', 'pattern' : pattern  }, 'GET')

    logging.log(DBSDEBUG, data)

    if self.verbose():
       print data

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):
	  if name == 'primary_dataset':
	    result.append(DbsPrimaryDataset (
                                             Name=str(attrs['primary_name']),
					     Type=str(attrs['type']),
                                             CreationDate=str(attrs['creation_date']),
                                             CreatedBy=str(attrs['created_by']),
                                             LastModificationDate=str(attrs['last_modification_date']),
                                             LastModifiedBy=str(attrs['last_modified_by']),
                                            )
                         )

      xml.sax.parseString (data, Handler ())
      return result
    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  # ------------------------------------------------------------
  def listProcessedDatasets(self, patternPrim="*", patternDT="*", patternProc="*",   
                                  patternVer="*", patternFam="*", patternExe="*", patternPS="*"):
    """
    Retrieve list of processed datasets matching shell glob patterns for dataset and application
    User can provide a comprehensive list of parameters for which he/she want to find dataset(s).
    The search criteria can be based of a pattern for Primary dataset, Processed Dataset, Data Tier,
    Application version, Application Family, Application executyable name and ParameterSet name. Note that
    this API returns the list of processed dataset which are diffrent from datasetPath. A processed dataset
    can conatin multiple datasetPath only if files exists in that dataset

    Returns a list of DbsProcessedDataset objects.  If the pattern(s) are

    params:
        patternPrim: glob pattern for Primary Dataset, MyPrimaryDataset001, MyPrimaryDataSets*, *
        patternDT: glob pattern for Data Tier, SIM, RECO, SIM-DIGI, SIM*, *
        patternProc: glob pattern for Processed Dataset, MyProcDataset001, MyProcDataset*, *
        patternVer: glob pattern for Application Version, v00_00_01, *
        patternFam: glob pattern for Application Family, GEN, *
        patternExe: glob pattern for Application Executable Name, CMSSW, writeDigi, *
        patternPS: glob pattern for PSet Name, whatever, *
 
    returns: list of DbsProcessedDataset objects  

    examples:
           Say I want to list all datasets that have a Primary Dataset of type MyPrimaryDatasets*, with SIM data tier,
           and application version v00_00_03, produced by Application CMSSW, I can make my call as,

           api.listProcessedDatasets(patternPrim='MyPrimaryDatasets*', patternDT='SIM', patternVer='v00_00_03', patternFam='CMSSW') 
   
    raise: DbsApiException, DbsBadResponse

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))


    # Lets get all tiers no matter what, otherwise Server puts unnecessary checks on the DataTier
    patternDT='*'

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


    logging.log(DBSDEBUG, data)  

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        
	def startElement(self, name, attrs):
	  if name == 'processed_dataset':
	    self.procName = str(attrs['processed_datatset_name'])	  
	    self.primName = str(attrs['primary_datatset_name'])	  
            self.currDataset = DbsProcessedDataset ( 
                                                Name=self.procName,     
						PhysicsGroup=str(attrs['physics_group_name']),
						PhysicsGroupConverner=str(attrs['physics_group_convener']),
                                                #openForWriting=str(attrs['open_for_writing']), 
                                                PrimaryDataset=DbsPrimaryDataset(Name=self.primName),
                                                CreationDate=str(attrs['creation_date']),
                                                CreatedBy=str(attrs['created_by']),
                                                LastModificationDate=str(attrs['last_modification_date']),
                                                LastModifiedBy=str(attrs['last_modified_by']),
                                                )
          if name == 'data_tier':
            self.currDataset['TierList'].append(str(attrs['name']))
            #self.currDataset['PathList'].append("/" + self.primName + "/" + str(attrs['name']) + "/" + self.procName)

          if name == 'path':
	    self.currDataset['PathList'].append(str(attrs['dataset_path']))

          if name == 'algorithm':
            self.currDataset['AlgoList'].append(DbsAlgorithm( ExecutableName=str(attrs['app_executable_name']),
                                                         ApplicationVersion=str(attrs['app_version']),
                                                         ApplicationFamily=str(attrs['app_family_name']),
							 ParameterSetID=DbsQueryableParameterSet(
								 Hash=str(attrs['ps_hash']))
							 ) )
        def endElement(self, name):
          if name == 'processed_dataset':
             result.append(self.currDataset)

      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
	raise DbsBadResponse(exception=ex)

  # ------------------------------------------------------------
  def listAlgorithms(self, patternVer="*", patternFam="*", patternExe="*", patternPS="*"):
    """
    Retrieve list of applications/algorithms matching a shell glob pattern.
    User can base his/her search on patters for Application Version, 
    Application Family, Application Executable Name or Parameter Set.

    returns:  list of DbsApplication objects.  

    params:
        patternVer: glob pattern for Application Version, v00_00_01, *
        patternFam: glob pattern for Application Family, GEN, *
        patternExe: glob pattern for Application Executable Name, CMSSW, writeDigi, *
        patternPS: glob pattern for PSet Name, whatever, *
 
    raise: DbsApiException.
    examples:
           Say I want to list all listAlgorithms that have application version v00_00_03, 
           produced by Application CMSSW, I can make my call as,

                 api.listAlgorithms(patternVer='v00_00_03', patternFam='CMSSW') 

           List ALL Algorithms know to DBS

                 api.listAlgorithms("*")

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))


    # Invoke Server.
    data = self._server._call ({ 'api' : 'listAlgorithms',
		    'app_version' : patternVer, 
		    'app_family_name' : patternFam, 
		    'app_executable_name' : patternExe, 
		    'ps_hash' : patternPS }, 
		    'GET')
    logging.log(DBSDEBUG, data) 
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):
	  if name == 'algorithm':
            result.append(DbsAlgorithm( ExecutableName=str(attrs['app_executable_name']),
                                                         ApplicationVersion=str(attrs['app_version']),
                                                         ApplicationFamily=str(attrs['app_family_name']),
                                                         ParameterSetID=DbsQueryableParameterSet
                                                          (
                                                           Hash=str(attrs['ps_hash']),
                                                           Name=str(attrs['ps_name']),
                                                           Version=str(attrs['ps_version']),
                                                           Type=str(attrs['ps_type']),
                                                           Annotation=str(attrs['ps_annotation']),
                                                           Content=base64.decodestring(str(attrs['ps_content']))
                                                           ),
                                                         CreationDate=str(attrs['creation_date']),
                                                         CreatedBy=str(attrs['created_by']),
                                                         LastModificationDate=str(attrs['last_modification_date']),
                                                         LastModifiedBy=str(attrs['last_modified_by']),
                                                        ) )
      xml.sax.parseString (data, Handler ())
      return result
    except Exception, ex:
      raise DbsBadResponse(exception=ex)


  # ------------------------------------------------------------
  def listRuns(self, dataset):
    """
    Retrieve list of runs inside a Processed Dataset.

    returns: list of DbsRun objects.
  
    params:
        dataset: No Default user has to provide a value. 

    examples: 

        List ALL Runs for Dataset Path /test_primary_anzar_001/TestProcessedDS002/SIM

	   Note: Mind that the TIER(s) portion of Path will have NO effect on the selection
		 This is still an open question if we need include that to Run selection as well.


           api.listRuns("/test_primary_anzar_001/TestProcessedDS002/SIM")
        
       Using a Dataset Object 
            primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
            proc = DbsProcessedDataset (
                            PrimaryDataset=primary,
                            Name="TestProcessedDS002",
                            PhysicsGroup="BPositive",
                            Status="VALID",
                            TierList=['SIM', 'RECO'],
                            AlgoList=[WhateverAlgoObject],
                            )
             api.listRuns(proc)


    raise: an DbsApiException.

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    path = self._path(dataset)
    # Invoke Server.
    data = self._server._call ({ 'api' : 'listRuns', 'path' : path }, 'GET')
    logging.log(DBSDEBUG, data) 

    # Parse the resulting xml output.
    try:
      result = []

      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'run':
               self.currRun= DbsRun (
                                   RunNumber=getLong(attrs['run_number']),
                                   NumberOfEvents=getLong(attrs['number_of_events']),
                                   NumberOfLumiSections=getLong(attrs['number_of_lumi_sections']),
                                   TotalLuminosity=getLong(attrs['total_luminosity']),
                                   StoreNumber=getLong(attrs['store_number']),
                                   StartOfRun=str(attrs['start_of_run']),
                                   EndOfRun=str(attrs['end_of_run']),
                                   CreationDate=str(attrs['creation_date']),
                                   CreatedBy=str(attrs['created_by']),
                                   LastModificationDate=str(attrs['last_modification_date']),
                                   LastModifiedBy=str(attrs['last_modified_by']),
                                  )
          if name =='processed_dataset':
               self.currRun['Dataset'].append(DbsProcessedDataset (
                                            Name=str(attrs['processed_datatset_name']),
                                            PrimaryDataset=DbsPrimaryDataset(Name=str(attrs['primary_datatset_name']))
                                            ) )

        def endElement(self, name):
            if name == 'run':
               result.append(self.currRun)
      xml.sax.parseString (data, Handler ())
      return result
    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  # ------------------------------------------------------------

  def listTiers(self, dataset):

    """
    Retrieve list of Tiers inside a Processed Dataset.

    returns: list of DbsDataTier objects.

    params:
        dataset: Not Defaulted user need to provide a dataset path.

    examples: 

        List ALL Data Tiers for Dataset Path /test_primary_anzar_001/TestProcessedDS002/SIM 
          Note: Mind that the TIER(s) portion of Path will have NO effect on the selection
                 This is still an open question, this call will return ALL tiers in the Processed Dataset.


           api.listTiers("/test_primary_anzar_001/TestProcessedDS002/SIM")

       Using a Dataset Object 
            primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
            proc = DbsProcessedDataset (
                            PrimaryDataset=primary,
                            Name="TestProcessedDS002",
                            PhysicsGroup="BPositive",
                            Status="VALID",
                            TierList=['SIM', 'RECO'],
                            AlgoList=[WhateverAlgoObject],
                            )
             api.listTiers(proc)
        
    raise: DbsApiException.

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    path = self._path(dataset)
    # Invoke Server.
    data = self._server._call ({ 'api' : 'listTiers', 'path' : path }, 'GET')
    logging.log(DBSDEBUG, data)

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'data_tier':
               result.append(DbsDataTier(
                                          Name=str(attrs['name']),
                                          CreationDate=str(attrs['creation_date']),
                                          CreatedBy=str(attrs['created_by']),
                                          LastModificationDate=str(attrs['last_modification_date']),
                                          LastModifiedBy=str(attrs['last_modified_by']),
                                        )
                            )

      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  #-------------------------------------------------------------------

  def listBlocks(self, dataset=None, block_name="*", storage_element_name="*"):
    """
    Retrieve list of Blocks matching shell glob pattern for Block Name and/or 
    Storage Element Name, for a dataset path.  All the three parameters are optional.
    

    returns: list of DbsFileBlock objects.

    params:
        dataset: An optional field. It represent the dataset path in the format /prim/proc/dt . The user can leave it empty
        block_name: pattern, if provided it will be matched against the content as a shell glob pattern
        storage_element_name: pattern, if provided it will be matched against the content as a shell glob pattern
         
    raise: DbsApiException.

    examples:

      All Blocks from path /test_primary_001/TestProcessedDS001/SIM
         api.listBlocks("/test_primary_001/TestProcessedDS001/SIM") 
      Block from path /test_primary_001/TestProcessedDS001/SIM whoes name is /this/hahah/SIM#12345
           api.listBlocks("/TestPrimary1167862926.47/TestProcessed1167862926/SIM", "/this/hahah/SIM#12345"):
      All Blocks from path /test_primary_001/TestProcessedDS001/SIM whoes name starts with /this/*
           api.listBlocks("/TestPrimary1167862926.47/TestProcessed1167862926/SIM", "/this/*"):
      All Blocks with a storage element name starting with SE3
           api.listBlocks(storage_element_name="SE3*"):
      All Storage elements within a block name starting with /this/haha
           api.listBlocks(block_name="/this/haha*"):
      All Blocks with all storage element with in any dataset
           api.listBlocks():

      Using a Dataset Object 
            primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
            proc = DbsProcessedDataset (
                            PrimaryDataset=primary,
                            Name="TestProcessedDS002",
                            PhysicsGroup="BPositive",
                            Status="VALID",
                            TierList=['SIM', 'RECO'],
                            AlgoList=[WhateverAlgoObject],
                            )
             api.listBlocks(proc)

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    # Invoke Server.
    path = self._path(dataset)
    data = self._server._call ({ 'api' : 'listBlocks', 'path' : path, 'block_name' : block_name, 'storage_element_name' : storage_element_name }, 'GET')
    logging.log(DBSDEBUG, data)


    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'block':
               self.currBlock = DbsFileBlock(
                                       Name=str(attrs['name']), 
                                       Path=str(attrs['path']), 
                                       BlockSize=long(attrs['size']),
                                       NumberOfFiles=long(attrs['number_of_files']),
                                       NumberOfEvents=long(attrs['number_of_events']),
                                       OpenForWriting=str(attrs['open_for_writing']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       )

          if name == 'storage_element':
               self.currBlock['StorageElementList'].append(DbsStorageElement(
								Name=str(attrs['storage_element_name']))
							  )		
	       
        def endElement(self, name):
          if name == 'block':
             result.append(self.currBlock)
  
      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  #-------------------------------------------------------------------

  def listStorageElements(self, storage_element_name="*"):
    """
    Retrieve list of all storage elements that matches a shell glob pattern for storage element name

    returns: list of DbsStorageElement objects.

    params:
        storage_element_name: pattern, if provided it will be matched against the content as a shell glob pattern
         
    raise: DbsApiException.

    examples:

      All Storage elements matching *SE*
           api.listStorageElements("*SE*")
	   
      All Storage elements
           api.listStorageElements()
      

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    # Invoke Server.
    data = self._server._call ({ 'api' : 'listStorageElements', 'storage_element_name' : storage_element_name }, 'GET')
    logging.log(DBSDEBUG, data)


    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'storage_element':
               result.append(DbsStorageElement(Name=str(attrs['storage_element_name']),
		       CreationDate=str(attrs['creation_date']),
		       CreatedBy=str(attrs['created_by']),
		       LastModificationDate=str(attrs['last_modification_date']),
		       LastModifiedBy=str(attrs['last_modified_by']),
		       ))
	       
      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  # ------------------------------------------------------------

  def listFiles(self, path="", primary="", proc="", tier_list=[], analysisDataset="",blockName="", patternLFN="*", details=None):
    """
    Retrieve list of files in a dataset, in a block, or matching pattern of LFNs, 
    or any combinition of dataset, block and or LFN pattern.

    returns: list of DbsFile objects

    params: 

	path : STRICTLY is the DBS Data PATH in its definition. CANNOT be substituted for anything else like Processed Dataset.
	Note that if path is supplied then all other parameters like primary and processed and tier_list are ignored. The files
	are listed just from within this path
       
        primary: If the user does not specify the path then he/she can specify the primary dataset name with processed dataset name
	
        proc: want to list files of THIS primary AND processed dataset combinition
		(pri, proc) is mutually exclusive to path. Give path or give (pri, proc) pair
		
	tier_list: is a list of tiers that the user wants the file to belong to. If not supplied then all the tiers are returned

	analysisDataset: is the name of the analysis dataset the user wants to list the files from. This is an optional parameter
                         It is mutually exclusive to (path and (pri, proc))
         
        blockName: Defaulted to "" means files (That match dataset and/or LFN pattern criteria). 
        If the blockName is given, it will be matched against the block name.
         
        patternLFN: Defaulted to "*" means files (That match dataset and/or LFN pattern criteria). 
        If the patternLFN patterm is given, it will be matched against the content as a shell glob pattern.

	User MUST provide one of (path, (pri, proc), blockName, patternLFN)

        details: if not None, then server will return details like list of Tier, Parents, etc etc.
         
    raise: DbsApiException.

    examples:
          List all files in path /PrimaryDS_01/SIM/procds-01
             api.listFiles("/PrimaryDS_01/SIM/procds-01")
          List all files in path /PrimaryDS_01/SIM/procds-01, with LFNs that start with 'GoodFile'
             api.listFiles(path="/PrimaryDS_01/SIM/procds-01", patternLFN="GoodFile*")
	  List all files with pattern "GoodFile*"
             api.listFiles(patternLFN="GoodFile*")
          List all files in block /this/block#1230-87698
             api.listFiles(blockName="/this/block#1230-87698")
          List all files in analysis dataset abcd
             api.listFiles(analysisDataset="abcd")

          etc etc.
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    #path = self._path(dataset)
    # Invoke Server.

    sendTier = string.join(tier_list, "-")

    if details not in ("", None, False):
       data = self._server._call ({ 'api' : 'listFiles', 'path' : path, 
				    'primary_dataset' : primary, 
				    'processed_dataset' : proc,
				    'data_tier_list' : sendTier,
		                    'analysis_dataset_name' : analysisDataset,
                                    'block_name' : blockName, 
                                    'pattern_lfn' : patternLFN, 'detail' : 'True' }, 'GET')
    else:
       data = self._server._call ({ 'api' : 'listFiles', 
                                    'primary_dataset': primary,
                                    'processed_dataset' : proc,
                                    'data_tier_list' : sendTier,
                                    'path' : path, 'block_name' : blockName, 
		                    'analysis_dataset_name' : analysisDataset,
                                    'pattern_lfn' : patternLFN}, 'GET')
    logging.log(DBSDEBUG, data)

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'file':
             self.currFile = DbsFile (
                                       LogicalFileName=str(attrs['lfn']),
                                       FileSize=long(attrs['size']),
                                       NumberOfEvents=long(attrs['number_of_events']),
                                       Status=str(attrs['status']),
                                       Block=DbsFileBlock(Name=str(attrs['block_name'])),
                                       FileType=str(attrs['type']),
                                       Checksum=str(attrs['checksum']),
                                       QueryableMetadata=str(attrs['queryable_meta_data']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       )

          if name == 'file_data_tier':
            self.currFile['TierList'].append(str(attrs['name']))

          if name == 'file_branch':
            self.currFile['BranchList'].append(str(attrs['name']))

          if name == 'file_lumi_section':
             self.currFile['LumiList'].append(DbsLumiSection(
                                                   LumiSectionNumber=long(attrs['lumi_section_number']),
                                                   StartEventNumber=long(attrs['start_event_number']),
                                                   EndEventNumber=long(attrs['end_event_number']),   
                                                   LumiStartTime=str(attrs['lumi_start_time']),
                                                   LumiEndTime=str(attrs['lumi_end_time']),
                                                   RunNumber=long(attrs['run_number']),
                                                   CreationDate=str(attrs['creation_date']),
                                                   CreatedBy=str(attrs['created_by']),
                                                   LastModificationDate=str(attrs['last_modification_date']),
                                                   LastModifiedBy=str(attrs['last_modified_by']), 
                                              ))
          if name == 'file_algorithm':
            self.currFile['AlgoList'].append(DbsAlgorithm( ExecutableName=str(attrs['app_executable_name']),
                                                         ApplicationVersion=str(attrs['app_version']),
                                                         ApplicationFamily=str(attrs['app_family_name']),
                                                         CreationDate=str(attrs['creation_date']),
                                                         CreatedBy=str(attrs['created_by']),
                                                         LastModificationDate=str(attrs['last_modification_date']),
                                                         LastModifiedBy=str(attrs['last_modified_by']),
							 ParameterSetID=DbsQueryableParameterSet(
								         Hash=str(attrs['ps_hash']),
         								 Name=str(attrs['ps_name']),
							 		)
                                              ) ) 
          if name == 'file_parent':
             self.currFile['ParentList'].append(DbsFile (
                                       LogicalFileName=str(attrs['lfn']),
                                       FileSize=long(attrs['size']),
                                       NumberOfEvents=long(attrs['number_of_events']),
                                       Status=str(attrs['status']),
                                       Block=DbsFileBlock(Name=str(attrs['block_name'])),
                                       FileType=str(attrs['type']),
                                       Checksum=str(attrs['checksum']),
                                       QueryableMetadata=str(attrs['queryable_meta_data']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       ))

          if name == 'file_child':
             self.currFile['ChildList'].append(DbsFile (
                                       LogicalFileName=str(attrs['lfn']),
                                       FileSize=long(attrs['size']),
                                       NumberOfEvents=long(attrs['number_of_events']),
                                       Status=str(attrs['status']),
                                       Block=DbsFileBlock(Name=str(attrs['block_name'])),
                                       FileType=str(attrs['type']),
                                       Checksum=str(attrs['checksum']),
                                       QueryableMetadata=str(attrs['queryable_meta_data']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       ))
          if name == 'file_run':
		self.currFile['RunsList'].append(DbsRun (
                                   RunNumber=getLong(attrs['run_number']),
                                   NumberOfEvents=getLong(attrs['number_of_events']),
                                   NumberOfLumiSections=getLong(attrs['number_of_lumi_sections']),
                                   TotalLuminosity=getLong(attrs['total_luminosity']),
                                   StoreNumber=getLong(attrs['store_number']),
                                   StartOfRun=str(attrs['start_of_run']),
                                   EndOfRun=str(attrs['end_of_run']),
                                   CreationDate=str(attrs['creation_date']),
                                   CreatedBy=str(attrs['created_by']),
                                   LastModificationDate=str(attrs['last_modification_date']),
                                   LastModifiedBy=str(attrs['last_modified_by']),
                                  ))


        def endElement(self, name):
          if name == 'file':
             result.append(self.currFile)
  
      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)


  #-------------------------------------------------------------------
  def listFileParents(self, lfn):
    """
    Retrieves the list of parents of the given file lfn.
    Returns a list of DbsFile objects.  If the lfn is not
    given, then it will raise an exception.

    params:
          lfn:  the logical file name of the file whose parents needs to be listed. There is no default value for lfn.
    returns: 
          list of DbsFile objects  
    examples: 
          api.listFileParents("aaaa2233-uuuuu-9767-8764aaaa") : List ALL parents for given LFN

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listFileParents', 'lfn' : lfn  }, 'GET')

    logging.log(DBSDEBUG, data)

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'file_parent':
             result.append( DbsFile (
                                       LogicalFileName=str(attrs['lfn']),
                                       FileSize=long(attrs['size']),
                                       NumberOfEvents=long(attrs['number_of_events']),
                                       Status=str(attrs['status']),
                                       Block=DbsFileBlock(Name=str(attrs['block_name'])),
                                       FileType=str(attrs['type']),
                                       Checksum=str(attrs['checksum']),
                                       QueryableMetadata=str(attrs['queryable_meta_data']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       ))

 
      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  #-------------------------------------------------------------------

  def listFileAlgorithms(self, lfn):
    """
    Retrieves the list of algorithms of the given file lfn.
    Returns a list of DbsAlgorithmFile objects.  If the lfn is not
    given, then it will raise an exception.

    params:
          lfn:  the logical file name of the file whose algorithms needs to be listed. There is no default value for lfn.
    returns: 
          list of DbsAlgorithm objects  
    examples: 
          api.listFileAlgorithms("aaaa2233-uuuuu-9767-8764aaaa") : List ALL algorithm for given LFN

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listFileAlgorithms', 'lfn' : lfn  }, 'GET')

    logging.log(DBSDEBUG, data)

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
	   if name == 'file_algorithm':
              result.append(DbsAlgorithm( ExecutableName=str(attrs['app_executable_name']),
                                                         ApplicationVersion=str(attrs['app_version']),
                                                         ApplicationFamily=str(attrs['app_family_name']),
                                                         ParameterSetID=DbsQueryableParameterSet
                                                          (
                                                           Hash=str(attrs['ps_hash']),
                                                           Name=str(attrs['ps_name']),
                                                           ),
                                                         CreationDate=str(attrs['creation_date']),
                                                         CreatedBy=str(attrs['created_by']),
                                                         LastModificationDate=str(attrs['last_modification_date']),
                                                         LastModifiedBy=str(attrs['last_modified_by']),
                                                        ) )

      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  #-------------------------------------------------------------------
  def listFileTiers(self, lfn):
    """
    Retrieves the list of tiers of the given file lfn.
    Returns a list of DbsFile objects.  If the lfn is not
    given, then it will raise an exception.

    
    params:
          lfn:  the logical file name of the file whose tiers needs to be listed. There is no default value for lfn.
    returns: 
          list of DbsDataTier objects  
    examples: 
          api.listFileTiers("aaaa2233-uuuuu-9767-8764aaaa") : List ALL tiers for given LFN

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listFileTiers', 'lfn' : lfn  }, 'GET')

    logging.log(DBSDEBUG, data)

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'file_data_tier':
             result.append(DbsDataTier (
                                       Name=str(attrs['name']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       ))

  
      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  #-------------------------------------------------------------------
  def listFileBranches(self, lfn):
    """
    Retrieves the list of Branches of the given file lfn.
    Returns a list of DbsFile objects.  If the lfn is not
    given, then it will raise an exception.

    params:
          lfn:  the logical file name of the file whose branches needs to be listed. There is no default value for lfn.
    returns: 
          list of DbsFileBranch objects  
    examples: 
          api.listFileBranches("aaaa2233-uuuuu-9767-8764aaaa") : List ALL branches for given LFN

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listFileBranches', 'lfn' : lfn  }, 'GET')

    logging.log(DBSDEBUG, data)

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'file_branch':
	      result.append(DbsFileBranch (
                                       Name=str(attrs['name']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       ))


  
      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)


  #-------------------------------------------------------------------
  def listFileLumis(self, lfn):
    """
    Retrieves the list of parents of the given file lfn.
    Returns a list of DbsLumiSection objects.  If the lfn is not
    given, then it will raise an exception.

    
    params:
          lfn:  the logical file name of the file whose lumi sections needs to be listed. There is no default value for lfn.
    returns: 
          list of DbsLumiSection objects  
    examples: 
          api.listFileLumis("aaaa2233-uuuuu-9767-8764aaaa") : List ALL lumi sections for given LFN

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listFileLumis', 'lfn' : lfn  }, 'GET')

    logging.log(DBSDEBUG, data)
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'file_lumi_section':
             result.append(DbsLumiSection (
                                                   LumiSectionNumber=long(attrs['lumi_section_number']),
                                                   StartEventNumber=long(attrs['start_event_number']),
                                                   EndEventNumber=long(attrs['end_event_number']),   
                                                   LumiStartTime=str(attrs['lumi_start_time']),
                                                   LumiEndTime=str(attrs['lumi_end_time']),
                                                   RunNumber=long(attrs['run_number']),
                                                   CreationDate=str(attrs['creation_date']),
                                                   CreatedBy=str(attrs['created_by']),
                                                   LastModificationDate=str(attrs['last_modification_date']),
                                                   LastModifiedBy=str(attrs['last_modified_by']), 
                                              ))


      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)


  #-------------------------------------------------------------------

  def listAnalysisDatasetDefinition(self, pattern="*"):
    """
    Retrieves the list of definitions of the analysis dataset by matching against the given shell pattern for analysis 
    dataset definition name.
    Returns a list of DbsAnalysisDatasetDefinition objects. 

    
    params:
          pattern:  the shell pattren for nanlysis dataset definition name. 
                    If not given then the default value of * is assigned to it and all the definations are listed
    returns: 
          list of DbsAnalysisDatasetDefinition objects  
    examples: 
          api.listAnalysisDatasetDefinition()
	  api.listAnalysisDatasetDefinition("mydef*")

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    # Invoke Server.
    data = self._server._call ({ 'api' : 'listAnalysisDatasetDefinition',
				 'pattern_analysis_dataset_definition_name' : pattern 
				}, 'GET')

    logging.log(DBSDEBUG, data)
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'analysis_dataset_definition':
                curr_def = DbsAnalysisDatasetDefinition (
                        Name=str(attrs['analysis_dataset_definition_name']),
                        RunsList=str(attrs['runs']).split(','),
                        TierList=str(attrs['tiers']).split(','),
                        FileList=str(attrs['lfns']).split(','),
                        LumiList=str(attrs['lumi_sections']).split(','),
                        AlgoList=str(attrs['algorithms']).split(','),
                        ProcessedDatasetPath=str(attrs['path']),
                        RunRangeList=str(attrs['runs_ranges']).split(','),
                        AnalysisDSList=str(attrs['analysis_dataset_names']).split(','),
                        LumiRangeList=str(attrs['lumi_section_ranges']).split(','),
                        UserCut=str(attrs['user_cut']),
                        #Description=str(attrs['name']),
                        CreationDate=str(attrs['creation_date']),
                        CreatedBy=str(attrs['created_by']),
                        LastModificationDate=str(attrs['last_modification_date']),
                        LastModifiedBy=str(attrs['last_modified_by']),
                        )
                result.append(curr_def)

      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  #-------------------------------------------------------------------

  def listAnalysisDataset(self, pattern="*", path=""):
    """
    Retrieves the list of analysis dataset by matching against the given shell pattern for analysis 
    dataset name.
    Returns a list of DbsAnalysisDataset objects. 

    
    params:
          pattern:  the shell pattren for nanlysis dataset name. If not given then the default value of * is assigned to it and all the datasets are listed
	  path: is the processed dataset path in the format /prim/proc/datatier which if given list all the analysis dataset within that processed dataset
    returns: 
          list of DbsAnalysisDataset objects  
    examples: 
          api.listAnalysisDataset("*t005", "/test_primary_anzar_001/TestProcessedDS001/SIM")
          api.listAnalysisDataset()

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    path = self._path(path)

    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listAnalysisDataset', 
                                 'analysis_dataset_name_pattern' : pattern,
                                 'path' : path  
				}, 'GET')

    logging.log(DBSDEBUG, data)
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'analysis_dataset':
		self.curr_analysis = DbsAnalysisDataset (
         		Annotation=str(attrs['annotation']),
         		Name=str(attrs['analysis_dataset_name']),
         		Type=str(attrs['type']),
         		Status=str(attrs['status']),
         		#PhysicsGroup=str(attrs['physics_group']),
         		#Definition=
                        CreationDate=str(attrs['creation_date']),
                        CreatedBy=str(attrs['created_by']),
                        LastModificationDate=str(attrs['last_modification_date']),
                        LastModifiedBy=str(attrs['last_modified_by']),
			)
	  if name == 'analysis_dataset_definition':
                self.curr_def = DbsAnalysisDatasetDefinition (
            		Name=str(attrs['analysis_dataset_definition_name']),
            		RunsList=str(attrs['runs']).split(','),
            		TierList=str(attrs['tiers']).split(','),
            		FileList=str(attrs['lfns']).split(','),
            		LumiList=str(attrs['lumi_sections']).split(','),
            		AlgoList=str(attrs['algorithms']).split(','),
            		ProcessedDatasetPath=str(attrs['path']),
            		RunRangeList=str(attrs['runs_ranges']).split(','),
            		AnalysisDSList=str(attrs['analysis_dataset_names']).split(','),
            		LumiRangeList=str(attrs['lumi_section_ranges']).split(','),
            		UserCut=str(attrs['user_cut']),
            		#Description=str(attrs['name']),
                        CreationDate=str(attrs['creation_date']),
                        CreatedBy=str(attrs['created_by']),
                        LastModificationDate=str(attrs['last_modification_date']),
                        LastModifiedBy=str(attrs['last_modified_by']),
			)
                self.curr_analysis['Definition'] = self.curr_def	
                result.append(self.curr_analysis)

      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)


  #-------------------------------------------------------------------

  def listDatasetParents(self, dataset):
    """
    Retrieves the list of processed dataset which are parents iof the given dataset.
    Returns a list of DbsAnalysisDataset objects. 

    
    params:
	  dataset: is the processed dataset represented either in string format as path (/prim/datatier/proc) or in DbsProcessedDataset format.
	  This is a mandatory field and is not defaulted
	  
    returns: 
          list of DbsProcessedDataset objects  
    examples: 
          api.listDatasetParents("/test_primary_anzar_001/TestProcessedDS001/SIM")

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    path = self._path(dataset)

    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listDatasetParents', 
                                 'path' : path  
				}, 'GET')

    logging.log(DBSDEBUG, data)
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        def startElement(self, name, attrs):
          if name == 'processed_dataset_parent':
		  myPath = str(attrs['path']).split('/')
		  result.append(DbsProcessedDataset ( 
			  			Name=myPath[2],
                                                #openForWriting=str(attrs['open_for_writing']), 
                                                PrimaryDataset=DbsPrimaryDataset(Name=myPath[1]),
                                                PhysicsGroup=str(attrs['physics_group_name']),
                                                PhysicsGroupConverner=str(attrs['physics_group_convener']),
                                                CreationDate=str(attrs['creation_date']),
                                                CreatedBy=str(attrs['created_by']),
                                                LastModificationDate=str(attrs['last_modification_date']),
                                                LastModifiedBy=str(attrs['last_modified_by']),
                                                #PathList=[str(attrs['path'])],     
                                                #Path=[str(attrs['path'])],     
                                                ))

      xml.sax.parseString (data, Handler ())
      return result

    except Exception, ex:
      raise DbsBadResponse(exception=ex)


  #-------------------------------------------------------------------

  def listDatasetContents(self, path, block_name):
    """
    Dumps contents of a block in dataset in XML format.
    This API call is used for insertDatasetContents, which actually use this XML dump and
    repopulates (another) instance of DBS with same dataset 

    params: 
        path : Not Defaulted. Its the dataset path for which API is being invoked (can be provided as dataset object).
        block_name : Name of the Block thats being dumped.

    examples:
        Dump the contents of Block /this/block#1230-87698 for Dataset /PrimaryDS_01/procds-01/SIM
                  api.listDatasetContents("/PrimaryDS_01/procds-01/SIM", "/this/block#1230-87698") 
        
        Using a Dataset Object 
            primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
            proc = DbsProcessedDataset (
                            PrimaryDataset=primary,
                            Name="TestProcessedDS002",
                            PhysicsGroup="BPositive",
                            Status="VALID",
                            TierList=['SIM', 'RECO'],
                            AlgoList=[WhateverAlgoObject],
                            )
             api.listDatasetContents(proc, "/this/block#1230-87698")
 
    raisei: DbsApiException.

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    try:
       # Invoke Server.
       path = self._path(path)
       data = self._server._call ({ 'api' : 'listDatasetContents', 'path' : path, 'block_name' : block_name }, 'GET')
       #logging.log(DBSDEBUG, data)

       return data

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

  #-------------------------------------------------------------------

  def insertDatasetContents(self, xmlinput):
    """

    This API call is used for inserting Dataset from XML dump generated by listDatasetContents
    The APIrepopulates (another) instance of DBS with same dataset as 
    produced by the listDatasetContents counterpart 

    params: 
        xmlinput : XML dump generated by listDatasetContents

    examples:

        Dump the contents of Block /this/block#1230-87698 for Dataset /PrimaryDS_01/procds-01/SIM
                  api.listDatasetContents("/PrimaryDS_01/procds-01/SIM", "/this/block#1230-87698") 
        
        Using a Dataset Object 
            primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
            proc = DbsProcessedDataset (
                            PrimaryDataset=primary,
                            Name="TestProcessedDS002",
                            PhysicsGroup="BPositive",
                            Status="VALID",
                            TierList=['SIM', 'RECO'],
                            AlgoList=[WhateverAlgoObject],
                            )
             xmldataset = api.listDatasetContents(proc, "/this/block#1230-87698")
             api.insertDatasetContents(xmldataset)

    May raise an DbsApiException.

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    try:
       # Invoke Server.
       data = self._server._call ({ 'api' : 'insertDatasetContents', 'xmlinput' : xmlinput }, 'POST')
       #logging.log(DBSDEBUG, data)

       return data

    except Exception, ex:
      raise DbsBadResponse(exception=ex)


 # ------------------------------------------------------------

  def insertPrimaryDataset(self, dataset):
    """
    Inserts a new primary dataset in the DBS databse. 
    
    param: 
	dataset : The primary dataset passed in as DbsPrimaryDataset object.  The following are mandatory and should be present
	          in the dbs primary dataset object:  primary_name	  
		  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
         api.insertPrimaryDataset (primary)

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<primary_dataset annotation='"+dataset.get('Annotation', '')+"' "
    xmlinput += " primary_name='"+dataset.get('Name', '')+"' "
    xmlinput += " start_date='"+dataset.get('StartDate', '')+"' end_date='"+dataset.get('EndDate', '')+"'"
    xmlinput += " description='"+dataset.get('Description', '')+"'"
    #xmlinput += " trigger_path_description='"+dataset.get('TriggerPathDesc', '')+"'"
    #xmlinput += " mc_channel_description='"+dataset.get('McChannelDesc', '')+"' mc_production='"+dataset.get('McProdDesc', '')+"'"
    #xmlinput += " mc_decay_chain='"+dataset.get('McDecayChain', '')+"' other_description='"+dataset.get('OtherDesc', '')
    xmlinput += " type='"+dataset.get('Type', '')+"'>"
    xmlinput += " </primary_dataset>"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)

    if self.verbose():
       print "insertPrimaryDataset, xmlinput",xmlinput
    data = self._server._call ({ 'api' : 'insertPrimaryDataset',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)

  # ------------------------------------------------------------
  def insertAlgorithm(self, algorithm):

    """
    Inserts a new dbs algorithm/application . An algorithm is uniquely identified by appverion, appfamily , 
    appexecutable and parametersetname collectively. If the algorithm already exist then it just displays a warnning.
    
    param: 
        algorithm : The dbs algorithm passed in as an DbsAlgorithm object. The following are mandatory and should be present
	          in the dbs algorithm object:
		  app_version, app_family_name, app_executable_name and ps_name
		  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         algo = DbsAlgorithm (
                ExecutableName="TestExe01",
                ApplicationVersion= "TestVersion01",
                ApplicationFamily="AppFamily01",
                ParameterSetID=DbsQueryableParameterSet(
                     Hash="001234565798685",
                     Name="MyFirstParam01",
                     Version="V001",
                     Type="test",
                     Annotation="This is test",
                     Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
                )
         )

         api.insertAlgorithm (algo)
	
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))


    # Prepare XML description of the input

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<algorithm app_version='"+algorithm.get('ApplicationVersion', "")+"'"
    xmlinput += " app_family_name='"+algorithm.get('ApplicationFamily', "")+"'"
    xmlinput += " app_executable_name='"+algorithm.get('ExecutableName', "")+"'"
    pset = algorithm.get('ParameterSetID')

    if pset != None: 
       xmlinput += " ps_hash='"+pset.get('Hash', "")+"'"
       xmlinput += " ps_name='"+pset.get('Name', "")+"'"
       xmlinput += " ps_version='"+pset.get('Version', "")+"'"
       xmlinput += " ps_type='"+pset.get('Type', "")+"'"
       xmlinput += " ps_annotation='"+pset.get('Annotation', "")+"'"
       # Converting Content to base64 encoded string, otherwise it can leave the xml invalid
       xmlinput += " ps_content='"+base64.binascii.b2a_base64(pset.get('Content', ""))+"'"
    xmlinput += "/>"
    xmlinput += "</dbs>"
    logging.log(DBSDEBUG, xmlinput)

    
    if self.verbose():
       print "insertAlgorithm, xmlinput",xmlinput
    data = self._server._call ({ 'api' : 'insertAlgorithm',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)

  # ------------------------------------------------------------

  def insertProcessedDataset(self, dataset):
    """
    Inserts a new dbs processed dataset in an existing primary dataset . It insert all the parents of the processed dataset, 
    insert and assocaite  all the tiers of the processed dataset, associate all the algorithms of the processed dataset and 
    associate all the runs of the processed dataset. Note that inserting a processed dataset doesnot imply that there are now
    datasetPaths in this processed dataset. Only when a block or a file is inserted a datasetPath becomes available in the processed
    dataset.
    The parents, algorithms and runs of the processed dataset should exist before the  processed dataset could be inserted.
    
    param: 
        dataset : The procsssed dataset can be passed as an DbsProcessedDataset object. The following are mandatory and should be present
	          in the dbs procsssed dataset object:
		  processed_datatset_name and primary_datatset_name
		  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         algo = DbsAlgorithm (
                ExecutableName="TestExe01",
                ApplicationVersion= "TestVersion01",
                ApplicationFamily="AppFamily01",
                ParameterSetID=DbsQueryableParameterSet(
                     Hash="001234565798685",
                     Name="MyFirstParam01",
                     Version="V001",
                     Type="test",
                     Annotation="This is test",
                     Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
                )
         )

         primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
	 
         proc = DbsProcessedDataset (
                PrimaryDataset=primary, 
                Name="TestProcessedDS002", 
                PhysicsGroup="BPositive",
                Status="Valid",
                TierList=['SIM', 'RECO'],
                AlgoList=[algo],
         )

         api.insertProcessedDataset (proc)
	 
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    xmlinput  = "<?xml version='1.0' standalone='yes'?>" 
    xmlinput += "<dbs>" 
    xmlinput += "<processed_dataset "
    primary = dataset.get('PrimaryDataset')
    if primary == None: 
       raise DbsApiException(ErrorMsg="Serious Error Primary Dataset not specified")
    xmlinput += " primary_datatset_name='"+primary.get('Name', "")+"'" 
    xmlinput += " processed_datatset_name='"+dataset.get('Name', "")+"'"
    xmlinput += " open_for_writing='"+dataset.get('OpenForWriting', "")+"'"
    xmlinput += " physics_group_name='"+dataset.get('PhysicsGroup', "")+"'"
    xmlinput += " physics_group_convener='"+dataset.get('PhysicsGroupConverner', "")+"'"
    xmlinput += " status='"+dataset.get('Status', "")+"'>" 
    
    for tier in dataset.get('TierList',[]):
        xmlinput += "<data_tier name='"+tier+"'/>"

    # Path of the Parent Dataset(s) must be specified, sever expects a "Path"
    for parentPath in dataset.get('ParentList',[]):
        xmlinput += "<parent path='"+self._path(parentPath)+"'/>"

    for algorithm in dataset.get('AlgoList',[]):
        xmlinput += "<algorithm app_version='"+algorithm.get('ApplicationVersion', "")+"'"
        xmlinput += " app_family_name='"+algorithm.get('ApplicationFamily', "")+"'"
        xmlinput += " app_executable_name='"+algorithm.get('ExecutableName', "")+"'"
        pset = algorithm.get('ParameterSetID')
        # Server expects a ps_name, it should expect a ps_hash instead 
        if pset != None:
           xmlinput += " ps_hash='"+pset.get('Hash', "")+"'"
           #xmlinput += " ps_name='"+pset.get('Name', "")+"'"
           #xmlinput += " ps_version='"+pset.get('Version', "")+"'"
           #xmlinput += " ps_type='"+pset.get('Type', "")+"'"
           #xmlinput += " ps_annotation='"+pset.get('Annotation', "")+"'"
           #xmlinput += " ps_content='"+base64.binascii.b2a_base64(pset.get('Content', ""))+"'"
        xmlinput += "/>"

    for run in dataset.get('RunsList',[]):
        runNum = str(self._get_run(run))
        if runNum not in ("", None):
           xmlinput += "<run run_number='"+runNum+"'/>"

    xmlinput += "</processed_dataset>"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertProcessedDataset, xmlinput",xmlinput
    
    # Call the method
    data = self._server._call ({ 'api' : 'insertProcessedDataset',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)

# ------------------------------------------------------------

  def insertRun(self, run):

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    """
    Inserts a new run in the DBS databse. 
    
    param: 
	run : The dbs run passed in as DbsRun object. The following are mandatory and should be present
	      in the dbs run object: 
	      RunNumber, number_of_events, number_of_lumi_sections, total_luminosity and store_number
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
    
         run = DbsRun (
                 RunNumber=1,
                 NumberOfEvents= 100,
                 NumberOfLumiSections= 20,
                 TotalLuminosity= 2222,
                 StoreNumber= 123,
                 StartOfRun= 'now',
                 EndOfRun= 'never',
         )
 
         api.insertRun (run)

    """

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<run"
    xmlinput += " run_number='"+str(run.get('RunNumber', ''))+"'"
    xmlinput += " number_of_events='"+str(run.get('NumberOfEvents', ''))+"'"
    xmlinput += " number_of_lumi_sections='"+str(run.get('NumberOfLumiSections', ''))+"'"
    xmlinput += " total_luminosity='"+str(run.get('TotalLuminosity', ''))+"'"
    xmlinput += " store_number='"+str(run.get('StoreNumber', ''))+"'"
    xmlinput += " start_of_run='"+run.get('StartOfRun', '')+"'"
    xmlinput += " end_of_run='"+run.get('EndOfRun', '')+"'"
    xmlinput += " />"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertRun, xmlinput",xmlinput
       
    data = self._server._call ({ 'api' : 'insertRun',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)


# ------------------------------------------------------------
  def updateFileStatus(self, lfn, status):
    """
    Updates the Status filed of a File (lfn)

    lfn: Logical File Name of file that needs to be updated
    status: One of the possible status 
  
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    data = self._server._call ({ 'api' : 'updateFileStatus',
                         'lfn' : self._file_name(lfn),
                         'status' : status,
                         }, 'POST')

# ------------------------------------------------------------
  def updateProcDSStatus(self, dataset, status):
    """
    Updates the Status filed of a Dataset

    dataset: Dataset to be updated
    status: One of the possible status 
  
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    data = self._server._call ({ 'api' : 'updateProcDSStatus',
                         'path' : self._path(dataset),
                         'status' : status,
                         }, 'POST')

# -----------------------------------------------------------
  def updateRun(self, run):


    """
    Updates a run in the DBS databse.

    param:
        run : The dbs run passed in as DbsRun object. RunNumber is mandatory
	      Following values could be updated, one can provide one or all
		NumberOfEvents, NumberOfLumiSections, TotalLuminosity, EndOfRun

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError,
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException

    examples:

         run = DbsRun (
                 RunNumber=1,
                 NumberOfEvents= 100,
                 NumberOfLumiSections= 20,
                 TotalLuminosity= 2222,
                 EndOfRun= 'never',
         )

         api.insertRun (run)

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<run"
    xmlinput += " run_number='"+str(run.get('RunNumber', ''))+"'"
    xmlinput += " number_of_events='"+str(run.get('NumberOfEvents', ''))+"'"
    xmlinput += " number_of_lumi_sections='"+str(run.get('NumberOfLumiSections', ''))+"'"
    xmlinput += " total_luminosity='"+str(run.get('TotalLuminosity', ''))+"'"
    xmlinput += " end_of_run='"+run.get('EndOfRun', '')+"'"
    xmlinput += " />"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertRun, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'updateRun',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)

  # ------------------------------------------------------------

  def insertFiles(self, dataset=None, files=[], block=None):
    """ 
    Inserts a new dbs file in an existing block in a given processed dataset. It also insertes lumi sections
    assocated with the file. It insert all the parents of the file, assocaite  all the tiers of the file and 
    associate all the algorithms of the file. The parents, tiers and algorithms of the file should exist before 
    the file could be inserted.
    
    param: 
        dataset : The procsssed dataset can be passed as an DbsProcessedDataset object or just as a dataset 
	          path in the format of /prim/proc/datatier
                  
        block : The dbs file block passed in as an DbsFileBlock obejct. This object can be passed in also, 
                as a string containing the block name, instead of DbsFileBlock object. The following fields 
                are mandatory and should be present in the dbs file block object: block_name
                          
	files : The list of dbs files in the format of DbsFile obejct. The following are mandatory and should be present 
		in the dbs file object:	lfn
		  
	Note:
		IF block (boock name or DbsFileBlock object) is provide it gets precedense over dataset (or path)
                So files will be inserted into provided block and DBS will only raise a warning that dataset/path
                is being ignored. In that case just set dataset="" anbd DBS will ignore it.
		
	  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         algo = DbsAlgorithm (
                ExecutableName="TestExe01",
                ApplicationVersion= "TestVersion01",
                ApplicationFamily="AppFamily01",
                ParameterSetID=DbsQueryableParameterSet(
                     Hash="001234565798685",
                     Name="MyFirstParam01",
                     Version="V001",
                     Type="test",
                     Annotation="This is test",
                     Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
                )
         )

         primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
	 
         proc = DbsProcessedDataset (
                PrimaryDataset=primary, 
                Name="TestProcessedDS002", 
                PhysicsGroup="BPositive",
                Status="Valid",
                TierList=['SIM', 'RECO'],
                AlgoList=[algo],
         )

         lumi1 = DbsLumiSection (
                 LumiSectionNumber=1222,
                 StartEventNumber=100,
                 EndEventNumber=200,
                 LumiStartTime='notime',
                 LumiEndTime='neverending',
                 RunNumber=1,
         )

         lumi2 = DbsLumiSection (
                 LumiSectionNumber=1333,
                 StartEventNumber=100,
                 EndEventNumber=200,
                 LumiStartTime='notime',
                 LumiEndTime='neverending',
                 RunNumber=1,
         )

         myfile1= DbsFile (
                Checksum= '999',
                LogicalFileName= 'aaa1122-0909-9767-8764aaa',
                NumberOfEvents= 10000,
                FileSize= 12340,
                Status= 'VALID',
        	ValidationStatus = 'VALID',
                FileType= 'EVD',
                Dataset= proc,
                LumiList= [lumi1, lumi2],
                TierList= ['SIM', 'RECO'],
         )


        myfile2= DbsFile (
                 Checksum= '000',
                 LogicalFileName= 'aaaa2233-0909-9767-8764aaa',
                 NumberOfEvents= 10000,
                 FileSize= 12340,
                 Status= 'VALID',
         	 ValidationStatus = 'VALID',
                 FileType= 'EVD',
                 Dataset= proc,
                 TierList= ['SIM', 'RECO'],
                 AlgoList = [algo],
                 ParentList = ['aaa1122-0909-9767-8764aaa']  
         )
                            
         block = DbsFileBlock (
                 Name="/this/hahah/SIM#12345"
         )

         api.insertFiles (proc, [myfile1, myfile2], block)

         api.insertFiles ("/test_primary_anzar_001/TestProcessedDS002/SIM",[myfile1, myfile2], "/this/hahah/SIM#12345")
	 
         api.insertFiles (proc, [myfile1, myfile2], "/this/hahah/SIM#12345")
   
         api.insertFiles ("/test_primary_anzar_001/TestProcessedDS002/SIM", [myfile1, myfile2],  block)

    """
    # Prepare XML description of the input

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"

    # Anzar Afaq
    # This comment-out is part of the HACK to allow inconsistent Block names
    #
    #if (isinstance(dataset, DbsProcessedDataset)):
    #    xmlinput += " <processed_datatset path=''>"
    #else :
    #   xmlinput += " <processed_datatset path='"+self._path(dataset)+"'>"
    xmlinput += " <processed_datatset path='"+self._path(dataset)+"'>"

    if block not in (None, ""):
       xmlinput += "<block block_name='"+ block.get("Name", "") +"'>"
       #xmlinput += " open_for_writing='"+block.get('OpenForWriting', "")+"'"
       #xmlinput += " path='"+path+"'>"
    if type(block) != type("str") and block not in (None, "") :
       if (block['StorageElementList'] not in ( [], None)) :
         for aSe in block['StorageElementList']:
            xmlinput += " <storage_element storage_element_name='"+self._name(aSe)+"'/>"
    if block not in (None, ""):	
    	xmlinput += "</block>"

    for file in files:
       xmlinput += " <file lfn='"+file.get('LogicalFileName', '')+"'"
       xmlinput += " checksum='"+file.get('Checksum', '')+"'"
       xmlinput += " number_of_events='"+str(file.get('NumberOfEvents', ''))+"'"
       xmlinput += " size='"+str(file.get('FileSize', ''))+"'"
       xmlinput += " file_status='"+file.get('Status', '')+"'" 
       xmlinput += " type= '"+file.get('FileType', '')+"'"
       xmlinput += " validation_status='"+file.get('ValidationStatus', '')+"'"
       xmlinput += " queryable_meta_data='"+file.get('QueryableMetadata', '')+"'"
       xmlinput += " >" 

       for lumi in file.get('LumiList', []):
            xmlinput += "<file_lumi_section lumi_section_number='"+str(lumi.get('LumiSectionNumber', ''))+"'"
            xmlinput += " run_number='"+str(lumi.get('RunNumber', ''))+"'"
            xmlinput += " start_event_number='"+str(lumi.get('StartEventNumber', ''))+"'" 
            xmlinput += " end_event_number='"+str(lumi.get('EndEventNumber', ''))+"'"
            xmlinput += " lumi_start_time='"+lumi.get('LumiStartTime', '')+"'" 
            xmlinput += " lumi_end_time='"+lumi.get('LumiEndTime', '')+"'"
            xmlinput += " />"

       for run in file.get('RunsList', []):
            runNum = str(self._get_run(run))
            if runNum not in ("", None):
               xmlinput += "<file_lumi_section "
               xmlinput += " run_number='"+runNum+"'"
               xmlinput += " />"

       for tier in file.get('TierList',[]):
            xmlinput += "<file_data_tier name='"+self._name(tier)+"'/>"

       for branch in file.get('BranchList',[]):
            xmlinput += "<file_branch name='"+branch+"'/>"
   
       # LFNs of the Parent Files(s) may be specified, sever expects LFNs
       for parent in file.get('ParentList',[]):
            xmlinput += "<file_parent lfn='"+self._file_name(parent)+"' />"

       # LFNs of the Children Files(s) may be specified, sever expects LFNs
       for child in file.get('ChildList',[]):
            xmlinput += "<file_child lfn='"+self._file_name(child)+"' />"

       for algorithm in file.get('AlgoList',[]):
           xmlinput += "<file_algorithm app_version='"+algorithm.get('ApplicationVersion', "")+"'"
           xmlinput += " app_family_name='"+algorithm.get('ApplicationFamily', "")+"'"
           xmlinput += " app_executable_name='"+algorithm.get('ExecutableName', "")+"'"
           pset = algorithm.get('ParameterSetID')
           # Server expects a ps_name, it should expect a ps_hash instead 
           if pset != None:
              xmlinput += " ps_hash='"+pset.get('Hash', "")+"'"
              #xmlinput += " ps_name='"+pset.get('Name', "")+"'"
              #xmlinput += " ps_version='"+pset.get('Version', "")+"'"
              #xmlinput += " ps_type='"+pset.get('Type', "")+"'"
              #xmlinput += " ps_annotation='"+pset.get('Annotation', "")+"'"
              #xmlinput += " ps_content='"+base64.binascii.b2a_base64(pset.get('Content', ""))+"'"
           xmlinput += "/>"
       xmlinput += "</file>"
       xmlinput += "\n"
      
    xmlinput += "</processed_datatset>"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)

    if self.verbose():
       print "insertFiles, xmlinput",xmlinput

    if (isinstance(dataset, DbsProcessedDataset)) :

	# Call the method
	data = self._server._call ({ 'api' : 'insertFiles',
			 'primary_dataset' : dataset['PrimaryDataset']['Name'],
			 'processed_dataset' : dataset['Name'],
                         'xmlinput' : xmlinput }, 'POST')
    else :
        # Call the method
        data = self._server._call ({ 'api' : 'insertFiles',
                         'xmlinput' : xmlinput }, 'POST')
        logging.log(DBSDEBUG, data)

  # ------------------------------------------------------------
  def remapFiles_DEPRECATED(self, inFiles, outFile):
    """ 
    Remaps the file parentage . This api is called after a merge job is completed.
    A list of input file lfns and one output file is given to this api. The parents of all the input files
    becomes the parent of the output file. The childern of all the input files becomes the children of the 
    output file.
    
    param: 
        inFiles : A list of LFNs that are to be remapped. 
	
        outFiles : The LFN of the final output merged file
	
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
    
         fileList = ['aaa1122-0909-9767-8764aaa', 'aaa1122-0909-9767-8764bb']
         api.remapFiles (fileList, 'MyoutFile')
    """
    # Prepare XML description of the input

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    
    for afile in inFiles:
       xmlinput += " <in_file lfn='" + afile +"'/>"
       
    xmlinput += " <out_file lfn='" + outFile +"'/>"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "remapFiles, xmlinput",xmlinput

    # Call the method
    data = self._server._call ({ 'api' : 'remapFiles',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)

  # ------------------------------------------------------------

  def insertBlock(self, dataset, block=None, storage_element_list=None, open_for_writing='y'):
    """
    Inserts a new dbs file block in a given processed dataset. 
    
    param: 
        dataset : The procsssed dataset can be passed as an DbsProcessedDataset object or just as a dataset 
	          path in the format of /prim/proc/datatier  
	          [PATH is mandatory, To Support DBS-1 block which do not have Tiers in BlockName]

	block : The dbs file block passed in as a string containing the block name. This field is not mandatory.
	        If the block name is not provided the server creates one based on the primary dataset name, processed
		dataset name and tiers (from Path) and a random GUID. It returns back this newly created block
			  
	storage_element : The list of storage element names in the string format. This field is not mandatory. If 
	                  this field is not provided then just the block is inserted without any storage element 
			  associated with it.

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
	 proc = DbsProcessedDataset (
               PrimaryDataset=primary,
               Name="TestProcessedDS002",
               TierList=['SIM', 'RECO'],
         )
         api.insertBlock (proc)

         api.insertBlock (proc, "/this/hahah/SIM#12345")
    
         api.insertBlock (proc, "/this/hahah/SIM#12345",  [se1Obj, se2Obj, se3Obj])
	 
         api.insertBlock (proc, "",  ['se1', 'se2', 'se3'])
    
         api.insertBlock ("/test_primary_anzar_001/TestProcessedDS002/SIM" , "/this/hahah/SIM#12345")
    
         api.insertBlock ("/test_primary_anzar_001/TestProcessedDS002/SIM , "/this/hahah/SIM#12345",  [se1Obj, se2Obj, se3Obj])
	 
         api.insertBlock ("/test_primary_anzar_001/TestProcessedDS002/SIM , "",  [se1Obj, se2Obj, se3Obj])
    

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    path = self._path(dataset)
    name = self._name(block)

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<block name='"+ name +"'"
    if type(block) != type("str") and block != None :
       xmlinput += " open_for_writing='"+block.get('OpenForWriting', "")+"'"
    """
    if (isinstance(dataset, DbsProcessedDataset)) :
	xmlinput += " primary_dataset='"+dataset['PrimaryDataset']['Name']+"'"
	xmlinput += " processed_dataset='"+dataset['Name']+"'"
	xmlinput += " path='' >"
    else :
    	xmlinput += " path='"+path+"'>"
    """
    xmlinput += " path='"+path+"'>"

    if (storage_element_list) not in ( [], None ) : 
         for aSe in storage_element_list:
            xmlinput += " <storage_element storage_element_name='"+self._name(aSe)+"'/>"
    xmlinput += "</block>"  
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertBlock, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertBlock',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)


    
    # Parse the resulting xml output.
    try:
     result = []
     class Handler (xml.sax.handler.ContentHandler):
      def startElement(self, name, attrs):
        if name == 'block':
             result.append(str(attrs['block_name'])) 
     xml.sax.parseString (data, Handler ())
     if len(result) > 0:
        return result[0] 
     else: 
        return None   
    except Exception, ex:
      raise DbsBadResponse(exception=ex)


  # ------------------------------------------------------------
  def deleteReplicaFromBlock(self, block, storage_element):

    """
    Deletes the Storage Element assocaition with the Block in the DBS.
    
    param: 
        block : The dbs file block passed in as a string containing the block name or a dbsFileBlock object.
        storage_element : The name of storage element in the string format. Please note that if the user does not provide any
        of these two parameters then all the file blcoks with thier storage elements relationships will get deleted.
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         api.deleteReplicaFromBlock ("/this/hahah/SIM#12345", "se1")


        Note that se1 is a STRING not a StorageElement Object
 
    """   

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    bname = self._name(block)
    sename = self._name(storage_element)
    
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<storage_element storage_element_name='"+ sename +"' block_name='"+ bname +"'/>"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "deleteReplicaFromBlock, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'deleteSEFromBlock',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)

   # ------------------------------------------------------------


  def renameSE(self, storage_element_from, storage_element_to):
    """
    Renames the Storage Element  in the DBS.
    
    param: 
	storage_element_from : The name of storage element in the string format or object format that needs to be changed
	storage_element_to : The name of storage element in the string format or object format that it will be changed to.
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         api.renameSE ("se1", "se2")

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    seNameFrom = self._name(storage_element_from)
    seNameTo = self._name(storage_element_to)
    
    data = self._server._call ({ 'api' : 'updateSEName',
		    'storage_element_name_from' : seNameFrom,
		    'storage_element_name_to' : seNameTo }, 'POST')
    logging.log(DBSDEBUG, data)

   # ------------------------------------------------------------

  def closeBlock(self, block=None ):
    """
    Updates the dbs file block states to closed. 
    
    param: 
	block : The dbs file block passed in as a string containing the block name or as a dbsFileBlock object. 
	This field is mandatory.
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         api.closeBlock ("/this/hahah/SIM#12345")

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    # Invoke Server.
    block_name = self._name(block)
    data = self._server._call ({ 'api' : 'closeBlock', 'block_name' : block_name }, 'POST')
    logging.log(DBSDEBUG, data)

    
   # ------------------------------------------------------------

  def addReplicaToBlock(self, block, storageElement):
    """
    Inserts a new storage element in a given block. 
    
    param: 
        block : The dbs file block passed in as an DbsFileBlock obejct. This object can be  passed in also, 
                as a string containing the block name, instead of DbsFileBlock object. The following fields 
                are mandatory and should be present in the dbs file block object: 
                block_name and storage_element_name
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         block = DbsFileBlock (
                Name="/TestPrimary1164751189.48/HIT1164751189.48/TestProcessed1164751189.48"
         )
         api.addReplicaToBlock ( block , 'se1')
         
         api.addReplicaToBlock ( "/this/hahah/SIM#12345" , 'se2')

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    bname = self._name(block)
    sename = self._name(storageElement)
    
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    if (storageElement not in ( '', None)) : 
          xmlinput += " <storage_element block_name='" + bname + "' storage_element_name='"+ sename +"'/>"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)

    data = self._server._call ({ 'api' : 'insertStorageElement',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)


  # ------------------------------------------------------------

  def insertTier(self, tier_name):
    """
    Inserts a new tier in the DBS databse. 
    
    param: 
	tier_name : The data tier name passed in as string 
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         tier_name = "GEN-SIM-TEST"
         api.insertTier (tier_name)

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
 
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<tier tier_name='"+ tier_name +"' />"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertTier, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertTier', 
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)


  # ------------------------------------------------------------

  def insertTierInPD(self, dataset, tier_name):
    """
    Inserts a new tier in the DBS databse. 
    
    param: 
	tier_name : The data tier name passed in as string 
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         tier_name = "GEN-SIM-TEST"
         api.insertTierInPD ("/prim/dt/proc", tier_name)

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    path = self._path(dataset)
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<processed_dataset path='" + path + "'/>"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertTier, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertTierInPD', 
		         'tier_name' : tier_name,
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)


  # ------------------------------------------------------------

  def insertParentInPD(self, dataset, parentDS):
    """
    Inserts a new parent in the DBS databse. 
    
    param: 
	parentDS : The dataset oath of the parent passed in as string 
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         tier_name = "GEN-SIM-TEST"
         api.insertParentInPD ("/prim/proc/dt", "/adc/def/rfg")

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    path = self._path(dataset)
    parentPath = self._path(parentDS)
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<processed_dataset path='" + path + "'/>"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertParent, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertParentInPD', 
		         'parent_path' : parentPath,
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)


  # ------------------------------------------------------------

  def insertAlgoInPD(self, dataset, algorithm):
    """
    Inserts a new algorithm in the DBS databse. 
    
    param: 
	aqlgo : The algorithm 
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         tier_name = "GEN-SIM-TEST"
         api.insertParentInPD ("/prim/proc/dt", "/adc/def/rfg")

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    path = self._path(dataset)
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<processed_dataset path='" + path + "'/>"

    xmlinput += "<algorithm app_version='"+algorithm.get('ApplicationVersion', "")+"'"
    xmlinput += " app_family_name='"+algorithm.get('ApplicationFamily', "")+"'"
    xmlinput += " app_executable_name='"+algorithm.get('ExecutableName', "")+"'"
    pset = algorithm.get('ParameterSetID')
    if pset != None: 
       xmlinput += " ps_hash='"+pset.get('Hash', "")+"'"
    xmlinput += "/>"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertParent, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertAlgoInPD', 
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)



  # ------------------------------------------------------------

  def insertRunInPD(self, dataset, run):
    """
    Inserts a new Run in the DBS databse. 
    
    param: 
	aqlgo : The algorithm 
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         tier_name = "GEN-SIM-TEST"
         api.insertParentInPD ("/prim/proc/dt", "/adc/def/rfg")

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    path = self._path(dataset)
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<processed_dataset path='" + path + "'/>"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertParent, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertRunInPD', 
		         'run_number' : run,
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)

  # ------------------------------------------------------------

  def insertLumiSection(self, lumi):
	  
    """
    Inserts a new lumi section in the DBS databse. 
    
    param: 
	lumi : The lumi section passed as an DbsLumiSection obejct. The following fields 
	       are mandatory and should be present in the lumi section object : 
               lumi_section_number, run_number, start_event_number and end_event_number
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         lumi = DbsLumiSection (
                LumiSectionNumber=99,
                StartEventNumber=100,
                EndEventNumber=200,
                LumiStartTime='notime',
                LumiEndTime='neverending',
                RunNumber=1,
         )
         api.insertLumiSection(lumi)

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
  
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<lumi_section "
    xmlinput += " lumi_section_number='"+str(lumi.get('LumiSectionNumber', ''))+"'"
    xmlinput += " run_number='"+str(lumi.get('RunNumber', ''))+"'"
    xmlinput += " start_event_number='"+str(lumi.get('StartEventNumber', ''))+"'"
    xmlinput += " end_event_number='"+str(lumi.get('EndEventNumber', ''))+"'"
    xmlinput += " lumi_start_time='"+lumi.get('LumiStartTime', '')+"'"
    xmlinput += " lumi_end_time='"+lumi.get('LumiEndTime', '')+"'"
    xmlinput += " />"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertLumiSection, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertLumiSection',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)

  # ------------------------------------------------------------
  def insertMergedDataset(self, dataset, merege_ds_name, merge_algo):
    """
    Clones a dataset and add another Algo to it.
    This is done for merged datasets.
    params:
         dataset: the dataset needs to be cloned 
         merge_algo:  merge application that needs to added to cloned dataset.
                      Assuming for now that merge_algo is just ONE Algo object
    """

    path = self._path(dataset) 
    token = path.split("/")

    #print token

    proc = self.listProcessedDatasets(token[1], token[3], token[2])[0]
    logging.log(DBSDEBUG, "proc fetched from DBS %s" %proc)

    proc['Name'] = merege_ds_name
    if merge_algo not in (None, ''):
	#raise DbsApiException(args="You must provide an Algorithm object for the merged dataset")
	#return
        #logging.log(DBSDEBUG, "Algorithm object for the merged dataset is not provided")
        self.insertAlgorithm(merge_algo)  
        proc['AlgoList'].append(merge_algo) 

    #Grab the parents as well.
    proc['ParentList'] = self.listDatasetParents(path)

    #Create the dataset
    self.insertProcessedDataset (proc)

    #Lets grab the Runs as well
    ds_runs = self.listRuns(path)
    #And add the to newly created dataset
    for aRun in ds_runs:
        self.insertRunInPD(proc, aRun['RunNumber'])

    return proc

  # ------------------------------------------------------------
  def insertMergedFile(self, parents, outputFile):
    """
    Inserts a LFN into DBS, while getting all details of input LFNs 
    as PHYSICS and merge "parents" of THIS outputLFN.

    params:
        outputFile: is the output file object with all detals, such a Check sum, Number of events etc.
        Also specifying ParentFiles (The files that were merged into THIS file)
   
    """

    #parents = outputFile['ParentList']
    #Reset parents as REAL parents will be those that are 
    #from the Parent of Unmerged files
    outputFile['ParentList'] = []
    for anInputLFN in parents:
       

        #def listFiles(self, path="", primary="", proc="", tier_list=[], analysisDataset="",blockName="", patternLFN="*", details=None)
        fileDetails = self.listFiles(patternLFN=self._name(anInputLFN), details=True) 
        if len(fileDetails) < 1:
		raise DbsApiException(args="Unmerged file %s not found in DBS" %self._name(anInputLFN), code="1999")
        fileDetail = fileDetails[0] 
        for atier in fileDetail['TierList']:
		if atier not in outputFile['TierList']:
			outputFile['TierList'].append(atier)

        for alumi in fileDetail['LumiList']:
                if alumi not in outputFile['LumiList']:
                        outputFile['LumiList'].append(alumi)

        for algo in fileDetail['AlgoList']:
                if algo not in outputFile['AlgoList']:
                        outputFile['AlgoList'].append(algo)
   
 
        for achild in fileDetail['ChildList']:
                if achild not in outputFile['ChildList']:
                        outputFile['ChildList'].append(achild)

        for aparent in fileDetail['ParentList']:
                if aparent not in outputFile['ParentList']:
                        outputFile['ParentList'].append(aparent)

        # Branches must be same, I hope !!!!!!!!!!
        for abranch in fileDetail['BranchList']:
                if abranch not in outputFile['BranchList']:
                        outputFile['BranchList'].append(abranch)
 
    self.insertFiles(outputFile['Dataset'], [outputFile], outputFile['Block'])

  # ------------------------------------------------------------
  def createAnalysisDataset(self, analysisdataset, defName):
    """
    Creates analysis dataset based on the definition provided

    params:

       analysisdataset: Dict object of type DbsAnalysisDataset, describes this analysis dataset.
                        Following fields are mandatory and should be present in the analysis dataset object : 
                                    name, type, status, path, physics_group_name and annotation
                                          
       def_name:  string: name of Analysis Dataset Definition that define this AnalysisDataset
                                Required and must already exists 
                                (should have been created using createAnalysisDatasetDefinition)  

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError,
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException

    examples:
        analysis = DbsAnalysisDataset(
                Name='TestAnalysisDataset001',
                Annotation='This is a test analysis dataset',
                Type='KnowTheType',
                Status='VALID',
                PhysicsGroup='BPositive'
        )
        api.createAnalysisDataset (analysis, "IcreatedThisDefEarlier")

    """  

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    
    if defName in ("", None):
       raise DbsApiException(args="You must provide AnalysisDatasetDefinition (second parameter of this API call)")
       return
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>" 
    xmlinput += "<analysis_dataset name='"+ analysisdataset.get('Name', '') +"'"
    xmlinput += " analysisds_def_name='"+ defName +"'"
    xmlinput += " annotation='"+ analysisdataset.get('Annotation', '') +"'"
    xmlinput += " type='"+ analysisdataset.get('Type', '') +"'"
    xmlinput += " status='"+ analysisdataset.get('Status', '') +"'"
    xmlinput += " physics_group_name='"+ analysisdataset.get('PhysicsGroup', '') +"'" 
    # Path is taken from the definition 
    #xmlinput += " path='"+path+"'/>"
    xmlinput += " />"
    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    #print xmlinput

    if self.verbose(): 
       print "createAnalysisDataset, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'createAnalysisDataset',
                         'xmlinput' : xmlinput }, 'POST')
    logging.log(DBSDEBUG, data)


    #-----------------------------------------------------------------------------
  def createAnalysisDatasetDefinition(self, analysisDatasetDefinition ):

    """
    Creates a new analysis dataset definition from a combinition of following parameters:
          Name, List-of-lumis, list-of-runs, processed-dataset, tier, run, run-range, 
          lumi-range
    Beside the Name, all other parameters are Optional.

    params: 
         analysisDatasetDefinition: DbsAnalysisDatasetDefinition object

    Note:    
    In case the definition already exists an EXCEPTION will be raised by the Server
    This is to avoid the case that User might use an already existing Definition silently 
    as happens for other tables. By raising exception we are telling user that definition 
    was already created by him/someone with blah blah criteria, so he/she can compare what 
    alraedy defined.

    """

    defName = analysisDatasetDefinition.get('Name')
 
    if defName in ("", None):
       raise DbsApiException(args="You must provide a name for AnalysisDatasetDefinition")
       return
    
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<analysis_dataset_definition analysisds_def_name='"+ defName +"'"
    #xmlinput += " status='"+ analysisdataset.get('Status', '') +"'"
    #xmlinput += " physics_group_name='"+ analysisdataset.get('PhysicsGroup', '') +"'"
    xmlinput += " path='"+analysisDatasetDefinition.get('ProcessedDatasetPath', '')+"'"
    xmlinput += " created_by='"+analysisDatasetDefinition.get('CreationDate', '')+"'"
    xmlinput += " creation_date='"+analysisDatasetDefinition.get('CreationDate', '')+"'"
    xmlinput += " user_cut='"+analysisDatasetDefinition.get('UserCut', '')+"'"
    xmlinput += " description='"+analysisDatasetDefinition.get('Description', '')+"'"
    xmlinput += "/>"

    for aLumi in analysisDatasetDefinition.get('LumiList', []):
       xmlinput += " <lumi_section lumi_section_number='"+aLumi+"'/>"

    for lfn in analysisDatasetDefinition.get('FilesList', []):
       xmlinput += " <file lfn='"+lfn+"'/>"

    for analysisds in  analysisDatasetDefinition.get('AnalysisDSList', []):
        xmlinput += "<analysis_dataset analysis_dataset_name='"+analysisds+"'/>"

    for algorithm in analysisDatasetDefinition.get('AlgoList',[]):
        xmlinput += "<algorithm app_version='"+algorithm.get('ApplicationVersion', "")+"'"
        xmlinput += " app_family_name='"+algorithm.get('ApplicationFamily', "")+"'"
        xmlinput += " app_executable_name='"+algorithm.get('ExecutableName', "")+"'"
        pset = algorithm.get('ParameterSetID')
        if pset != None:
           xmlinput += " ps_hash='"+pset.get('Hash', "")+"'"
        xmlinput += "/>"

    for tier in analysisDatasetDefinition.get('TierList',[]):
        xmlinput += "<data_tier tier_name='"+tier+"'/>"

    for run in analysisDatasetDefinition.get('RunsList',[]):
        xmlinput += "<run run_number='"+str(self._get_run(run))+"'/>"

    for alumiRange in analysisDatasetDefinition.get('LumiRangeList', []):
        xmlinput += "<lumi_section lumi_section_range='"+ alumiRange[0] +','+alumiRange[1]+"'/>"

    for arunRange in analysisDatasetDefinition.get('RunRangeList', []):
        xmlinput += "<run run_range='"+ arunRange[0]+','+arunRange[1]+"'/>"

    xmlinput += "</dbs>"

    logging.log(DBSDEBUG, xmlinput)
    #print xmlinput

    if self.verbose():
       print "createAnalysisDatasetDefinition, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'createAnalysisDatasetDefinition',
                         'xmlinput' : xmlinput }, 'POST')


    #Just return the name of definition if everything went fine.  
    return defName

    logging.log(DBSDEBUG, data)


  # ------------------------------------------------------------
  #def remap(self, eventCollections, outEventCollection, dataset):
  def remap_DEPRECATED(self, files, outFile):

   funcInfo = inspect.getframeinfo(inspect.currentframe())
   logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

   input = "<dbs>"
   for f in files:
        input += "<file-in lfn='%s'/>" % escape (f.get('logicalFileName'))
   input += "<file lfn='%s'/>" % escape (outFile.get('logicalFileName'))
   input += "</dbs>"
    
   logging.log(DBSDEBUG, input)
   #print "calling _server._call remap inside dbsApi"
   data = self._server._call ({ 'api' : 'remap',
		         'xmlinput' : input })
   logging.log(DBSDEBUG, data)

 
#############################################################################
# Unit testing: see $PWD/UnitTests
