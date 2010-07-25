
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsRun import DbsRun
from dbsLumiSection import DbsLumiSection
from dbsFile import DbsFile
from dbsFileBlock import DbsFileBlock
from dbsAlgorithm import DbsAlgorithm
from dbsFileTriggerTag import DbsFileTriggerTag
from dbsQueryableParameterSet import DbsQueryableParameterSet
from dbsException import DbsException
from dbsApiException import *
from xml.sax import SAXParseException

import inspect

from dbsUtil import *

from listPADSFiles import listPADSFiles

def dbsApiImplListFiles(self, path="", primary="", proc="", tier_list=[], analysisDataset="",blockName="", patternLFN="", runNumber="", details=None, retriveList=[], otherDetails = False):
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

	branchNTrig: <REMOVED> Want to list File Branches and Triggers, by default False.
         
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

    #path = get_path(dataset)
    # Invoke Server.

    sendTier = string.join(tier_list, "-")

    #if branchNTrig not in ("", None, False):
    #	branchNTrig = "True"

    allowedRetriveValue = ['all',
		    'retrive_invalid_files', 
		    'retrive_status',
		    'retrive_type',
		    'retrive_block',
		    'retrive_date',
		    'retrive_person',
		    'retrive_parent',
		    'retrive_parent_block',
		    'retrive_child',
		    'retrive_algo',
		    'retrive_tier',
		    'retrive_lumi',
		    'retrive_lumi_excluded',
		    'retrive_run',
		    'retrive_branch',
		    ]
    retrive_list = ""
    for i in retriveList:
	    if i not in allowedRetriveValue:
		    raise DbsBadRequest (args="The argument " + i + "  is not allowed argument. The allowed values are " + str(allowedRetriveValue) , code=1500)
	    retrive_list += i + ","
	
    if analysisDataset:
	try:
	    return listPADSFiles(analysisDataset, self)
	except DbsException, dbsex:
	    if str(dbsex).find("not found in DBS MART") != -1:
		pass
	    else:
		raise
	    
    if details not in ("", None, False):
       data = self._server._call ({ 'api' : 'listFiles', 'path' : path, 
				    'primary_dataset' : primary, 
				    'processed_dataset' : proc,
				    'data_tier_list' : sendTier,
		                    'analysis_dataset_name' : analysisDataset,
                                    'block_name' : blockName, 
                                    'other_detail' : str(otherDetails), 
				    'run_number' : str(get_run(runNumber)),
                                    'pattern_lfn' : patternLFN, 'detail' : 'True',
				    'retrive_list' : retrive_list }, 'GET')
                                    #'pattern_lfn' : patternLFN, 'detail' : 'True', 'branchNTrig' : str(branchNTrig) }, 'GET')
    else:
       data = self._server._call ({ 'api' : 'listFiles', 
                                    'primary_dataset': primary,
                                    'processed_dataset' : proc,
                                    'data_tier_list' : sendTier,
                                    'other_detail' : str(otherDetails), 
                                    'path' : path, 'block_name' : blockName, 
		                    'analysis_dataset_name' : analysisDataset,
				    'run_number' : str(get_run(runNumber)),
                                    #'pattern_lfn' : patternLFN, 'branchNTrig' : str(branchNTrig) }, 'GET')
                                    'pattern_lfn' : patternLFN, 
				    'retrive_list' : retrive_list}, 'GET')

    # 
    #  Below contains HINTS as how to use DbsXmlBaseHandler to avoid "double parsing"
    #


    # Parse the resulting xml output.
    try:
      #from dbsXmlBaseHandler import DbsXmlBaseHandler
      result = []
      #class Handler (DbsXmlBaseHandler):
      class Handler (xml.sax.handler.ContentHandler):

        def get(self, attrs, key):
		if key in attrs.keys():
			return attrs[key]
		return ""

        def startElement(self, name, attrs):
          #DbsXmlBaseHandler.startElement(self, name, attrs)
	  auto_x_sec=0.0
          if name == 'file':
	     try:
	     	if attrs.has_key('auto_cross_section'):
	     		auto_x_sec=float(attrs['auto_cross_section'])
             except:
                        auto_x_sec=0.0

             self.currFile = DbsFile (
                                       LogicalFileName=str(attrs['lfn']),
                                       FileSize=getLong(attrs['size']),
                                       NumberOfEvents=getLong(attrs['number_of_events']),
                                       Status=str(self.get(attrs,'status')),
                                       Block=DbsFileBlock(Name=str(self.get(attrs, 'block_name'))),
                                       FileType=str(self.get(attrs,'type')),
                                       Checksum=str(attrs['checksum']),
				       #Adler32=str(self.get(attrs, 'adler32')),
                                       Md5=str(self.get(attrs, 'md5')),
                                       QueryableMetadata=str(attrs['queryable_meta_data']),
				       AutoCrossSection=auto_x_sec,
                                       CreationDate=str(self.get(attrs,'creation_date')),
                                       CreatedBy=str(self.get(attrs,'created_by')),
                                       LastModificationDate=str(self.get(attrs,'last_modification_date')),
                                       LastModifiedBy=str(self.get(attrs,'last_modified_by')),
                                       )
	     Adler32=str(self.get(attrs, 'adler32'))
	     if Adler32 not in ('', 'NOTSET'):
		self.currFile['Adler32']=Adler32

          if name == 'file_data_tier':
            self.currFile['TierList'].append(str(attrs['name']))

          if name == 'file_branch':
            #self.currFile['BranchList'].append(str(attrs['name']))
		self.currFile['BranchHash']=str(self.get(attrs,'hash'))

          if name == 'file_lumi_section':
             self.currFile['LumiList'].append(DbsLumiSection(
                                                   LumiSectionNumber=getLong(attrs['lumi_section_number']),
                                                   StartEventNumber=getLong(attrs['start_event_number']),
                                                   EndEventNumber=getLong(attrs['end_event_number']),   
                                                   LumiStartTime=getLong(attrs['lumi_start_time']),
                                                   LumiEndTime=getLong(attrs['lumi_end_time']),
                                                   RunNumber=getLong(attrs['run_number']),
                                                   CreationDate=str(attrs['creation_date']),
                                                   CreatedBy=str(attrs['created_by']),
                                                   LastModificationDate=str(attrs['last_modification_date']),
                                                   LastModifiedBy=str(attrs['last_modified_by']), 
                                              ))
	  if name == 'file_lumi_section_excluded':
             self.currFile['LumiExcludedList'].append(DbsLumiSection(
                                                   LumiSectionNumber=getLong(attrs['lumi_section_number']),
                                                   StartEventNumber=getLong(attrs['start_event_number']),
                                                   EndEventNumber=getLong(attrs['end_event_number']),   
                                                   LumiStartTime=getLong(attrs['lumi_start_time']),
                                                   LumiEndTime=getLong(attrs['lumi_end_time']),
                                                   RunNumber=getLong(attrs['run_number']),
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
	     auto_x_sec=0.0	
	     try:
	     	if attrs.has_key('auto_cross_section'):
               		auto_x_sec=float(attrs['auto_cross_section'])
             except:
                        auto_x_sec=0.0
             self.currFile['ParentList'].append(DbsFile (
                                       LogicalFileName=str(attrs['lfn']),
                                       FileSize=getLong(attrs['size']),
                                       NumberOfEvents=getLong(attrs['number_of_events']),
                                       Status=str(attrs['status']),
                                       Block=DbsFileBlock(Name=str(attrs['block_name'])),
                                       FileType=str(attrs['type']),
                                       Checksum=str(attrs['checksum']),
                                       Adler32=str(self.get(attrs, 'adler32')),
                                       Md5=str(self.get(attrs, 'md5')),
                                       QueryableMetadata=str(attrs['queryable_meta_data']),
				       AutoCrossSection=auto_x_sec,
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       ))
		

          if name == 'file_child':
             self.currFile['ChildList'].append(DbsFile (
                                       LogicalFileName=str(attrs['lfn']),
                                       FileSize=getLong(attrs['size']),
                                       NumberOfEvents=getLong(attrs['number_of_events']),
                                       Status=str(attrs['status']),
                                       Block=DbsFileBlock(Name=str(attrs['block_name'])),
                                       FileType=str(attrs['type']),
                                       Checksum=str(attrs['checksum']),
                                       Adler32=str(self.get(attrs, 'adler32')),
                                       Md5=str(self.get(attrs, 'md5')),
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
                                   StartOfRun=getLong(attrs['start_of_run']),
                                   EndOfRun=getLong(attrs['end_of_run']),
                                   CreationDate=str(attrs['creation_date']),
                                   CreatedBy=str(attrs['created_by']),
                                   LastModificationDate=str(attrs['last_modification_date']),
                                   LastModifiedBy=str(attrs['last_modified_by']),
                                  ))

	  if name == 'file_trigger_tag':
		self.currFile['FileTriggerMap'].append( DbsFileTriggerTag (
				   TriggerTag=str(attrs['trigger_tag']),
				   NumberOfEvents=getLong(attrs['number_of_events']),
	         		   CreationDate=str(attrs['creation_date']),
                                   CreatedBy=str(attrs['created_by']),
                                   LastModificationDate=str(attrs['last_modification_date']),
                                   LastModifiedBy=str(attrs['last_modified_by']),
				))
          """
	  if name == 'file_assoc':
                self.currFile['FileAssoc'] = DbsFile (
                                       LogicalFileName=str(attrs['lfn']),
                                       FileSize=long(attrs['size']),
                                       NumberOfEvents=long(attrs['number_of_events']),
                                       Status=str(attrs['status']),
                                       Block=DbsFileBlock(Name=str(attrs['block_name'])),
                                       FileType=str(attrs['type']),
                                       Checksum=str(attrs['checksum']),
                                       Adler32=str(self.get(attrs, 'adler32')),,
                                       Md5=str(self.get(attrs, 'md5')),
                                       QueryableMetadata=str(attrs['queryable_meta_data']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       )
          """

        def endElement(self, name):
          if name == 'file':
             result.append(self.currFile)
  
      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")
  
