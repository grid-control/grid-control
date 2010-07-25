
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsAnalysisDataset import DbsAnalysisDataset
from dbsAnalysisDatasetDefinition import DbsAnalysisDatasetDefinition
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListAnalysisDataset(self, pattern="*", path="", version=None):
    """
    Retrieves the list of analysis dataset by matching against the given shell pattern for analysis 
    dataset name.
    Returns a list of DbsAnalysisDataset objects. 

    
    params:
          pattern:  the shell pattren for nanlysis dataset name. If not given then the default value of * is assigned to it and all the datasets are listed
	  path: is the processed dataset path in the format /prim/proc/datatier which if given list all the analysis dataset within that processed dataset
	  version: by DEFAULT LATEST version of Analysis Dataset is returned, User can specify a Version.
    returns: 
          list of DbsAnalysisDataset objects  
    examples: 
          api.listAnalysisDataset("*t005", "/test_primary_anzar_001/TestProcessedDS001/SIM")
          api.listAnalysisDataset()

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    path = get_path(path)

    # Invoke Server.    
    if version not in (None, ''):
    	data = self._server._call ({ 'api' : 'listAnalysisDataset', 
                                 'analysis_dataset_name_pattern' : pattern,
                                 'path' : path,
                                 'version' : str(version)  
				}, 'GET')
    else:
	data = self._server._call ({ 'api' : 'listAnalysisDataset',
                                 'analysis_dataset_name_pattern' : pattern,
                                 'path' : path
                                }, 'GET')

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'analysis_dataset':
		self.curr_analysis = DbsAnalysisDataset (
         		Path=str(attrs['path']),
         		Name=str(attrs['analysis_dataset_name']),
         		Type=str(attrs['type']),
         		Status=str(attrs['status']),
			Version=str(attrs['version']),
         		PhysicsGroup=str(attrs['physics_group_name']),
         		Description=str(attrs['description']),
                        CreationDate=str(attrs['creation_date']),
                        CreatedBy=str(attrs['created_by']),
                        LastModificationDate=str(attrs['last_modification_date']),
                        LastModifiedBy=str(attrs['last_modified_by']),
			)
	  if name == 'analysis_dataset_definition':
                self.curr_def = DbsAnalysisDatasetDefinition (
            		Name=str(attrs['analysis_dataset_definition_name']),
            		ProcessedDatasetPath=str(attrs['path']),
            		UserInput=str(attrs['user_input']),
			SQLQuery=str(attrs['sql_query']),
         		Description=str(attrs['description']),
                        CreationDate=str(attrs['creation_date']),
                        CreatedBy=str(attrs['created_by']),
                        LastModificationDate=str(attrs['last_modification_date']),
                        LastModifiedBy=str(attrs['last_modified_by']),
			)
                self.curr_analysis['Definition'] = self.curr_def	
                result.append(self.curr_analysis)

      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


