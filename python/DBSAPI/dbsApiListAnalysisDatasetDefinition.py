
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsAnalysisDatasetDefinition import DbsAnalysisDatasetDefinition
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListAnalysisDatasetDefinition(self, pattern="*"):
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

    # Invoke Server.
    data = self._server._call ({ 'api' : 'listAnalysisDatasetDefinition',
				 'pattern_analysis_dataset_definition_name' : pattern 
				}, 'GET')

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'analysis_dataset_definition':
                curr_def = DbsAnalysisDatasetDefinition (
                        Name=str(attrs['analysis_dataset_definition_name']),
                        ProcessedDatasetPath=str(attrs['path']),
                        #Description=str(attrs['name']),
			UserInput=str(attrs['user_input']),
                        SQLQuery=str(attrs['sql_query']),
                        CreationDate=str(attrs['creation_date']),
                        CreatedBy=str(attrs['created_by']),
                        LastModificationDate=str(attrs['last_modification_date']),
                        LastModifiedBy=str(attrs['last_modified_by']),
                        )
                result.append(curr_def)

      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")



