
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsAnalysisDataset import DbsAnalysisDataset
from dbsCompositeAnalysisDataset import DbsCompositeAnalysisDataset

from dbsException import DbsException
from dbsApiException import *
from xml.sax import SAXParseException
import inspect

from dbsUtil import *

def dbsApiImplListCompADS(self, pattern="*"):
    """
    Retrieves the list of comp analysis dataset by matching against the given shell pattern for analysis 
    dataset name.
    Returns a list of ComADS objects. 

    params:
          pattern:  the shell pattren for comp Ananlysis dataset name. If not given then the default value of * is assigned to it and all the datasets are listed
    returns: 
          list of DbsCompositeAnalysisDataset objects  
    examples: 
          api.listCompADS("*t005")
          api.listCompADS()

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listCompADS', 
                                 'pattern' : pattern,
				}, 'GET')

    # Parse the resulting xml output.

    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):

          if name == 'comp_analysis_dataset':
                self.curr_compads = DbsCompositeAnalysisDataset(
                        Name=str(attrs['name']),
                        Description=str(attrs['description']),
                        CreationDate=str(attrs['creation_date']),
                        CreatedBy=str(attrs['created_by']),
                        LastModificationDate=str(attrs['last_modification_date']),
                        LastModifiedBy=str(attrs['last_modified_by']),
			)
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
		self.curr_compads['ADSList'].append(self.curr_analysis)

        def endElement(self, name):
          if name == 'comp_analysis_dataset':
		result.append(self.curr_compads)

      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


