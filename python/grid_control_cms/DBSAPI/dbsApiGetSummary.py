
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsProcessedDataset import DbsProcessedDataset
from dbsPrimaryDataset import DbsPrimaryDataset
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplGetSummary(self, dataset, block):
    """
    List the summary of a processed dataset 

    
    params:
	  dataset: is the processed dataset represented either in string format as path (/prim/datatier/proc) or in DbsProcessedDataset format.
	  This is a mandatory field and is not defaulted
	  block : is the Block name /prim/datatier/proc#guid	  
    returns: 
          summary of dataset in a list  
    examples: 
          api.getSummary(dataset="/test_primary_anzar_001/TestProcessedDS001/SIM")

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    path = get_path(dataset)

    # Invoke Server.    
    data = self._server._call ({ 'api' : 'getSummary', 
                                 'path' : path,
				 'block': block,  
				}, 'GET')

    # Parse the resulting xml output.
    try:
      result = {}
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):

          if name == 'summary':
		result['path'] = path
		result['block'] = block
		result['number_of_blocks'] = str(attrs['number_of_blocks'])
		result['number_of_events'] = str(attrs['number_of_events'])
		result['number_of_files'] = str(attrs['number_of_files'])
		result['number_of_lumis'] = str(attrs['number_of_lumis'])
		result['total_size'] = str(attrs['total_size'])

      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


