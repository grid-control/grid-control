
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsProcessedDataset import DbsProcessedDataset
from dbsPrimaryDataset import DbsPrimaryDataset
from dbsAnalysisDataset import DbsAnalysisDataset

from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplListPathParents(self, dataset):
    """
    Retrieves the list of processed dataset which are parents iof the given dataset.

    
    params:
	  dataset: is the processed dataset represented either in string format as path (/prim/datatier/proc) or in DbsProcessedDataset format.
	  This is a mandatory field and is not defaulted
	  
    returns: 
          list of DbsProcessedDataset objects  
    examples: 
          api.listPathParents("/test_primary_anzar_001/TestProcessedDS001/SIM")

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    path = get_path(dataset)

    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listPathParents', 
                                 'path' : path  
				}, 'GET')

    ##logging.log(DBSDEBUG, data)
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'processed_dataset_parent':


		if name == 'processed_dataset_parent':
			parentPath = str(attrs['path'])
			myPath = parentPath.split('/')

			result.append(DbsProcessedDataset ( 
			  			Name=myPath[2],
                                                PrimaryDataset=DbsPrimaryDataset(Name=myPath[1]),
                                                PathList=[parentPath],     
                                                ))

      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


