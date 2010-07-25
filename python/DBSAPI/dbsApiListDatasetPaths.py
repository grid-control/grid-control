import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsProcessedDataset import DbsProcessedDataset
from dbsPrimaryDataset import DbsPrimaryDataset
from dbsAlgorithm import DbsAlgorithm
from dbsQueryableParameterSet import DbsQueryableParameterSet
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *


def dbsApiImplListDatasetPaths(self):

    """
	List all the datasets Paths known to THIS DBS 

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listDatasetPaths'}, 'GET')

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):


        def startElement(self, name, attrs):
          if name == 'processed_dataset':
		result.append(str(attrs['path']))
		
      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


