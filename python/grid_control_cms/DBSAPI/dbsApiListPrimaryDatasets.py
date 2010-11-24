
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsPrimaryDataset import DbsPrimaryDataset
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListPrimaryDatasets(self, pattern="*"):
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
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listPrimaryDatasets', 'pattern' : pattern  }, 'GET')


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

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

