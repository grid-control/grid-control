
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsDataTier import DbsDataTier
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListFileTiers(self, lfn):
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
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listFileTiers', 'lfn' : lfn  }, 'GET')


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

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

