
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsFile import DbsFile
from dbsFileBlock import DbsFileBlock
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListFileParents(self, lfn):
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
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listFileParents', 'lfn' : lfn  }, 'GET')

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def get(self, attrs, key):
                if key in attrs.keys():
                        return attrs[key]
                return ""

        def startElement(self, name, attrs):
          if name == 'file_parent':
             result.append( DbsFile (
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

 
      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

