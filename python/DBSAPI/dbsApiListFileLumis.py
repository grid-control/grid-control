
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsLumiSection import DbsLumiSection
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplListFileLumis(self, lfn):
    """
    Retrieves the list of parents of the given file lfn.
    Returns a list of DbsLumiSection objects.  If the lfn is not
    given, then it will raise an exception.

    
    params:
          lfn:  the logical file name of the file whose lumi sections needs to be listed. There is no default value for lfn.
    returns: 
          list of DbsLumiSection objects  
    examples: 
          api.listFileLumis("aaaa2233-uuuuu-9767-8764aaaa") : List ALL lumi sections for given LFN

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listFileLumis', 'lfn' : lfn  }, 'GET')

    ##logging.log(DBSDEBUG, data)
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'file_lumi_section':
             result.append(DbsLumiSection (
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


      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


