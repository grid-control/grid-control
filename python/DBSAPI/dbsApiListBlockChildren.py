#!/usr/bin/env python
#
# Revision: $"
# Id: $"
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsFileBlock import DbsFileBlock
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListBlockChildren(self, blockname):
    """
    Retrieves the list of children of the given Block.
    Returns a list of DbsFileBlock objects.  

    params:
          blockname:  the valid name of the block whose parents needs to be listed. 
	  There is no default value for the blockname.
    returns: 
          list of DbsFileBlock objects  
    examples: 
          api.listFileBlockChildren(blockname="/a/b/c#1234") : List ALL children for given Block

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """

    parent_or_child = 'CHILDREN'

    funcInfo = inspect.getframeinfo(inspect.currentframe())
 
    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listBlockProvenance', 'block_name' : get_name(blockname), 'parent_or_child': parent_or_child  }, 'GET')


    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'block':
               self.currBlock = DbsFileBlock(
                                       Name=str(attrs['name']),
                                       Path=str(attrs['path']),
                                       BlockSize=getLong(attrs['size']),
                                       NumberOfFiles=getLong(attrs['number_of_files']),
                                       NumberOfEvents=getLong(attrs['number_of_events']),
                                       OpenForWriting=str(attrs['open_for_writing']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       )

          if name == 'storage_element':
                role=""
                if attrs.has_key('role'):
                        role=str(attrs['role'])
                self.currBlock['StorageElementList'].append(DbsStorageElement(
                                                                Name=str(attrs['storage_element_name']),
                                                                Role=role
                                                                )
                                                          )
        def endElement(self, name):
          if name == 'block':
             result.append(self.currBlock)

      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

