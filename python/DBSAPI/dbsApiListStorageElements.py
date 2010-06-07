
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsStorageElement import DbsStorageElement
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplListStorageElements(self, storage_element_name="*"):
    """
    Retrieve list of all storage elements that matches a shell glob pattern for storage element name

    returns: list of DbsStorageElement objects.

    params:
        storage_element_name: pattern, if provided it will be matched against the content as a shell glob pattern
         
    raise: DbsApiException.

    examples:

      All Storage elements matching *SE*
           api.listStorageElements("*SE*")
	   
      All Storage elements
           api.listStorageElements()
      

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    # Invoke Server.
    data = self._server._call ({ 'api' : 'listStorageElements', 'storage_element_name' : storage_element_name }, 'GET')
    ##logging.log(DBSDEBUG, data)


    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'storage_element':
               result.append(DbsStorageElement(Name=str(attrs['storage_element_name']),
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


