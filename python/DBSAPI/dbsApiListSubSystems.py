
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsDQFlag import DbsDQFlag
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListSubSystems(self):
    """
    ist all the Sub Systems known to this DBS instance
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    data = self._server._call ({ 'api' : 'listSubSystems'}, 'POST')

    # Parse the resulting xml output.
    try:
      result = []

      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'sub_system': 
			dqSubSys = DbsDQFlag(Name=str(attrs['name']),Parent=str(attrs['parent']))
			result.append(dqSubSys)


      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

