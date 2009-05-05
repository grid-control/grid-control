
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *
from xml.sax import SAXParseException

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplGetServerInfo(self):
    """
    Retrieves the server parameters, such as Server version, Schema version
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    # Invoke Server.
    data = self._server._call ({ 'api' : 'getDBSServerVersion' }, 'GET')

    """
	if data in ('', None):
	
	msg = "Unable to parse XML response from DBS Server"
      	msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      	raise DbsBadXMLData(args=msg, code="5999")
    """

    ##logging.log(DBSDEBUG, data)
    # Parse the resulting xml output.
    result = {}

    try:
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'dbs_version':
		result['ServerVersion'] = str(attrs['server_version'])
		if result['ServerVersion'] in ('', None):
			result['ServerVersion'] = 'PROBABLY_CVS_HEAD'
		result['SchemaVersion'] = str(attrs['schema_version'])
		result['InstanceName'] = str(attrs['instance_name'])
		if attrs.has_key('instance_type'):
			result['InstanceType'] = str(attrs['instance_type'])
      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

