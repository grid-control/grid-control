import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsLumiSection import DbsLumiSection
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListRecycleBin(self, path=""):
    # Invoke Server.    
    #import pdb
    #pdb.set_trace()
    if path=="":
	data = self._server._call ({ 'api' : 'listRecycleBin'}, 'GET')
	##print data
    else:	
	data = self._server._call ({ 'api' : 'listRecycleBin', 'path' : path  }, 'GET')
    try:
	result = []
	class Handler(xml.sax.handler.ContentHandler):

	    def startElement(self, name, attrs):
		if name=='recycle_bin':
		    result.append({'path':str(attrs['path']), 'block':str(attrs['block']), 'creationdate':str(attrs['creationdate'])
		    , 'createdby': str(attrs['createdby'])})
		    #print 'data'
		    #print "path=%s, block=%s, date=%s, name=%s" %(attrs['path'], attrs['block'], str(attrs['creationdate'], attrs['createdby'])
	xml.sax.parseString (data, Handler ())
	return result
    except SAXParseException, ex:
	msg = "Unable to parse XML response from DBS Server"
	msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
	raise DbsBadXMLData(args=msg, code="5999")
