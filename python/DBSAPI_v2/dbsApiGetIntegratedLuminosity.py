
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

def dbsApiImplGetIntegratedLuminosity(self, path, run =  "", runRange = "", tag = ""):
    """
    Caluates and returns the integrated luminosity in a dataset

    
    params:
    returns: 
          integrated luminosity and error
    examples: 
          api.getIntegratedLuminoisty("/Primary/Processed/Tier") 

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsException	
             
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
 
    # Invoke Server.
    toCall = {}
    toCall['api'] = 'getIntegratedLuminosity'
    toCall['path'] = path
    if run not in (None, ""): 
	    toCall['run'] = run
    if runRange not in (None, ""):
	    toCall['runRange'] = runRange
    if tag not in (None, ""):
	    toCall['tag'] = tag
    """
    data = self._server._call ({ 'api' : 'getIntegratedLuminosity', 'path' : path, 
		    'run' : run, 
		    'runRange' : runRange, 
		    'tag' : tag  }, 
		    'GET')
    """
    data = self._server._call (toCall,    'GET')


    ##logging.log(DBSDEBUG, data)
    # Parse the resulting xml output.
    try:
      result = {}
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'lumi':
		  result['integrated_luminosity'] = attrs['integrated_luminosity']
		  result['error'] = attrs['error']

      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


