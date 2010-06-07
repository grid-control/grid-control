
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsPrimaryDataset import DbsPrimaryDataset
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplExecuteQuery(self, query="*", begin="", end="", type="exe", ignoreCase=True):
    """
    """
    try: 
      funcInfo = inspect.getframeinfo(inspect.currentframe())
      ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

      # Invoke Server.    
      upper = "True"
      if not ignoreCase: upper = "False"
      data = self._server._callOriginal ({ 'api' : 'executeQuery', 'query' : query , 
		      'begin':str(begin),
		      'end':str(end),
		      'type':type,
		      'upper':upper
		      }, 'GET')

      ###logging.log(DBSDEBUG, data)

      # No parsing nothing at this point, lets just return the data.
      return data

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

def dbsApiImplCountQuery(self, query="*", ignoreCase=True):
    """
    """
    try: 
      funcInfo = inspect.getframeinfo(inspect.currentframe())
      ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

      # Invoke Server.    
      upper = "True"
      if not ignoreCase: upper = "False"
      data = self._server._callOriginal ({ 'api' : 'countQuery', 'query' : query , 
		      'upper':upper
		      }, 'GET')

      ###logging.log(DBSDEBUG, data)
      result = []
      class Handler (xml.sax.handler.ContentHandler):
		      def startElement(self, name, attrs):
			      if name == 'result':
					result.append(attrs['CNT'])
					#print 'result is '
					#print result
      xml.sax.parseString (data, Handler ())
      if len(result) == 0:
	      return 0
      else :
	      return int(result[0])

    except SAXParseException, ex:
      	msg = "Unable to parse XML response from DBS Server"
	msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
        raise DbsBadXMLData(args=msg, code="5999")

def dbsApiImplExecuteSummary(self, query, begin="", end="", sortKey="", sortOrder=""):
    """
    """
    try: 
      funcInfo = inspect.getframeinfo(inspect.currentframe())
      ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

      # Invoke Server.    
      data = self._server._callOriginal ({ 'api' : 'executeSummary', 'query' : query , 
		      'begin':str(begin),
		      'end':str(end),
		      'sortKey':sortKey,
		      'sortOrder':sortOrder
		      }, 'GET')

      ###logging.log(DBSDEBUG, data)

      # No parsing nothing at this point, lets just return the data.
      return data

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

