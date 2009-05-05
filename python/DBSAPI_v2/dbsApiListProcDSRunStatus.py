
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
from dbsRunStatus import DbsRunStatus

def dbsApiImplListProcDSRunStatus(self, path, run):


    """
    List Run status (Complete/Done (1) or otherwise (0) the Proc DS as from DBS database.

    param:

	path: Dataset path (ProcessedDataset): MANDATORY
        run : The dbs run passed in as DbsRun object. 
	      Following values could be updated, one can provide one or all
		NumberOfEvents, NumberOfLumiSections, TotalLuminosity, EndOfRun

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError,
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException

    examples:

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    data = self._server._call ({ 'api' : 'listProcDSRunStatus',
                         'path' : get_path(path),
                         'run_number' : get_run(run),
                         }, 'POST')


    if self.verbose():
       print data

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'run_status':
		
            result.append(DbsRunStatus
				(
				RunNumber=long(attrs['run_number']), 
				Done=long(attrs['done'])
				)  
			) 	
      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


