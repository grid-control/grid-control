
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

def dbsApiImplMarkPDRunDone(self, path, run):


    """
    Marks a run in the Proc DS as DONE DBS databse.

    param:

	path: Dataset path (ProcessedDataset)
        run : The dbs run passed in as DbsRun object.
	      Following values could be updated, one can provide one or all
		NumberOfEvents, NumberOfLumiSections, TotalLuminosity, EndOfRun

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError,
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException

    examples:

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    data = self._server._call ({ 'api' : 'markPDRunDone',
                         'path' : get_path(path),
                         'run_number' : get_run(run),
                         }, 'POST')



