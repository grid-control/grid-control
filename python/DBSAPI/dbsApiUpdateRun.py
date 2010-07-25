
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplUpdateRun(self, run):


    """
    Updates a run in the DBS databse.

    param:
        run : The dbs run passed in as DbsRun object. RunNumber is mandatory
	      Following values could be updated, one can provide one or all
		NumberOfEvents, NumberOfLumiSections, TotalLuminosity, EndOfRun

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError,
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException

    examples:

         run = DbsRun (
                 RunNumber=1,
                 NumberOfEvents= 100,
                 NumberOfLumiSections= 20,
                 TotalLuminosity= 2222,
                 EndOfRun= 'never',
         )

         api.insertRun (run)

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<run"
    xmlinput += " run_number='"+str(run.get('RunNumber', ''))+"'"
    xmlinput += " number_of_events='"+str(run.get('NumberOfEvents', ''))+"'"
    xmlinput += " total_luminosity='"+str(run.get('TotalLuminosity', ''))+"'"
    xmlinput += " end_of_run='"+str(run.get('EndOfRun', ''))+"'"
    xmlinput += " />"
    xmlinput += "</dbs>"

    if self.verbose():
       print "insertRun, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'updateRun',
                         'xmlinput' : xmlinput }, 'POST')

  # ------------------------------------------------------------
