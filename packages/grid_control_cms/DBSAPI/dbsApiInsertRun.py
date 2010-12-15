
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplInsertRun(self, run):

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    """
    Inserts a new run in the DBS databse. 
    
    param: 
	run : The dbs run passed in as DbsRun object. The following are mandatory and should be present
	      in the dbs run object: 
	      RunNumber, number_of_events, number_of_lumi_sections, total_luminosity and store_number
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
    
         run = DbsRun (
                 RunNumber=1,
                 NumberOfEvents= 100,
                 NumberOfLumiSections= 20,
                 TotalLuminosity= 2222,
                 StoreNumber= 123,
                 StartOfRun= 1234,
                 EndOfRun= 2345,
         )
 
         api.insertRun (run)

    """

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<run"
    xmlinput += " run_number='"+str(run.get('RunNumber', ''))+"'"
    xmlinput += " number_of_events='"+str(run.get('NumberOfEvents', ''))+"'"
    xmlinput += " number_of_lumi_sections='"+str(run.get('NumberOfLumiSections', ''))+"'"
    xmlinput += " total_luminosity='"+str(run.get('TotalLuminosity', ''))+"'"
    xmlinput += " store_number='"+str(run.get('StoreNumber', ''))+"'"
    xmlinput += " start_of_run='"+str(run.get('StartOfRun', ''))+"'"
    xmlinput += " end_of_run='"+str(run.get('EndOfRun', ''))+"'"
    xmlinput += " />"
    xmlinput += "</dbs>"

    if self.verbose():
       print "insertRun, xmlinput",xmlinput
       
    data = self._server._call ({ 'api' : 'insertRun',
                         'xmlinput' : xmlinput }, 'POST')


# ------------------------------------------------------------
