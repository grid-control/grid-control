
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplInsertLumiSection(self, lumi):
	  
    """
    Inserts a new lumi section in the DBS databse. 
    
    param: 
	lumi : The lumi section passed as an DbsLumiSection obejct. The following fields 
	       are mandatory and should be present in the lumi section object : 
               lumi_section_number, run_number, start_event_number and end_event_number
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         lumi = DbsLumiSection (
                LumiSectionNumber=99,
                StartEventNumber=100,
                EndEventNumber=200,
                LumiStartTime=1234,
                LumiEndTime='neverending',
                RunNumber=1,
         )
         api.insertLumiSection(lumi)

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
  
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<lumi_section "
    xmlinput += " lumi_section_number='"+str(lumi.get('LumiSectionNumber', ''))+"'"
    xmlinput += " run_number='"+str(lumi.get('RunNumber', ''))+"'"
    xmlinput += " start_event_number='"+str(lumi.get('StartEventNumber', ''))+"'"
    xmlinput += " end_event_number='"+str(lumi.get('EndEventNumber', ''))+"'"
    xmlinput += " lumi_start_time='"+str(lumi.get('LumiStartTime', ''))+"'"
    xmlinput += " lumi_end_time='"+str(lumi.get('LumiEndTime', ''))+"'"
    xmlinput += " />"
    xmlinput += "</dbs>"

    ###logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertLumiSection, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertLumiSection',
                         'xmlinput' : xmlinput }, 'POST')
    ###logging.log(DBSDEBUG, data)

  # ------------------------------------------------------------
