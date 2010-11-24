
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplUpdateLumiSection(self, lumi):
	  
    """
    Updates the lumi section in the DBS databse. 
    
    param: 
	lumi : The lumi section passed as an DbsLumiSection obejct. The following fields 
	       are mandatory and should be present in the lumi section object : 
               lumi_section_number, run_number
			  
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
         api.updateLumiSection(lumi)

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
  
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

    if self.verbose():
       print "updateLumiSection, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'updateLumiSection',
                         'xmlinput' : xmlinput }, 'POST')


  # ------------------------------------------------------------
