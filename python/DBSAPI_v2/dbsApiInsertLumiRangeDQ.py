
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

def dbsApiImplInsertLumiRangeDQ(self, runNumber, startLumi, endLumi, dqFlagList):
    """
    This API is used to insert Data Quality information for a *range* of LumiSections within a Run 
    params:
        runNumber: RunNumber of the Run these LumiSections belong to
        startLumi: First lumi section number in the Range of lumi sections
        endLumi: Last lumi section number in the Range of lumi sections
        dqFlagList: List of DQ Flags, each object is of type DbsDQFlag.
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    
    for aFlag in dqFlagList:
            xmlinput += "<dq_sub_system name='" + aFlag.get('Name') + "'  value='" + aFlag.get('Value') + "'  />"
            # Sub sub system list
            for aSubFlag in aFlag.get('SubSysFlagList'):
                    xmlinput += "<dq_sub_subsys name='" + aSubFlag.get('Name') + "'  value='" + aSubFlag.get('Value') + "'  />"
    
    xmlinput += "</dbs>"
    
    ###logging.log(DBSDEBUG, xmlinput)
    
    data = self._server._call ({ 'api' : 'insertLumiRangeDQ',
					'run_number': str(get_run(runNumber)),
                                        'start_lumi': str(startLumi),
                                        'end_lumi': str(endLumi),
                                        'xmlinput' : xmlinput }, 'POST')
    
    ###logging.log(DBSDEBUG, data)
  #-------------------------------------------------------------------

