
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplInsertRunRangeDQ(self, startRun, endRun, dqFlagList):
    """
    This API is used to insert Data Quality information for a *range* of Runs 

    params:
        startRun: First run number in the Range of Runs
        endRun: Last run number in the Range of Runs
        dqFlagList: List of DQ Flags, each object is of type DbsDQFlag.

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"

    for aFlag in dqFlagList:
            xmlinput += "<dq_sub_system name='" + aFlag.get('Name') + "'  value='" + aFlag.get('Value') + "'  />"
            # Sub sub system list
            for aSubFlag in aFlag.get('SubSysFlagList'):
                    xmlinput += "<dq_sub_subsys name='" + aSubFlag.get('Name') + "'  value='" + aSubFlag.get('Value') + "'  />"

    xmlinput += "</dbs>"


    data = self._server._call ({ 'api' : 'insertRunRangeDQ',
					'start_run': str(get_run(startRun)), 
					'end_run': str(get_run(endRun)),
                         		'xmlinput' : xmlinput }, 'POST')


  #-------------------------------------------------------------------

