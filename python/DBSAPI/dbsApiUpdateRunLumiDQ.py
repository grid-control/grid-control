
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplUpdateRunLumiDQ(self, dataset, runLumiDQList):
    """
    This API is used to UPDATE Data Quality information for a Run, a List of Runs, a LumiSection within a Run, or a List of Lumi Sections
    
    params: 
         runLumiDQList : Takes LIST of DbsRunLumiDQ objects, each representing a Run OR a LumiSection within a Run
                         with a list of Data Quality Flags (Sub-System and Sub-Sub-System Flags and their values)
                         defined as "DQFlagList". Each object of this list if of type DbsDQFlag.
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"

    for aRunLumiDQ in runLumiDQList:
        xmlinput += "<run run_number='"+str(get_run(aRunLumiDQ.get('RunNumber'))) + "'"
        xmlinput += " lumi_section_number='"+str(aRunLumiDQ.get('LumiSectionNumber', ''))+"'"
        xmlinput += " />"

        for aFlag in aRunLumiDQ.get('DQFlagList'):
                xmlinput += "<dq_sub_system name='" + aFlag.get('Name') + "'  value='" + aFlag.get('Value') + "'  />"
                # Sub sub system list
                for aSubFlag in aFlag.get('SubSysFlagList'):
                        xmlinput += "<dq_sub_subsys name='" + aSubFlag.get('Name') + "'  value='" + aSubFlag.get('Value') + "'  />"

    xmlinput += "</dbs>"


    data = self._server._call ({ 'api' : 'updateRunLumiDQ','dataset':dataset,
                         'xmlinput' : xmlinput }, 'POST')

  #-------------------------------------------------------------------

