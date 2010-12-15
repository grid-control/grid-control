
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplInsertRunLumiDQ(self, dataset, runLumiDQList):

    """
    This API is used to insert Data Quality information for a Run, a List of Runs, a LumiSection within a Run, or a List of Lumi Sections

    params: 
         runLumiDQList : Takes LIST of DbsRunLumiDQ obkjects, each representing a Run, a LumiSection withina Run
			 with a list of Data Quality Flags (Sub-System and Sub-Sub-System Flags and their values)
			 defined as "DQFlagList". Each object of this list if of type DbsDQFlag.

	DbsRunLumiDQ = {
        "RunNumber" : { "Comment" : "REQUIRED", "Validator" : isLongType },
        "LumiSectionNumber" : { "Comment" : "Optional LumiSection Number, Unique within this Run", "Validator" : isLongType },
        "DQFlagList" : { "Comment" : "List of DbsDQFlag Objects, representing Sub-System and Sub-SubSystem Flags", "Validator" : isListType },
        "CreationDate" : { "Comment" : "TimeStamp, object created in database (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
        "CreatedBy" : { "Comment" : "User DN, who created this object (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
        "LastModificationDate" : { "Comment" : "Last Modification, (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
        "CreatedBy" : { "Comment" : "User DN of who last modified this object (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
        }

        If LumiSectionNumber is provided, then the object represents a LumiSection, otherwise it represents a Run with RunNumber
    

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


    data = self._server._call ({ 'api' : 'insertRunLumiDQ',
                         'dataset' : get_path(dataset), 'xmlinput' : xmlinput }, 'POST')

  #-------------------------------------------------------------------

