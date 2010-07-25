# Revision: $
# Id: $
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplInsertAlgoInPD(self, dataset, algorithm):
    """
    Inserts a new algorithm in the DBS databse. 
    
    param: 
	aqlgo : The algorithm 
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         tier_name = "GEN-SIM-TEST"
         api.insertParentInPD ("/prim/proc/dt", "/adc/def/rfg")

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    path = get_path(dataset)
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<processed_dataset path='" + path + "'/>"

    xmlinput += "<algorithm app_version='"+algorithm.get('ApplicationVersion', "")+"'"
    xmlinput += " app_family_name='"+algorithm.get('ApplicationFamily', "")+"'"
    xmlinput += " app_executable_name='"+algorithm.get('ExecutableName', "")+"'"
    pset = algorithm.get('ParameterSetID')
    if pset != None: 
       xmlinput += " ps_hash='"+pset.get('Hash', "")+"'"
    xmlinput += "/>"
    xmlinput += "</dbs>"

    if self.verbose():
       print "insertParent, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertAlgoInPD', 
                         'xmlinput' : xmlinput }, 'POST')



  # ------------------------------------------------------------

