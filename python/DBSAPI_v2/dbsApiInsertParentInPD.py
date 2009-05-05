
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

def dbsApiImplInsertParentInPD(self, dataset, parentDS):
    """
    Inserts a new parent in the DBS databse. 
    
    param: 
	parentDS : The dataset oath of the parent passed in as string 
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         tier_name = "GEN-SIM-TEST"
         api.insertParentInPD ("/prim/proc/dt", "/adc/def/rfg")

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    path = get_path(dataset)
    parentPath = get_path(parentDS)
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<processed_dataset path='" + path + "'/>"
    xmlinput += "</dbs>"

    ###logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertParent, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertParentInPD', 
		         'parent_path' : parentPath,
                         'xmlinput' : xmlinput }, 'POST')
    ###logging.log(DBSDEBUG, data)


  # ------------------------------------------------------------

