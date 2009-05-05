
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

def dbsApiImplInsertTierInPD(self, dataset, tier_name):
    """
    Inserts a new tier in the DBS databse. 
    
    param: 
	tier_name : The data tier name passed in as string 
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         tier_name = "GEN-SIM-TEST"
         api.insertTierInPD ("/prim/dt/proc", tier_name)

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    path = get_path(dataset)
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<processed_dataset path='" + path + "'/>"
    xmlinput += "</dbs>"

    ###logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertTier, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertTierInPD', 
		         'tier_name' : tier_name,
                         'xmlinput' : xmlinput }, 'POST')
    ###logging.log(DBSDEBUG, data)


  # ------------------------------------------------------------

