# Revision: $"
# Id: $"


import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplDeleteADS(self, ads, version):

    """
    Deletes an ADS 

    Params:
        ads : Name of the ADS to be deleted
	version: version of that ADS (As ADS can have multiple versions, 
			starting from 0, its a required parameter)
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         api.deleteADS ("/GlobalCruzet1-A/Online-CMSSW_2_0_4/RAW/TESTADS01", 0)
 
    """   

    if version in ('', None):
	raise DbsBadData("ADS can have multiple versions, starting from 0, \
			provide the version that you want to delete", code = 1211)
	return
	
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    data = self._server._call ({ 'api' : 'deleteADS',
                         'version' : version, 'ads' : ads }, 'POST')

