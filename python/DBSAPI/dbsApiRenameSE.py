
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplRenameSE(self, storage_element_from, storage_element_to):
    """
    Renames the Storage Element  in the DBS.
    
    param: 
	storage_element_from : The name of storage element in the string format or object format that needs to be changed
	storage_element_to : The name of storage element in the string format or object format that it will be changed to.
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         api.renameSE ("se1", "se2")

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    seNameFrom = get_name(storage_element_from)
    seNameTo = get_name(storage_element_to)
    
    data = self._server._call ({ 'api' : 'updateSEName',
		    'storage_element_name_from' : seNameFrom,
		    'storage_element_name_to' : seNameTo }, 'POST')
 # ------------------------------------------------------------

