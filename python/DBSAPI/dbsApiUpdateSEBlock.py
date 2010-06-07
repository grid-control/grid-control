
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

def dbsApiImplUpdateSEBlock(self, blockName, storage_element_from, storage_element_to):
    """
    Updates the Storage Element in the spoecified block from torage_element_from to storage_element_to
    
    param: 
	blockName : the name of the block in which storage element will be changed
	storage_element_from : The name of storage element in the string format or object format that needs to be changed
	storage_element_to : The name of storage element in the string format or object format that it will be changed to.
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         api.updateSEBlock ("/pri/proc/tier#1234", "se1", "se2")

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ####logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    seNameFrom = get_name(storage_element_from)
    seNameTo = get_name(storage_element_to)
    
    name = get_name(blockName)

    data = self._server._call ({ 'api' : 'updateSEBlock',
		    'block_name' : name,
		    'storage_element_name_from' : seNameFrom,
		    'storage_element_name_to' : seNameTo }, 'POST')
    ####logging.log(DBSDEBUG, data)

def dbsApiImplUpdateSEBlockRole(self, blockName, storage_element, role):
    """
    Updates the role in Storage Element Block combination to the specified role
    
    param: 
	blockName : the name of the block in which storage element will be changed
	storage_element : The name of storage element in the string format or object format that needs to be changed
	role : The role name to be updated to. Allowed valued are 'Y' or 'N'
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         api.updateSEBlockRole ("/pri/proc/tier#1234", "se1", "Y")

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ####logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    seNameFrom = get_name(storage_element)
    
    name = get_name(blockName)

    data = self._server._call ({ 'api' : 'updateSEBlockRole',
		    'block_name' : name,
		    'storage_element_name' : seNameFrom,
		    'role' : role}, 'POST')
    ####logging.log(DBSDEBUG, data)

