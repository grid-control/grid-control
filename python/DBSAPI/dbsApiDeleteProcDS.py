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

def dbsApiImplDeleteProcDS(self, dataset):

    """
    Moves the dataset path into the recycle bin implemented in DBS server.
    
    param: 
        path : The dataset path represented in the form /primname/procname/datatier
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         api.deleteProcDS ("/this/hahah/SIM")
 
    """   

    path = get_path(dataset)
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    data = self._server._call ({ 'api' : 'deleteProcDS',
                         'path' : path }, 'POST')

   # ------------------------------------------------------------

def dbsApiImplDeleteRecycleBin(self, dataset, block):

    """
    Removes the block from the recycle bin forever. The block cannot be restored after this operation
    
    param: 
        path : The dataset path represented in the form /primname/procname/datatier
        blokc : The block represented in the form /primname/procname/datatier#GUID
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         api.deleteRecycleBin ("/this/hahah/SIM", "/this/hahah/SIM#GUID")
 
    """   

    path = get_path(dataset)
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    data = self._server._call ({ 'api' : 'deleteRecycleBin',
                         'path' : path,
                         'block_name' : block }, 'POST')

   # ------------------------------------------------------------

def dbsApiImplUndeleteProcDS(self, dataset):

    """
    Moves the dataset path from the recycle bin back in the DBS server.
    
    param: 
        path : The dataset path represented in the form /primname/procname/datatier
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         api.deleteProcDS ("/this/hahah/SIM")
 
    """   

    path = get_path(dataset)
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    data = self._server._call ({ 'api' : 'undeleteProcDS',
                         'path' : path }, 'POST')

   # ------------------------------------------------------------

