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

def dbsApiImplDeleteBlock(self, dataset, block):

    """
    Moves the block into the recycle bin implemented in DBS server.
    
    param: 
        dataset : The procsssed dataset can be passed as an DbsProcessedDataset object or just as a dataset
                  path in the format of /prim/proc/datatier
        block : The file block can be DbsFileBlock object or string represented in the form /primname/procname/datatier#GUID
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         api.deleteBlock ("/this/hahah/SIM", "/this/hahah/SIM#1234")
 
    """   

    path = get_path(dataset)
    blockName = get_name(block)
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    data = self._server._call ({ 'api' : 'deleteBlock',
                         'path' : path ,
			 'block_name' : blockName}, 'POST')

   # ------------------------------------------------------------

def dbsApiImplUndeleteBlock(self, dataset, block):

    """
    Moves the block from the recycle bin back in the DBS server.
    
    param: 
        dataset : The procsssed dataset can be passed as an DbsProcessedDataset object or just as a dataset
                  path in the format of /prim/proc/datatier

        block : The file block can be DbsFileBlock object or string represented in the form /primname/procname/datatier#GUID
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         api.deleteBlock ("/this/hahah/SIM", "/this/hahah/SIM#1234")
 
    """   

    path = get_path(dataset)
    blockName = get_name(block)
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    data = self._server._call ({ 'api' : 'undeleteBlock',
                         'path' : path ,
			 'block_name' : blockName}, 'POST')

   # ------------------------------------------------------------

