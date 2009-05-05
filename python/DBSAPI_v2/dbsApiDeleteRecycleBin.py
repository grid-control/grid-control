
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

def dbsApiImplDeleteRecycleBin(self, dataset, block):

    """
    Permanently deletes the block from the recycle bin implemented in DBS server.
    
    param: 
        dataset : The procsssed dataset can be passed as an DbsProcessedDataset object or just as a dataset
                  path in the format of /prim/proc/datatier
        block : The file block can be DbsFileBlock object or string represented in the form /primname/procname/datatier#GUID
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         api.deleteRecycleBin ("/this/hahah/SIM", "/this/hahah/SIM#1234")
 
    """   

    path = get_path(dataset)
    blockName = get_name(block)
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    data = self._server._call ({ 'api' : 'deleteRecycleBin',
                         'path' : path ,
			 'block_name' : blockName}, 'POST')
    ##logging.log(DBSDEBUG, data)

   # ------------------------------------------------------------


