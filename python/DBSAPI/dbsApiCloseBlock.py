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

def dbsApiImplCloseBlock(self, block=None ):
    """
    Updates the dbs file block states to closed. 
    
    param: 
	block : The dbs file block passed in as a string containing the block name or as a dbsFileBlock object. 
	This field is mandatory.
			  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         api.closeBlock ("/this/hahah/SIM#12345")

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())

    # Invoke Server.
    block_name = get_name(block)
    data = self._server._call ({ 'api' : 'closeBlock', 'block_name' : block_name }, 'POST')

    
   # ------------------------------------------------------------

