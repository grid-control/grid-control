
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

def dbsApiImplDeleteReplicaFromBlock(self, block, storage_element):

    """
    Deletes the Storage Element assocaition with the Block in the DBS.
    
    param: 
        block : The dbs file block passed in as a string containing the block name or a dbsFileBlock object.
        storage_element : The name of storage element in the string format. Please note that if the user does not provide any
        of these two parameters then all the file blcoks with thier storage elements relationships will get deleted.
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         api.deleteReplicaFromBlock ("/this/hahah/SIM#12345", "se1")


        Note that se1 is a STRING not a StorageElement Object
 
    """   

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    bname = get_name(block)
    sename = get_name(storage_element)
    
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<storage_element storage_element_name='"+ sename +"' block_name='"+ bname +"'/>"
    xmlinput += "</dbs>"

    ###logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "deleteReplicaFromBlock, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'deleteSEFromBlock',
                         'xmlinput' : xmlinput }, 'POST')
    ###logging.log(DBSDEBUG, data)

   # ------------------------------------------------------------


