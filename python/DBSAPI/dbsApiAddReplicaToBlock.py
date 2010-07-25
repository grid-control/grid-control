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

def dbsApiImplAddReplicaToBlock(self, block, storageElement):
    """
    Inserts a new storage element in a given block. 
    
    param: 
        block : The dbs file block passed in as an DbsFileBlock obejct. This object can be  passed in also, 
                as a string containing the block name, instead of DbsFileBlock object. The following fields 
                are mandatory and should be present in the dbs file block object: 
                block_name and storage_element_name
                          
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException  
           
    examples:
         block = DbsFileBlock (
                Name="/TestPrimary1164751189.48/HIT1164751189.48/TestProcessed1164751189.48"
         )
         api.addReplicaToBlock ( block , 'se1')
         
         api.addReplicaToBlock ( "/this/hahah/SIM#12345" , 'se2')

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    bname = get_name(block)
    sename = get_name(storageElement)
    
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    if (storageElement not in ( '', None)) : 
          xmlinput += " <storage_element block_name='" + bname + "' storage_element_name='"+ sename +"'/>"
    xmlinput += "</dbs>"


    data = self._server._call ({ 'api' : 'insertStorageElement',
                         'xmlinput' : xmlinput }, 'POST')


  # ------------------------------------------------------------

