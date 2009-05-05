
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsFileBlock import DbsFileBlock
from dbsStorageElement import DbsStorageElement

from dbsException import DbsException
from dbsApiException import *
from xml.sax import SAXParseException

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplListBlocks(self, dataset=None, block_name="*", storage_element_name="*",  userType="NORMAL"):
    """
    Retrieve list of Blocks matching shell glob pattern for Block Name and/or 
    Storage Element Name, for a dataset path.  All the three parameters are optional.
    

    returns: list of DbsFileBlock objects.

    params:
        dataset: An optional field. It represent the dataset path in the format /prim/proc/dt . The user can leave it empty
        block_name: pattern, if provided it will be matched against the content as a shell glob pattern
        storage_element_name: pattern, if provided it will be matched against the content as a shell glob pattern
         
    raise: DbsApiException.

    examples:

      All Blocks from path /test_primary_001/TestProcessedDS001/SIM
         api.listBlocks("/test_primary_001/TestProcessedDS001/SIM") 
      Block from path /test_primary_001/TestProcessedDS001/SIM whoes name is /this/hahah/SIM#12345
           api.listBlocks("/TestPrimary1167862926.47/TestProcessed1167862926/SIM", "/this/hahah/SIM#12345"):
      All Blocks from path /test_primary_001/TestProcessedDS001/SIM whoes name starts with /this/*
           api.listBlocks("/TestPrimary1167862926.47/TestProcessed1167862926/SIM", "/this/*"):
      All Blocks with a storage element name starting with SE3
           api.listBlocks(storage_element_name="SE3*"):
      All Storage elements within a block name starting with /this/haha
           api.listBlocks(block_name="/this/haha*"):
      All Blocks with all storage element with in any dataset
           api.listBlocks():

      Using a Dataset Object 
            primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
            proc = DbsProcessedDataset (
                            PrimaryDataset=primary,
                            Name="TestProcessedDS002",
                            PhysicsGroup="BPositive",
                            Status="VALID",
                            TierList=['SIM', 'RECO'],
                            AlgoList=[WhateverAlgoObject],
                            )
             api.listBlocks(proc)

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    # Invoke Server.
    path = get_path(dataset)
    data = self._server._call ({ 'api' : 'listBlocks', 'path' : path, 
		    'block_name' : block_name, 
		    'storage_element_name' : storage_element_name ,
		    'user_type' : userType}, 'GET')
    ##logging.log(DBSDEBUG, data)


    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'block':
               self.currBlock = DbsFileBlock(
                                       Name=str(attrs['name']), 
                                       Path=str(attrs['path']), 
                                       BlockSize=getLong(attrs['size']),
                                       NumberOfFiles=getLong(attrs['number_of_files']),
                                       NumberOfEvents=getLong(attrs['number_of_events']),
                                       OpenForWriting=str(attrs['open_for_writing']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       )

          if name == 'storage_element':
		role=""
		if attrs.has_key('role'):
			role=str(attrs['role'])
		self.currBlock['StorageElementList'].append(DbsStorageElement(
								Name=str(attrs['storage_element_name']),
								Role=role
								)
							  )		
	       

        def endElement(self, name):
          if name == 'block':
             result.append(self.currBlock)
  
      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


