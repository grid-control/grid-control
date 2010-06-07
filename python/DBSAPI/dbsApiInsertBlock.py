
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *
from xml.sax import SAXParseException

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplInsertBlock(self, dataset, block=None, storage_element_list=None, open_for_writing='y'):
    """
    Inserts a new dbs file block in a given processed dataset. 
    
    param: 
        dataset : The procsssed dataset can be passed as an DbsProcessedDataset object or just as a dataset 
	          path in the format of /prim/proc/datatier  
	          [PATH is mandatory, To Support DBS-1 block which do not have Tiers in BlockName]

	block : The dbs file block passed in as a string containing the block name. This field is not mandatory.
	        If the block name is not provided the server creates one based on the primary dataset name, processed
		dataset name and tiers (from Path) and a random GUID. It returns back this newly created block
			  
	storage_element : The list of storage element names in the string format. This field is not mandatory. If 
	                  this field is not provided then just the block is inserted without any storage element 
			  associated with it.

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
	 proc = DbsProcessedDataset (
               PrimaryDataset=primary,
               Name="TestProcessedDS002",
               TierList=['SIM', 'RECO'],
         )
         api.insertBlock (proc)

         api.insertBlock (proc, "/this/hahah/SIM#12345")
    
         api.insertBlock (proc, "/this/hahah/SIM#12345",  [se1Obj, se2Obj, se3Obj])
	 
         api.insertBlock (proc, "",  ['se1', 'se2', 'se3'])
    
         api.insertBlock ("/test_primary_anzar_001/TestProcessedDS002/SIM" , "/this/hahah/SIM#12345")
    
         api.insertBlock ("/test_primary_anzar_001/TestProcessedDS002/SIM , "/this/hahah/SIM#12345",  [se1Obj, se2Obj, se3Obj])
	 
         api.insertBlock ("/test_primary_anzar_001/TestProcessedDS002/SIM , "",  [se1Obj, se2Obj, se3Obj])
    

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    path = get_path(dataset)
    name = get_name(block)

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<block name='"+ name +"'"
    if type(block) != type("str") and block != None :
       xmlinput += " open_for_writing='"+block.get('OpenForWriting', "")+"'"
    """
    if (isinstance(dataset, DbsProcessedDataset)) :
	xmlinput += " primary_dataset='"+dataset['PrimaryDataset']['Name']+"'"
	xmlinput += " processed_dataset='"+dataset['Name']+"'"
	xmlinput += " path='' >"
    else :
    	xmlinput += " path='"+path+"'>"
    """
    xmlinput += " path='"+path+"'>"

    if (storage_element_list) not in ( [], None ) : 
         for aSe in storage_element_list:
            xmlinput += " <storage_element storage_element_name='"+get_name(aSe)+"'/>"
    xmlinput += "</block>"  
    xmlinput += "</dbs>"

    ###logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertBlock, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'insertBlock',
                         'xmlinput' : xmlinput }, 'POST')
    ###logging.log(DBSDEBUG, data)

    # Parse the resulting xml output.
    try:
     result = []
     class Handler (xml.sax.handler.ContentHandler):

      def startElement(self, name, attrs):
        if name == 'block':
             result.append(str(attrs['block_name'])) 
     xml.sax.parseString (data, Handler ())
     if len(result) > 0:
        return result[0] 
     else: 
        return None   

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

    except Exception, ex:
      raise DbsBadResponse(exception=ex)


  # ------------------------------------------------------------
