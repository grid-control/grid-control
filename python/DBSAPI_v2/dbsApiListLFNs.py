
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsFile import DbsFile
from dbsFileTriggerTag import DbsFileTriggerTag
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplListLFNs(self, path="", queryableMetaData=""):
    """
    Retrieve list of LFN in a dataset in a block that is closed and with a particular MetaData

    returns: list of DbsFile objects

    params: 

	path : STRICTLY is the DBS Data PATH in its definition. CANNOT be substituted for anything else like Processed Dataset.
	Note that if path is supplied then all other parameters like primary and processed and tier_list are ignored. The files
	are listed just from within this path
       
        queryableMetaData: Defaulted to None means files with any queryableMetaData will be returned.
         
    raise: DbsApiException.

    examples:
          List all lfns in path /PrimaryDS_01/SIM/procds-01
             api.listLFNs("/PrimaryDS_01/SIM/procds-01")
          List all files in path /PrimaryDS_01/SIM/procds-01, with queryableMetaData = abcd
             api.listFiles(path="/PrimaryDS_01/SIM/procds-01", queryableMetaData="abcd")
          etc etc.
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    #path = get_path(dataset)
    # Invoke Server.


    data = self._server._call ({ 'api' : 'listLFNs', 
                                    'path' : path, 'pattern_meta_data' : queryableMetaData},
                                     'GET')
    ##logging.log(DBSDEBUG, data)

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'file_lfn':
             self.currFile = DbsFile (
                                       LogicalFileName=str(attrs['lfn']),
                                       CreationDate=str(attrs['creation_date']),
                                       CreatedBy=str(attrs['created_by']),
                                       LastModificationDate=str(attrs['last_modification_date']),
                                       LastModifiedBy=str(attrs['last_modified_by']),
                                       )

	  if name == 'file_trigger_tag':
		self.currFile['FileTriggerMap'].append( DbsFileTriggerTag (
				   TriggerTag=str(attrs['trigger_tag']),
				   NumberOfEvents=getLong(attrs['number_of_events']),
				))



        def endElement(self, name):
          if name == 'file_lfn':
             result.append(self.currFile)
  
      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


