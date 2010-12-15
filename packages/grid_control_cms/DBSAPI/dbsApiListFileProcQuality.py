
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

from DBSAPI.dbsFileProcessingQuality import DbsFileProcessingQuality
from DBSAPI.dbsProcessedDataset import DbsProcessedDataset

import inspect

from dbsUtil import *

def dbsApiImplListFileProcQuality(self, lfn, path):
    """
   list file processing quality in DBS

	Param:
		lfn of the file whoes Processing Quality Needs to be investigated

	returns a LIST of dict objects DbsFileProcQuality:
            ParentFile : File for which processing quality is being recorded, LFN of the file that failed to produce a child file
            ChildDataset : The child dataset path, whoes file was suppose to be produced by this file
            FailedEventList : Which events were failed, optional
            ProcessingStatus : Status string representing what went wrong
            FailedEventCount : Number of events that failed, Optional
            Description : Upto 1000 chars of what possibly went wrong
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    data = self._server._call ({ 'api' : 'listFileProcQuality', 
                         'lfn' : lfn, 'path' : path }, 'GET')
    if self.verbose():
       print data

    # Parse the resulting xml output.
    try:

	result = []
	class Handler (xml.sax.handler.ContentHandler):
		def startElement(self, name, attrs):
			if name == 'file_proc_quality':
				failedevts= [ long(x) for x in (str(attrs['failed_event_list']).split()) ] 
				result.append(DbsFileProcessingQuality(
                                             ParentFile=str(attrs['lfn']),
                                             ChildDataset=str(attrs['child_dataset']),
					     FailedEventCount=long(attrs['failed_event_count']),
					     FailedEventList=failedevts,
					     Description=str(attrs['description']),
                                             CreationDate=str(attrs['creation_date']),
                                             CreatedBy=str(attrs['created_by']),
                                             LastModificationDate=str(attrs['last_modification_date']),
                                             LastModifiedBy=str(attrs['last_modified_by']),
                                             )
					)

	xml.sax.parseString (data, Handler ())
	return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


