
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

def dbsApiImplInsertFileProcQuality(self, fileprocquality):
    """
    Records file processing quality in DBS
            ParentFile : File for which processing quality is being recorded, LFN of the file that failed to produce a child file
            ChildDataset : The child dataset path, whoes file was suppose to be produced by this file
            FailedEventList : Which events were failed, optional
            ProcessingStatus : Status string representing what went wrong
            FailedEventCount : Number of events that failed, Optional
            Description : Upto 1000 chars of what possibly went wrong
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<file_proc_quality lfn='"+ file_name(fileprocquality.get('ParentFile', '')) +"'"
    xmlinput += " child_dataset='"+ get_path(fileprocquality.get('ChildDataset', '')) +"'"
    xmlinput += " failed_event_count='"+ str(fileprocquality.get('FailedEventCount', '')) +"'"
    xmlinput += " description='"+ fileprocquality.get('Description', '') +"'"
    xmlinput += " processing_status='"+ fileprocquality.get('ProcessingStatus', '') +"'"
    failed_evt_list=""
    for aEvent in fileprocquality.get('FailedEventList', []):
	failed_evt_list += " "+str(aEvent)	

    xmlinput += " failed_event_list='"+ failed_evt_list  +"'"
    xmlinput += " />"
    xmlinput += "</dbs>"

    if self.verbose():
       print "insertFileProcQuality, xmlinput", xmlinput

    data = self._server._call ({ 'api' : 'insertFileProcQuality', 
                         'xmlinput' : xmlinput }, 'POST')
    ###logging.log(DBSDEBUG, data)


  # ------------------------------------------------------------

