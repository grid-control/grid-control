
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplInsertPrimaryDataset(self, dataset):
    """
    Inserts a new primary dataset in the DBS databse. 
    
    param: 
	dataset : The primary dataset passed in as DbsPrimaryDataset object.  The following are mandatory and should be present
	          in the dbs primary dataset object:  primary_name	  
		  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
         api.insertPrimaryDataset (primary)

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<primary_dataset annotation='"+dataset.get('Annotation', '')+"' "
    xmlinput += " primary_name='"+dataset.get('Name', '')+"' "
    xmlinput += " start_date='"+dataset.get('StartDate', '')+"' end_date='"+dataset.get('EndDate', '')+"'"
    xmlinput += " description='"+dataset.get('Description', '')+"'"
    #xmlinput += " trigger_path_description='"+dataset.get('TriggerPathDesc', '')+"'"
    #xmlinput += " mc_channel_description='"+dataset.get('McChannelDesc', '')+"' mc_production='"+dataset.get('McProdDesc', '')+"'"
    #xmlinput += " mc_decay_chain='"+dataset.get('McDecayChain', '')+"' other_description='"+dataset.get('OtherDesc', '')
    xmlinput += " type='"+dataset.get('Type', '')+"'>"
    xmlinput += " </primary_dataset>"
    xmlinput += "</dbs>"


    if self.verbose():
       print "insertPrimaryDataset, xmlinput",xmlinput
    data = self._server._call ({ 'api' : 'insertPrimaryDataset',
                         'xmlinput' : xmlinput }, 'POST')

  # ------------------------------------------------------------
