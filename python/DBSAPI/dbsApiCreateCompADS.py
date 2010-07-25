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

def dbsApiImplCreateCompADS(self, compADS):

    """
    Creates a new composite analysis dataset commbinition of following parameters:
          Name, Description, List-of-Analysis Datasets (pre-existing in ths DBS)

    params: 
         compADS :  DbsCompositeAnalysisDataset object
	 	ADSList in this object contain tuple of (ADS, Version)
		ADS is mandatory, Version is optional in this tuple, the default version is the LATEST ADS Version

    Note:    
    In case the CompADS already exists an EXCEPTION will be raised by the Server.

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<comp_analysisds comp_analysisds_name='"+ compADS.get('Name', '') +"'"
    xmlinput += " comp_analysisds_desc='"+compADS.get('Description', '')+"'"
    xmlinput += " created_by='"+compADS.get('CreationDate', '')+"'"
    xmlinput += " creation_date='"+compADS.get('CreationDate', '')+"'"
    xmlinput += "/>"

    for anADS in  compADS.get('ADSList', []):
       xmlinput += " <analysis_dataset analysis_dataset_name='"+get_name(anADS)+"'" 
       if not type(anADS) == type(""): 		
	  xmlinput += " version='"+str(anADS.get('Version', ''))+"'"
       xmlinput += " />"

    xmlinput += "</dbs>"

    print xmlinput

    if self.verbose():
       print "dbsApiImplCreateCompADS, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'createCompositeAnalysisDataset',
                         'xmlinput' : xmlinput }, 'POST')


    #Just return the name of compADS if everything went fine.  
    return compADS 



