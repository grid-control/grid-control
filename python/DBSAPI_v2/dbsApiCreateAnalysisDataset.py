
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

def dbsApiImplCreateAnalysisDataset(self, analysisdataset, defName):
    """
    Creates analysis dataset based on the definition provided

    params:

       analysisdataset: Dict object of type DbsAnalysisDataset, describes this analysis dataset.
                        Following fields are mandatory and should be present in the analysis dataset object : 
                                    name, type, status, path, physics_group_name and annotation
                                          
       def_name:  string: name of Analysis Dataset Definition that define this AnalysisDataset
                                Required and must already exists 
                                (should have been created using createAnalysisDatasetDefinition)  

    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError,
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException

    examples:
        analysis = DbsAnalysisDataset(
                Annotation='This is a test analysis dataset',
                Type='KnowTheType',
                Status='VALID',
                PhysicsGroup='BPositive'
        )
        api.createAnalysisDataset (analysis, "IcreatedThisDefEarlier")

    """  

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    
    if defName in ("", None):
       raise DbsApiException(args="You must provide AnalysisDatasetDefinition (second parameter of this API call)")
       return
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>" 
    xmlinput += "<analysis_dataset "
    xmlinput += " analysisds_def_name='"+ defName +"'"
    xmlinput += " annotation='"+ analysisdataset.get('Annotation', '') +"'"
    xmlinput += " type='"+ analysisdataset.get('Type', '') +"'"
    xmlinput += " status='"+ analysisdataset.get('Status', '') +"'"
    xmlinput += " physics_group_name='"+ analysisdataset.get('PhysicsGroup', '') +"'" 
    xmlinput += " path='"+analysisdataset.get('Path','')+"'/>"
    xmlinput += " />"
    xmlinput += "</dbs>"

    ###logging.log(DBSDEBUG, xmlinput)
    #print xmlinput

    if self.verbose(): 
       print "createAnalysisDataset, xmlinput",xmlinput

    data = self._server._call ({ 'api' : 'createAnalysisDataset',
                         'xmlinput' : xmlinput }, 'POST')
    ###logging.log(DBSDEBUG, data)


    #-----------------------------------------------------------------------------
