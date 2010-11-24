# Revision: $
# Id: $
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplInsertAlgorithm(self, algorithm):

    """
    Inserts a new dbs algorithm/application . An algorithm is uniquely identified by appverion, appfamily , 
    appexecutable and parametersetname collectively. If the algorithm already exist then it just displays a warnning.
    
    param: 
        algorithm : The dbs algorithm passed in as an DbsAlgorithm object. The following are mandatory and should be present
	          in the dbs algorithm object:
		  app_version, app_family_name, app_executable_name and ps_name
		  
    raise: DbsApiException, DbsBadRequest, DbsBadData, DbsNoObject, DbsExecutionError, DbsConnectionError, 
           DbsToolError, DbsDatabaseError, DbsBadXMLData, InvalidDatasetPathName, DbsException	
	   
    examples:
         algo = DbsAlgorithm (
                ExecutableName="TestExe01",
                ApplicationVersion= "TestVersion01",
                ApplicationFamily="AppFamily01",
                ParameterSetID=DbsQueryableParameterSet(
                     Hash="001234565798685",
                     Name="MyFirstParam01",
                     Version="V001",
                     Type="test",
                     Annotation="This is test",
                     Content="int a= {}, b={c=1, d=33}, f={}, x, y, x"
                )
         )

         api.insertAlgorithm (algo)
	
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())


    # Prepare XML description of the input

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<algorithm app_version='"+algorithm.get('ApplicationVersion', "")+"'"
    xmlinput += " app_family_name='"+algorithm.get('ApplicationFamily', "")+"'"
    xmlinput += " app_executable_name='"+algorithm.get('ExecutableName', "")+"'"
    pset = algorithm.get('ParameterSetID')

    if pset != None: 
       xmlinput += " ps_hash='"+pset.get('Hash', "")+"'"
       xmlinput += " ps_name='"+pset.get('Name', "")+"'"
       xmlinput += " ps_version='"+pset.get('Version', "")+"'"
       xmlinput += " ps_type='"+pset.get('Type', "")+"'"
       #xmlinput += " ps_annotation='"+pset.get('Annotation', "")+"'"
       xmlinput += " ps_annotation='"+base64.binascii.b2a_base64(pset.get('Annotation', ""))+"'"
       # Converting Content to base64 encoded string, otherwise it can leave the xml invalid
       xmlinput += " ps_content='"+base64.binascii.b2a_base64(pset.get('Content', ""))+"'"
    xmlinput += "/>"
    xmlinput += "</dbs>"

    
    if self.verbose():
       print "insertAlgorithm, xmlinput",xmlinput
    data = self._server._call ({ 'api' : 'insertAlgorithm',
                         'xmlinput' : xmlinput }, 'POST')

  # ------------------------------------------------------------

