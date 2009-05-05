
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

def dbsApiImplInsertProcessedDataset(self, dataset):
    """
    Inserts a new dbs processed dataset in an existing primary dataset . It insert all the parents of the processed dataset, 
    insert and assocaite  all the tiers of the processed dataset, associate all the algorithms of the processed dataset and 
    associate all the runs of the processed dataset. Note that inserting a processed dataset doesnot imply that there are now
    datasetPaths in this processed dataset. Only when a block or a file is inserted a datasetPath becomes available in the processed
    dataset.
    The parents, algorithms and runs of the processed dataset should exist before the  processed dataset could be inserted.
    
    param: 
        dataset : The procsssed dataset can be passed as an DbsProcessedDataset object. The following are mandatory and should be present
	          in the dbs procsssed dataset object:
		  processed_datatset_name and primary_datatset_name
		  
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

         primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
	 
         proc = DbsProcessedDataset (
                PrimaryDataset=primary, 
                Name="TestProcessedDS002", 
                PhysicsGroup="BPositive",
                Status="Valid",
                TierList=['SIM', 'RECO'],
                AlgoList=[algo],
         )

         api.insertProcessedDataset (proc)
	 
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    xmlinput  = "<?xml version='1.0' standalone='yes'?>" 
    xmlinput += "<dbs>" 
    xmlinput += "<processed_dataset "
    primary = dataset.get('PrimaryDataset')
    if primary == None: 
       raise DbsApiException(ErrorMsg="Serious Error Primary Dataset not specified")
    xmlinput += " primary_datatset_name='"+primary.get('Name', "")+"'" 
    xmlinput += " processed_datatset_name='"+dataset.get('Name', "")+"'"
    xmlinput += " open_for_writing='"+dataset.get('OpenForWriting', "")+"'"
    xmlinput += " physics_group_name='"+dataset.get('PhysicsGroup', "")+"'"
    xmlinput += " analysis_datatset_parent='"+dataset.get('ADSParent', "")+"'"
    xmlinput += " physics_group_convener='"+dataset.get('PhysicsGroupConverner', "")+"'"
    xmlinput += " acquisition_era='"+dataset.get('AcquisitionEra', "")+"'"
    xmlinput += " global_tag='"+dataset.get('GlobalTag', "")+"'"
    xmlinput += " external_cross_section='"+str(dataset.get('XtCrossSection', ""))+"'"
    xmlinput += " status='"+dataset.get('Status', "")+"'>" 
    
    for tier in dataset.get('TierList',[]):
        xmlinput += "<data_tier name='"+tier+"'/>"

    # Path of the Parent Dataset(s) must be specified, sever expects a "Path"
    for parentPath in dataset.get('ParentList',[]):
        xmlinput += "<parent path='"+get_path(parentPath)+"'/>"

    for algorithm in dataset.get('AlgoList',[]):
        xmlinput += "<algorithm app_version='"+algorithm.get('ApplicationVersion', "")+"'"
        xmlinput += " app_family_name='"+algorithm.get('ApplicationFamily', "")+"'"
        xmlinput += " app_executable_name='"+algorithm.get('ExecutableName', "")+"'"
        pset = algorithm.get('ParameterSetID')
        # Server expects a ps_name, it should expect a ps_hash instead 
        if pset != None:
           xmlinput += " ps_hash='"+pset.get('Hash', "")+"'"
           #xmlinput += " ps_name='"+pset.get('Name', "")+"'"
           #xmlinput += " ps_version='"+pset.get('Version', "")+"'"
           #xmlinput += " ps_type='"+pset.get('Type', "")+"'"
           #xmlinput += " ps_annotation='"+pset.get('Annotation', "")+"'"
           #xmlinput += " ps_content='"+base64.binascii.b2a_base64(pset.get('Content', ""))+"'"
        xmlinput += "/>"

    for run in dataset.get('RunsList',[]):
        runNum = str(get_run(run))
        if runNum not in ("", None):
           xmlinput += "<run run_number='"+runNum+"'/>"

    xmlinput += "</processed_dataset>"
    xmlinput += "</dbs>"

    ###logging.log(DBSDEBUG, xmlinput)
    if self.verbose():
       print "insertProcessedDataset, xmlinput",xmlinput
    
    # Call the method
    data = self._server._call ({ 'api' : 'insertProcessedDataset',
                         'xmlinput' : xmlinput }, 'POST')
    ###logging.log(DBSDEBUG, data)

# ------------------------------------------------------------

