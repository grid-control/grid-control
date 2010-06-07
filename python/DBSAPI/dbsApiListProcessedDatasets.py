
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsProcessedDataset import DbsProcessedDataset
from dbsPrimaryDataset import DbsPrimaryDataset
from dbsAlgorithm import DbsAlgorithm
from dbsQueryableParameterSet import DbsQueryableParameterSet
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplListProcessedDatasets(self, patternPrim="*", patternDT="*", patternProc="*",
                                  patternVer="*", patternFam="*", patternExe="*", patternPS="*"):
    """
    Retrieve list of processed datasets matching shell glob patterns for dataset and application
    User can provide a comprehensive list of parameters for which he/she want to find dataset(s).
    The search criteria can be based of a pattern for Primary dataset, Processed Dataset, Data Tier,
    Application version, Application Family, Application executyable name and ParameterSet name. Note that
    this API returns the list of processed dataset which are diffrent from datasetPath. A processed dataset
    can conatin multiple datasetPath only if files exists in that dataset

    Returns a list of DbsProcessedDataset objects.  If the pattern(s) are

    params:
        patternPrim: glob pattern for Primary Dataset, MyPrimaryDataset001, MyPrimaryDataSets*, *
        patternDT: glob pattern for Data Tier, SIM, RECO, SIM-DIGI, SIM*, *
        patternProc: glob pattern for Processed Dataset, MyProcDataset001, MyProcDataset*, *
        patternVer: glob pattern for Application Version, v00_00_01, *
        patternFam: glob pattern for Application Family, GEN, *
        patternExe: glob pattern for Application Executable Name, CMSSW, writeDigi, *
        patternPS: glob pattern for PSet Name, whatever, *
 
    returns: list of DbsProcessedDataset objects  

    examples:
           Say I want to list all datasets that have a Primary Dataset of type MyPrimaryDatasets*, with SIM data tier,
           and application version v00_00_03, produced by Application CMSSW, I can make my call as,

           api.listProcessedDatasets(patternPrim='MyPrimaryDatasets*', patternDT='SIM', patternVer='v00_00_03', patternFam='CMSSW') 
   
    raise: DbsApiException, DbsBadResponse

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    api_version=self.getApiVersion()
    if api_version < "DBS_2_0_6":
    	# Lets get all tiers no matter what, otherwise Server puts unnecessary checks on the DataTier
	#This is how it wasdone in older versions and we are better off having this in place for older versions	
    	patternDT='*'


    # Invoke Server.    
    data = self._server._call ({ 'api' : 'listProcessedDatasets', 
		    'primary_datatset_name_pattern' : patternPrim, 
		    'data_tier_name_pattern' : patternDT, 
		    'processed_datatset_name_pattern' : patternProc, 
		    'app_version' : patternVer, 
		    'app_family_name' : patternFam, 
		    'app_executable_name' : patternExe, 
		    'ps_hash' : patternPS }, 
		    'GET')


    ##logging.log(DBSDEBUG, data)  
    ##print data
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
        

	def startElement(self, name, attrs):
	  if name == 'processed_dataset':
	    self.procName = str(attrs['processed_datatset_name'])	  
	    self.primName = str(attrs['primary_datatset_name'])	  
            ds_status='VALID'
	    ext_cross_sec=0.0
	    comment=''
	    if attrs.has_key('Description'):
		comment=str(attrs['Description'])
			    
            if attrs.has_key('status'):
		ds_status=str(attrs['status'])
	    try:
	    	if attrs.has_key('external_cross_section'):
			ext_cross_sec=str(attrs['external_cross_section'])	    
	    except:
		ext_cross_sec=0.0
            self.currDataset = DbsProcessedDataset ( 
                                                Name=self.procName,     
						PhysicsGroup=str(attrs['physics_group_name']),
						PhysicsGroupConverner=str(attrs['physics_group_convener']),
						Status=ds_status,
						AcquisitionEra=str(attrs.get('acquisition_era', '')),
						GlobalTag=str(attrs.get('global_tag', '')),
                                                #openForWriting=str(attrs['open_for_writing']), 
                                                PrimaryDataset=DbsPrimaryDataset(Name=self.primName),
						XtCrossSection=float(ext_cross_sec),
                                                CreationDate=str(attrs['creation_date']),
                                                CreatedBy=str(attrs['created_by']),
                                                LastModificationDate=str(attrs['last_modification_date']),
                                                LastModifiedBy=str(attrs['last_modified_by']),
						Description=comment
                                                )
          if name == 'data_tier':
		"""
		if api_version < "DBS_2_0_6":
			# This is how it was done in older versions!
			self.currDataset['TierList'].append(str(attrs['name']))
		else: 
            		self.currDataset['TierList']=str(attrs['name']).split('-')
		"""
		self.currDataset['TierList'].append(str(attrs['name']))

            #self.currDataset['PathList'].append("/" + self.primName + "/" + str(attrs['name']) + "/" + self.procName)

          if name == 'path':
	    self.currDataset['PathList'].append(str(attrs['dataset_path']))

          if name == 'algorithm':
            self.currDataset['AlgoList'].append(DbsAlgorithm( ExecutableName=str(attrs['app_executable_name']),
                                                         ApplicationVersion=str(attrs['app_version']),
                                                         ApplicationFamily=str(attrs['app_family_name']),
							 ParameterSetID=DbsQueryableParameterSet(
								 Hash=str(attrs['ps_hash']))
							 ) )

        def endElement(self, name):
          if name == 'processed_dataset':
             result.append(self.currDataset)

      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

