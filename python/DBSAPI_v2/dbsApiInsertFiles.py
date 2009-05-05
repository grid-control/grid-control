
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsProcessedDataset import DbsProcessedDataset

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplInsertFiles(self, dataset=None, files=[], block=None):
    """ 
    Inserts a new dbs file in an existing block in a given processed dataset. It also insertes lumi sections
    assocated with the file. It insert all the parents of the file, assocaite  all the tiers of the file and 
    associate all the algorithms of the file. The parents, tiers and algorithms of the file should exist before 
    the file could be inserted.
    
    param: 
        dataset : The procsssed dataset can be passed as an DbsProcessedDataset object or just as a dataset 
	          path in the format of /prim/proc/datatier
                  
        block : The dbs file block passed in as an DbsFileBlock obejct. This object can be passed in also, 
                as a string containing the block name, instead of DbsFileBlock object. The following fields 
                are mandatory and should be present in the dbs file block object: block_name
                          
	files : The list of dbs files in the format of DbsFile obejct. The following are mandatory and should be present 
		in the dbs file object:	lfn
		  
	Note:
		IF block (boock name or DbsFileBlock object) is provide it gets precedense over dataset (or path)
                So files will be inserted into provided block and DBS will only raise a warning that dataset/path
                is being ignored. In that case just set dataset="" anbd DBS will ignore it.
		
	  
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

         lumi1 = DbsLumiSection (
                 LumiSectionNumber=1222,
                 StartEventNumber=100,
                 EndEventNumber=200,
                 LumiStartTime=1234,
                 LumiEndTime=1234,
                 RunNumber=1,
         )

         lumi2 = DbsLumiSection (
                 LumiSectionNumber=1333,
                 StartEventNumber=100,
                 EndEventNumber=200,
                 LumiStartTime=1232,
                 LumiEndTime=1234,
                 RunNumber=1,
         )

         myfile1= DbsFile (
                Checksum= '999',
                LogicalFileName= 'aaa1122-0909-9767-8764aaa',
                NumberOfEvents= 10000,
                FileSize= 12340,
                Status= 'VALID',
        	ValidationStatus = 'VALID',
                FileType= 'EVD',
                Dataset= proc,
                LumiList= [lumi1, lumi2],
                TierList= ['SIM', 'RECO'],
         )


        myfile2= DbsFile (
                 Checksum= '000',
                 LogicalFileName= 'aaaa2233-0909-9767-8764aaa',
                 NumberOfEvents= 10000,
                 FileSize= 12340,
                 Status= 'VALID',
         	 ValidationStatus = 'VALID',
                 FileType= 'EVD',
                 Dataset= proc,
                 TierList= ['SIM', 'RECO'],
                 AlgoList = [algo],
                 ParentList = ['aaa1122-0909-9767-8764aaa']  
         )
                            
         block = DbsFileBlock (
                 Name="/this/hahah/SIM#12345"
         )

         api.insertFiles (proc, [myfile1, myfile2], block)

         api.insertFiles ("/test_primary_anzar_001/TestProcessedDS002/SIM",[myfile1, myfile2], "/this/hahah/SIM#12345")
	 
         api.insertFiles (proc, [myfile1, myfile2], "/this/hahah/SIM#12345")
   
         api.insertFiles ("/test_primary_anzar_001/TestProcessedDS002/SIM", [myfile1, myfile2],  block)

    """
    # Prepare XML description of the input

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"

    # Anzar Afaq
    # This comment-out is part of the HACK to allow inconsistent Block names
    #
    #if (isinstance(dataset, DbsProcessedDataset)):
    #    xmlinput += " <processed_datatset path=''>"
    #else :
    #   xmlinput += " <processed_datatset path='"+get_path(dataset)+"'>"
    xmlinput += " <processed_datatset path='"+get_path(dataset)+"'>"

    if block not in (None, ""):
       xmlinput += "<block block_name='"+ block.get("Name", "") +"'>"
       #xmlinput += " open_for_writing='"+block.get('OpenForWriting', "")+"'"
       #xmlinput += " path='"+path+"'>"
    if type(block) != type("str") and block not in (None, "") :
       if (block['StorageElementList'] not in ( [], None)) :
         for aSe in block['StorageElementList']:
            xmlinput += " <storage_element storage_element_name='"+get_name(aSe)+"'/>"
    if block not in (None, ""):	
    	xmlinput += "</block>"

    for file in files:
       xmlinput += " <file lfn='"+file.get('LogicalFileName', '')+"'"
       xmlinput += " checksum='"+file.get('Checksum', '')+"'"
       xmlinput += " adler32='"+file.get('Adler32', '')+"'"
       xmlinput += " md5='"+file.get('Md5', '')+"'"
       xmlinput += " number_of_events='"+str(file.get('NumberOfEvents', ''))+"'"
       xmlinput += " size='"+str(file.get('FileSize', ''))+"'"
       xmlinput += " file_status='"+file.get('Status', '')+"'" 
       xmlinput += " type= '"+file.get('FileType', '')+"'"
       xmlinput += " validation_status='"+file.get('ValidationStatus', '')+"'"
       xmlinput += " queryable_meta_data='"+file.get('QueryableMetadata', '')+"'"
       xmlinput += " file_assoc='"+file_name(file.get('FileAssoc', ''))+"'"
       xmlinput += " branch_hash='"+file.get('BranchHash', '')+"'"
       xmlinput += " auto_cross_section='"+str(file.get('AutoCrossSection', ''))+"'"
       xmlinput += " >" 

       for lumi in file.get('LumiList', []):
            xmlinput += "<file_lumi_section lumi_section_number='"+str(lumi.get('LumiSectionNumber', ''))+"'"
            xmlinput += " run_number='"+str(lumi.get('RunNumber', ''))+"'"
            #No need to provide other LumiSection info, User will never provide lumisection TO be added at this stage
            #All Lumi Scetions will already be in DBS (SV#28264). Anzar Afaq (08/20/2007)
            xmlinput += " start_event_number='"+str(lumi.get('StartEventNumber', ''))+"'" 
            xmlinput += " end_event_number='"+str(lumi.get('EndEventNumber', ''))+"'"
            xmlinput += " lumi_start_time='"+str(lumi.get('LumiStartTime', ''))+"'" 
            xmlinput += " lumi_end_time='"+str(lumi.get('LumiEndTime', ''))+"'"
            xmlinput += " />"

       for run in file.get('RunsList', []):
            runNum = str(get_run(run))
            if runNum not in ("", None):
               xmlinput += "<file_lumi_section "
               xmlinput += " run_number='"+runNum+"'"
               xmlinput += " />"

       for tier in file.get('TierList',[]):
            xmlinput += "<file_data_tier name='"+get_name(tier)+"'/>"

       #for branch in file.get('BranchList',[]):
       #     xmlinput += "<file_branch name='"+branch+"'/>"


       for trig in file.get('FileTriggerMap',[]):
	    xmlinput += "<file_trigger_tag trigger_tag='"+trig.get('TriggerTag')+"'"
	    xmlinput +=	" number_of_events='"+str(trig.get('NumberOfEvents'))+"'"
	    xmlinput +=	" />"

	    
   
       # LFNs of the Parent Files(s) may be specified, sever expects LFNs
       for parent in file.get('ParentList',[]):
            xmlinput += "<file_parent lfn='"+file_name(parent)+"' />"

       # LFNs of the Children Files(s) may be specified, sever expects LFNs
       for child in file.get('ChildList',[]):
            xmlinput += "<file_child lfn='"+file_name(child)+"' />"

       for algorithm in file.get('AlgoList',[]):
           xmlinput += "<file_algorithm app_version='"+algorithm.get('ApplicationVersion', "")+"'"
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
       xmlinput += "</file>"
       xmlinput += "\n"
      
    xmlinput += "</processed_datatset>"
    xmlinput += "</dbs>"

    ###logging.log(DBSDEBUG, xmlinput)
    #print xmlinput
    if self.verbose():
       print "insertFiles, xmlinput",xmlinput

    if (isinstance(dataset, DbsProcessedDataset)) :

	# Call the method
	data = self._server._call ({ 'api' : 'insertFiles',
			 'primary_dataset' : dataset['PrimaryDataset']['Name'],
			 'processed_dataset' : dataset['Name'],
                         'xmlinput' : xmlinput }, 'POST')
    else :
        # Call the method
        data = self._server._call ({ 'api' : 'insertFiles',
                         'xmlinput' : xmlinput }, 'POST')
        ###logging.log(DBSDEBUG, data)

  # ------------------------------------------------------------

def dbsApiImplDeleteFileParent(self, file, parentFile):
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    #logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    data = self._server._call ({ 'api' : 'deleteFileParent',
			  'lfn' : file_name(file),
			  'parent_lfn' : file_name(parentFile)
			  }, 'POST')

def dbsApiImplInsertFileParent(self, file, parentFile):
    funcInfo = inspect.getframeinfo(inspect.currentframe())
    #logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += " <file lfn='" + file_name(file) + "'/>" 
    xmlinput += "</dbs>"


    data = self._server._call ({ 'api' : 'insertParentInFile',
			  'xmlinput' : xmlinput,
			  'parent_lfn' : file_name(parentFile)
			  }, 'POST')

