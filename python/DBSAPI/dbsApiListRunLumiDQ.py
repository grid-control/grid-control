
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsDQFlag import DbsDQFlag
from dbsRunLumiDQ import DbsRunLumiDQ
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *


def dbsApiImplListRunLumiDQ(self, dataset, runLumiDQList=[], timeStamp="", dqVersion=""):

    """
    Lists the Run/LumiSection DQ information based on the input criteria 
	(for now if NO criteria is provided dumps all DQ information, DO NOT DO That !) 

    params: 
         runLumiDQList : Takes LIST of DbsRunLumiDQ objects, each representing a Run OR a LumiSection within a Run
                         with a list of Data Quality Flags (Sub-System and Sub-Sub-System Flags and their values)
                         defined as "DQFlagList". Each object of this list if of type DbsDQFlag. 
                         These values are made part of look up, so if you say
                        
			flag1 = DbsDQFlag (
				        Name = "HCAL+",
        				Value = "GOOD",
        			)
			run_dq_search_criteria = DbsRunLumiDQ (
        			RunNumber=1,
        			#LumiSectionNumber can be part of this serach criteria
        			#LumiSectionNumber=123,
        			DQFlagList = [flag1]
				#Multiple flags
        			#DQFlagList = [flag1, flag2, flag3]
        			)
			dqHierarchyList = api.listRunLumiDQ(  [run_dq_search_criteria]  )

			This will mean, Looking for DQ information for RunNumber = 1, with a Sub-System HCAL+ and Value=GOOD.
        		Anyother type of combinitions can be provided. This is just an example.

			The returned values can be marched through to see whats returned.

			    for aDQ in dqHierarchyList:
			        print "\nRunNumber: ", aDQ['RunNumber']
  				print "LumiSectionNumber: ", aDQ['LumiSectionNumber']
			        for aSubDQ in aDQ['DQFlagList']:
                			print "      ", aSubDQ['Name'], aSubDQ['Value']
                			for aSubSubDQ in aSubDQ['SubSysFlagList']:
                        			print "                ", aSubSubDQ['Name'], aSubSubDQ['Value']

	dqVersion:  User can specify a Data Quality Version, By DEFAULT latest values are listed by this API
	timeStamp: The SnapShot of Data Quality at a certain time, Time Specified in UNIX Time Format (Seconds since epoch)
    
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"

    for aRunLumiDQ in runLumiDQList:
	if aRunLumiDQ not in (None, '', []):
        	xmlinput += "<run run_number='"+str(get_run(aRunLumiDQ.get('RunNumber'))) + "'"
        	xmlinput += " lumi_section_number='"+str(aRunLumiDQ.get('LumiSectionNumber', ''))+"'"
        	xmlinput += " />"

        	for aFlag in aRunLumiDQ.get('DQFlagList'):
                	xmlinput += "<dq_sub_system name='" + aFlag.get('Name') + "'  value='" + aFlag.get('Value') + "'  />"
                	# Sub sub system list
                	for aSubFlag in aFlag.get('SubSysFlagList'):
                        	xmlinput += "<dq_sub_subsys name='" + aSubFlag.get('Name') + "'  value='" + aSubFlag.get('Value') + "'  />"

    	xmlinput += "</dbs>"

    ###logging.log(DBSDEBUG, xmlinput)

    data = self._server._call ({ 'api' : 'listRunLumiDQ',
			'dataset': get_path(dataset),
                         'xmlinput' : xmlinput, 
			'time_stamp':timeStamp, 
			'dq_version':dqVersion }, 'POST')
    ###logging.log(DBSDEBUG, data)

    # Parse the resulting xml output.
    try:
      result = []

      class Handler (xml.sax.handler.ContentHandler):

	def __init__(self):
		self.SubSysFlags = []
		self.SubSubFlags = []

        def startElement(self, name, attrs):
          if name == 'run':

               self.currRun = DbsRunLumiDQ (
						RunNumber=getLong(attrs['run_number']),
						LumiSectionNumber=getLong(attrs['lumi_section_number']),
						DQFlagList=[]
						)
          if name == 'dq_sub_system':
                        aSubFlag = DbsDQFlag(
                                        Name=str(attrs['name']),
                                        Value=str(attrs['value']),
					Parent=str(attrs['parent']),
                                        #CreationDate=str(attrs['creation_date']),
                                        #CreatedBy=str(attrs['created_by']),
                                        #LastModificationDate=str(attrs['last_modification_date']),
                                        #LastModifiedBy=str(attrs['last_modified_by']),
                                        )
			self.SubSysFlags.append(aSubFlag)
          if name == 'dq_sub_subsys':
                        subSubFlag = DbsDQFlag(
                                                Name=str(attrs['name']),
                                                Value=str(attrs['value']),
						Parent=str(attrs['parent']),
                                                #CreationDate=str(attrs['creation_date']),
                                                #CreatedBy=str(attrs['created_by']),
                                                #LastModificationDate=str(attrs['last_modification_date']),
                                                #LastModifiedBy=str(attrs['last_modified_by']),
                                                )
			self.SubSubFlags.append(subSubFlag)
        def endElement(self, name):
            if name == 'run':
		mark_for_removel=[]
		#resolve Sub Sub System Parentage First
		for asssys in self.SubSubFlags:
			added = 0
			
			for apsssys in self.SubSubFlags:
				#Name of a sub sub system is same as parent of sub system
				if apsssys['Name'] == asssys['Parent']:
					# Add it as child node
					apsssys['SubSysFlagList'].append(asssys)
					# And remove it from the master list
					#self.SubSubFlags.remove(asssys)
					mark_for_removel.append(asssys)
					added = 1

		for toDel in mark_for_removel:
			if toDel in self.SubSubFlags:
				self.SubSubFlags.remove(toDel)


                for asssys in self.SubSubFlags:
			for apsys in self.SubSysFlags:
				#Name of a sub system is same as parent of sub sub system
				if apsys['Name'] == asssys['Parent']:
					# Add it as a child node
					apsys['SubSysFlagList'].append(asssys)
					
		

		#
		self.currRun['DQFlagList'] = self.SubSysFlags
		result.append(self.currRun)
                self.SubSysFlags = []
                self.SubSubFlags = []


      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

