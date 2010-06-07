from dbsException import DbsException
from dbsApiException import *
from dbsExecHandler import DbsExecHandler

import os, re, string, xml.sax, xml.sax.handler
from xml.sax.saxutils import escape
from xml.sax import SAXParseException

class DbsXmlBaseHandler (xml.sax.handler.ContentHandler):
     def startElement(self, name, attrs):
             if name == 'exception':
                statusCode = attrs['code']
                statusCode_i = int(statusCode)
                exmsg = "DBS Server Raised An Error: %s, %s" \
                                 %(attrs['message'], attrs['detail'])

                if statusCode_i == 1018:
                    raise DbsBadRequest (args=exmsg, code=statusCode)

                elif statusCode_i in [1080, 3003, 2001, 4001]:
                    raise DbsToolError(args=exmsg, code=statusCode)

                if statusCode_i < 2000 and  statusCode_i > 1000 :
                   raise DbsBadRequest (args=exmsg, code=statusCode)

                if statusCode_i < 3000 and  statusCode_i >= 2000 :
                   raise DbsDatabaseError (args=exmsg, code=statusCode)

                if statusCode_i < 4000 and  statusCode_i > 3000 :
                   raise DbsBadXMLData (args=exmsg, code=statusCode)

                else: raise DbsExecutionError (args=exmsg, code=statusCode)

             elif name == 'warning':
                warn  = "\n DBS Raised a warning message"
                warn += "\n Waring Message: " + attrs['message']
                warn += "\n Warning Detail: " + attrs['detail']+"\n"
                ##logging.log(DBSWARNING, warn)

             else: 
		if name =='info':
                  info = "\n DBS Info Message: %s " %attrs['message']
                  info += "\n Detail: %s " %attrs['detail']+"\n"
                  ##logging.log(DBSINFO, info)

	     #else :
	     #	self.callChildMethod(name, attrs)

     #def callChildMethod(self, name, attrs):
     # 		print "Parent callChildMethod called"

#class DbsChildXmlHandler(DbsXmlBaseHandler):
#	def __init__(self):
#		DbsXmlBaseHandler.__init__(self)
#
#        def startElement(self, name, attrs):
#                DbsXmlBaseHandler.startElement(self, name, attrs) 
#                print "Child"
#                print "Element: ", name

        #def callChildMethod(self, name, attrs):
        #        print "Child callChildMethod called"

data="""<?xml version='1.0' standalone='yes'?>
<!-- DBS Version 1 -->
<dbs>
<primary_dataset id='5' annotation='NO ANNOTATION PROVIDED' primary_name='TestPrimary_002_20071016_12h21m00s' start_date='NO_END_DATE PROVIDED' end_date='' creation_date='1192555260' last_modification_date='1192555260' trigger_path_description='' mc_channel_description='' mc_production='' mc_decay_chain='' other_description='' type='test' created_by='anzar@cmssrv17.fnal.gov' last_modified_by='anzar@cmssrv17.fnal.gov'/>
<primary_dataset id='2' annotation='NO ANNOTATION PROVIDED' primary_name='TestPrimary_002_20071016_11h34m46s' start_date='NO_END_DATE PROVIDED' end_date='' creation_date='1192552486' last_modification_date='1192552486' trigger_path_description='' mc_channel_description='' mc_production='' mc_decay_chain='' other_description='' type='test' created_by='anzar@cmssrv17.fnal.gov' last_modified_by='anzar@cmssrv17.fnal.gov'/>
<primary_dataset id='4' annotation='NO ANNOTATION PROVIDED' primary_name='TestPrimary_001_20071016_12h21m00s' start_date='NO_END_DATE PROVIDED' end_date='' creation_date='1192555260' last_modification_date='1192555260' trigger_path_description='' mc_channel_description='' mc_production='' mc_decay_chain='' other_description='' type='test' created_by='anzar@cmssrv17.fnal.gov' last_modified_by='anzar@cmssrv17.fnal.gov'/>
<primary_dataset id='1' annotation='NO ANNOTATION PROVIDED' primary_name='TestPrimary_001_20071016_11h34m46s' start_date='NO_END_DATE PROVIDED' end_date='' creation_date='1192552486' last_modification_date='1192552486' trigger_path_description='' mc_channel_description='' mc_production='' mc_decay_chain='' other_description='' type='test' created_by='anzar@cmssrv17.fnal.gov' last_modified_by='anzar@cmssrv17.fnal.gov'/>
<primary_dataset id='3' annotation='NO ANNOTATION PROVIDED' primary_name='Ta/estHet' start_date='NO_END_DATE PROVIDED' end_date='' creation_date='1192552486' last_modification_date='1192552486' trigger_path_description='' mc_channel_description='' mc_production='' mc_decay_chain='' other_description='' type='test' created_by='anzar@cmssrv17.fnal.gov' last_modified_by='anzar@cmssrv17.fnal.gov'/>
<SUCCESS/>
</dbs>

"""


#xml.sax.parseString (data, DbsChildXmlHandler())
#xml.sax.parseString (data, DbsXmlBaseHandler())

