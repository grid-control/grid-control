import xml.sax.handler
import sys,imp,os
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsApi import DbsApi
from dbsLumiSection import DbsLumiSection
from dbsFile import DbsFile
from dbsFileBlock import DbsFileBlock

def importCode(code,name,add_to_sys_modules=0):
    """
    Returns a newly generated module.
    """
    module = imp.new_module(name)

    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module

    return module

def getDBSMARTQuery(userquery, dbsapi):

	try:
		adshome = os.path.expandvars(dbsapi.adshome())
		if not os.path.exists(adshome):
			try:
				adshome = os.path.expandvars("$HOME/ADSHOME")
			except:
				pass
			if adshome in (None, ""):
				adshome=os.getcwd()
	except Exception, ex:
		msg  = "Failed to set ADSHOME for the DBSMART"
		msg += "\n, please verify your configuration"
		msg += "\n,try setting set ADSHOME=$HOME/ADSHOME in your environment"
		raise DbsException(args=msg, code="900")

       	#use the DEFAULT Mart file
       	martfile = os.path.join(adshome, "Default.dm")
       	if not os.path.exists(martfile):
		raise DbsException(args="Unable to open DBS MART file %s/Default.dm" %adshome, code="901")
	
        martFile=open(martfile, "r")
        mart = importCode(martFile, "mart", 0)
        KnownQueries = mart.KnownQueries
	if KnownQueries.has_key(userquery):
		return KnownQueries[userquery]['USERINPUT']
	else :
	    raise DbsException(args="Query : %s not found in DBS MART %s" %(userquery, martfile), code="902")

def makeQuery(userquery, retriveList=[]):
    qparts=userquery.split("where")
    if len(qparts) < 2:
	raise DbsException(args="Invalid user query, the 'where' clause seems to be missing : %s" %userquery, code="903")
    # At this point I believe that CRAB always need 'retrive_block', 'retrive_run', 'retrive_lumi' so lets always have them in the query
    tmpq="find file, block, run, lumi"
    #if "retrive_parent" in retriveList:
	#tmpq+=", file.parent "
    tmpq+=" where "+qparts[1]
    return tmpq
	
def listPADSFiles(userquery, dbsapi, retriveList=[]):
	
      rowlist=[]
      outlist=[]
      alreadyDone={}
      class fileXMLHandler(xml.sax.handler.ContentHandler):
	      """
	      <row>
	        <file>/store/data/CRUZET3/TestEnables/RAW/v1/000/050/658/D8350900-224C-DD11-BEDF-000423D9939C.root</file>
		<block>/TestEnables/CRUZET3-v1/RAW#5e820269-d31b-4de8-92c6-b9349e7c8f3f</block>
		<run>50658</run>
		<lumi>2</lumi>
		<file.parent></file.parent>
	     </row>
	      """
              def __init__(self):
                       self.inFile = 0
                       self.inBlock = 0
                       self.inRun = 0
                       self.inLumi = 0
		       self.row={}

              def startElement(self, name, attributes):
		    if name == "file":
			self.buffer = ""
			self.inFile = 1
		    if name == "block":
			self.buffer = ""
			self.inBlock = 1
		    if name == "run":
			self.buffer = ""
			self.inRun = 1
		    if name == "lumi":
			self.buffer = ""
			self.inLumi = 1
		    
              def characters(self, data):
                    if self.inFile:
			self.buffer += data
		    if self.inBlock:
			self.buffer += data
		    if self.inRun:
			self.buffer += data
		    if self.inLumi:
			self.buffer += data
    			
              def endElement(self, name):
		    if name == "file":
                        self.inFile = 0
			self.row['file']=self.buffer
		    if name == "block":
                        self.inBlock = 0
			self.row['block']=self.buffer
			currblock=self.buffer
		    if name == "run":
                        self.inRun = 0
			self.row['run']=self.buffer
		    if name == "lumi":
                        self.inLumi = 0
			self.row['lumi']=self.buffer
			
		    if name == "row":
			rowlist.append(self.row)
			self.row={}
		
		    if name == "results":
			for arow in rowlist:
			    if arow['file'] not in alreadyDone:	
				newfile= DbsFile (
				    LogicalFileName=str(arow['file']),
				    FileSize=-1,
				    NumberOfEvents=-1,
				    Block=DbsFileBlock(Name=str(arow['block'])),
				    )
				newlumi=DbsLumiSection(
				    LumiSectionNumber=long(arow['lumi']),
				    RunNumber=long(arow['run'])
				    )
				newfile['LumiList'].append(newlumi)
			       	alreadyDone[arow['file']]=newfile
				outlist.append(newfile)
			    else:
				newlumi=DbsLumiSection(
				    LumiSectionNumber=long(str(arow['lumi'])),
				    RunNumber=long(str(arow['run']))
				    )
				if newlumi not in alreadyDone[arow['file']]['LumiList']:
				    alreadyDone[arow['file']]['LumiList'].append(newlumi)
				    
      query = getDBSMARTQuery(userquery, dbsapi)
      query=makeQuery(query, retriveList) 
      data=dbsapi.executeQuery(query)
#print data
      xml.sax.parseString (data, fileXMLHandler ())

      return outlist

if __name__ == "__main__":

      opts={}
      opts['url']="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
      opts['version']="DBS_2_0_9"
      retriveList=['retrive_block', 'retrive_run', 'retrive_lumi', 'retrive_parent']
      
      api = DbsApi(opts)
      #query="find file where dataset=/TestEnables/CRUZET3-v1/RAW and run=50658 and lumi=1"
      query="PADSTEST01"
      query="NOEXIST_PADSTEST01"
      print listPADSFiles(query, api)

