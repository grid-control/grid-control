#!/usr/bin/env python
#
# API Unit tests for the DBS JavaServer.

import sys
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsOptions import DbsOptionParser
#import pdb
import datetime
import time


optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)
      
class DbsUnitTestApi:
	def __init__(self, obj, fileObj):
		self.lapiObj = obj
		self.f = fileObj
		self.index = 0
                self.verbose=0
        def setVerboseLevel(self,level):
            self.verbose=level
        def printTestStatus(self,info,status,iMsg,timeDiff,exp=None):
            msg = "\n\nType : %s"%str(self.lapiObj.im_func.func_name)
            msg+= "\nDone "+info
            msg+= "\nComment      : %s"%iMsg
            msg+= "\nException    : %s"%exp
            msg+= "\nTest ended   : [%6s]"%status
            msg+= "\nTest LAPSED  : [%06s] seconds" % (str(timeDiff))
            msg+= "\nTest Number : "+str(self.index)
            self.f.write(msg)
            self.f.flush() 
            print msg 

	def run(self, *listArgs, **dictArgs):
		try:
			self.index +=  1
			info =  str(self.lapiObj.im_func.func_name) + str(listArgs)
			#info =  str(self.lapiObj.im_func.func_name) + str(listArgs[1:])
			#print info
			excep = dictArgs['excep']
                        startTime = time.mktime(datetime.datetime.now().timetuple()) 
                        #log_msg ="\nTest Starting: "+ str(self.lapiObj.im_func.func_name)+" test number " \
                        #      + str(self.index)+  \
                        #      " started at: " + str(datetime.datetime.fromtimestamp(startTime))
                        #print log_msg
                        #info+=log_msg
                        #self.f.write(log_msg)
			self.lapiObj(*listArgs)
                        endTime = time.mktime(datetime.datetime.now().timetuple())
                        timeDiff = endTime - startTime
                         
			#self.lapiObj(*listArgs[1:])
			#for data in apiObj(*listArgs):
				#print "  %s" % data
			if excep:
                                self.printTestStatus(info,"FAILED","AN EXCEPTION WAS EXPECTED BUT NONE WAS RAISED", timeDiff)
                                print "Test FAILED not STOPING EXECUTION."
                                sys.exit(1)  
			else:
                                self.printTestStatus(info,"PASSED","AN EXCEPTION WAS NOT EXPECTED AND NONE WAS RAISED", timeDiff)
		except:
                        #self.index += 1
                        endTime = time.mktime(datetime.datetime.now().timetuple())
                        timeDiff = endTime - startTime
			exception =  str(sys.exc_info()[0]) + " : " +  str(sys.exc_info()[1])
                        print exception
			if excep:
                                self.printTestStatus(info,"PASSED","AN EXCEPTION WAS EXPECTED AND RAISED. THE EXCEPTION IS",timeDiff,exception)
			else:
                                self.printTestStatus(info,"FAILED","AN EXCEPTION WAS NOT EXPECTED BUT RAISED. THE EXCEPTION IS",timeDiff,exception)
                                print "Test FAILED not STOPING EXECUTION."
                                sys.exit(1)  

	def getExistingPDPath(self):
              try:
		for proc in api.listProcessedDatasets("*"):
			return "/" + str(proc['PrimaryDataset']['Name']) + "/" + str(proc['tierList'][0]) + "/" + str(proc['Name'])
              except:
                        exception =  str(sys.exc_info()[0]) + " : " +  str(sys.exc_info()[1])
                        self.f.write("\n " + str(exception))
                        
	def getExistingBlock(self):
              try:
		for block in api.listBlocks(self.getExistingPDPath()):
			return block['Name']
              except:
                        exception =  str(sys.exc_info()[0]) + " : " +  str(sys.exc_info()[1])
                        self.f.write("\n " + str(exception))

	"""
	def getExistingRunNumber(self):
		for proc in api.listProcessedDatasets("*"):
			path =  "/" + str(proc['PrimaryDataset']['Name']) + "/" + str(proc['tierList'][0]) + "/" + str(proc['Name'])
			for run in api.listRuns(path):
				print run
				#print run['run_number']
	"""
#a = DbsUnitTestApi(None, None)
#print a.getExistingBlock()
#print a.getExistingPDPath()
#print a.getExistingRunNumber()
