#!/usr/bin/env python
#
# Unit tests for the DBS CGI implementation.
import sys
import os
#from DBSAPI.dbsApi import DbsApi
from dbsException import *
from dbsApiException import *
from dbsOptions import DbsOptionParser
from dbsLogger import *

class DbsMigrateApi:
	
	apiSrc = None
	apiDst = None
	force = False
	pBranches = False
	allPaths = []
	#def __init__(self, srcURL, dstURL, force = False):
	def __init__(self, apiSrc, apiDst, force = False, pBranches = False):
		#self.apiSrc = self.makeAPI(srcURL)
		#self.apiDst = self.makeAPI(dstURL)
		self.apiSrc = apiSrc
		self.apiDst = apiDst
		self.force = force
		self.pBranches = pBranches
		
	def getAPISrc(self):
		return self.apiSrc
	
	def getAPIDst(self):
		return self.apiDst

	def makeAPI(self, url):
		args = {}
		if url.startswith('http'):
			args['url'] = url
			args['mode'] = 'POST'
		return DbsApi(args)

	"""
	def getParentPathListOLD(self, api, path):
		print 'getting parents for %s' %path
		self.allPaths.append(path)
		pathList = []
		#print "listing parents for %s" %path
		datasets = api.listDatasetParents(path)
		#print "parents are %s " %datasets
		if datasets not in [[], None] :
			for dataset in datasets:
				for proc in api.listProcessedDatasets(patternPrim = dataset['PrimaryDataset']['Name'],  patternProc = dataset['Name']):
					for aPath in proc['PathList']:
						if(aPath not in self.allPaths):
							pathList.append(aPath)
		print "parents %s " %pathList				
		return pathList
	"""
	def getParentPathList(self, api, path):
		print 'getting parents for %s' %path
		self.allPaths.append(path)
		pathList = []
		#print "listing parents for %s" %path

		datasets = api.listDatasetParents(path)
		#print "parents are %s " %datasets
		if datasets not in [[], None] :
			for dataset in datasets:
				for aPath in dataset['PathList']:
					if aPath.endswith("TIER_DOES_NOT_MATTER"):
						for proc in api.listProcessedDatasets(patternPrim = \
								dataset['PrimaryDataset']['Name'],  patternProc = dataset['Name']):
                                        		for aPath in proc['PathList']:
                                                		if(aPath not in self.allPaths):
                                                        		pathList.append(aPath)
					else: 
						if(aPath not in self.allPaths):
							pathList.append(aPath)
							self.allPaths.append(aPath)
		print "parents %s " %pathList				
		return pathList

	def doesPathExist(self, api, path):
		if (self.force):
			##logging.log(DBSWARNING, "The dataset path " + path + " will not be checked for existance in the destination DBS.\n If you want to enforce the checking of path existance before transefrring, use force=False option in this API")
			return False
		return self.doesPathExistNoForce(api, path)
		"""
		tokens = path.split('/')
		#print "checking path existinace %s " %path
		datasets = api.listProcessedDatasets(patternPrim = tokens[1], patternProc = tokens[2])
		if datasets in [[], None] :
			#print "NO path found for tokens prim %s " %tokens[1]
			#print "proc %s " %tokens[2]
			return False;
		else:
			#print "Path Found "
			##logging.log(DBSWARNING, "The dataset path " + path + " already exists in the destination DBS and will NOT be transferred. If you want to  remove the existance check before transferring, use force=True option in this API")
			return True;
		"""	
	
	def doesPathExistNoForce(self, api, path):
		tokens = path.split('/')
		datasets = api.listProcessedDatasets(patternPrim = tokens[1], patternProc = tokens[2], patternDT = tokens[3])
		if datasets in [[], None] :
			return False;
		else:
			return True;
	
	def isBlockIn(self, blockToCheck, blockList):
		#print 'checking block %s ' %blockToCheck
		#print "in blockList %s " %blockList
		for aBlock in blockList:
			if aBlock['Name'] == blockToCheck['Name']:
				 if aBlock['NumberOfEvents'] != blockToCheck['NumberOfEvents']: return False
				 if aBlock['BlockSize'] != blockToCheck['BlockSize']: return False
				 #if aBlock['LastModificationDate'] != blockToCheck['LastModificationDate']: return False
				 return True
		return False
		
	def migratePath(self, path):
		
		#Get the parents of the path
		self.checkDatasetStatus(path)
		datasets = self.getParentPathList(self.apiSrc, path)
		if datasets not in [[], None] :
			for dataset in datasets:
				#Does the parent exist in dst DBS
				if not self.doesPathExist(self.apiDst, dataset):
					#Transfer all the blocks in this parent dataset
					self.migratePath(dataset)
				else :
					blockInSrc = self.apiSrc.listBlocks(dataset)
					blockInDst = []
					try :
						blockInDst = self.apiDst.listBlocks(dataset)
					except DbsBadRequest, ex:
						if int(ex.getErrorCode()) != 1008: raise ex
					for aBlockInSrc in blockInSrc:
						if not self.isBlockIn(aBlockInSrc, blockInDst):
							self.migratePath(dataset)
							break
							
					
		#print "checking path %s in dest " %path
		if not self.doesPathExist(self.apiDst, path):
			#print "path does not exists "
			#Transfer all the blocks in this child path
			#print "listing blocks in path "
			self.migratePathBasic(path)
			#for block in self.apiSrc.listBlocks(path):
			#	self.migrateBlockBasic(path, block['Name'])
	


	def migratePathBasic(self, path):
		blockInSrc = self.apiSrc.listBlocks(path)
		#print 'blockInSrc %s' %blockInSrc
		blockInDst = []
		try:
			if self.doesPathExistNoForce(self.apiDst, path):
				blockInDst = self.apiDst.listBlocks(path)
			#print 'blockIndST %s' %blockInDst
		except DbsBadRequest, ex:
			#print int(ex.getErrorCode())
			if int(ex.getErrorCode()) != 1008: raise ex
		#except Exception, ge:
		#	print 'excpeiton was rasied'
		for aBlockInSrc in blockInSrc:
			if not self.isBlockIn(aBlockInSrc, blockInDst):
				self.migrateBlockBasic(path, aBlockInSrc['Name'])
			else :
				print "-----------------------------------------------------------------------------------"
				print "Ignoring path %s " %path
				print "            block %s " %aBlockInSrc['Name']
				print "because it already exist in the destination DBS and has NOT changed in source DBS"
				print "-----------------------------------------------------------------------------------\n"

	def migratePathROBasic(self, path):
		for block in self.apiSrc.listBlocks(path):
			self.migrateBlockROBasic(path, block['Name'])

	
	
	def migrateBlockBasic(self, path, blockName):
		try:
			print "-----------------------------------------------------------------------------------"
			print "Transferring path %s " %path
			print "            block %s " %blockName
			print "-----------------------------------------------------------------------------------\n"
			if( not self.pBranches):
				self.apiDst.insertDatasetContents(self.apiSrc.listDatasetContents(path,  blockName))
			else:
				fileName = blockName.replace('/', '_').replace('#', '_') + ".xml"
				f = open(fileName, "w");
				f.write(self.apiSrc.listDatasetContents(path,  blockName))
				f.close()
				#print self.pruneBranchesFromFile(fileName)
				self.apiDst.insertDatasetContents(self.pruneBranchesFromFile(fileName))

		except DbsBadRequest, ex:
			#print "----------------------------------------EXCEPTION ---------------------------------------"
			#print ex
			#print "----------------------------------------EXCEPTION CODE---------------------------------------"
			#print  int(ex.getErrorCode())
			#print "----------------------------------------DONE---------------------------------------"
			# If not block excep then raise it again
			if int(ex.getErrorCode()) != 1024:
				raise ex
		
	def migrateBlockROBasic(self, path, blockName):
		try:
			print "-----------------------------------------------------------------------------------"
			print "Transferring path %s " %path
			print "            block %s " %blockName
			print "-----------------------------------------------------------------------------------\n"
			if(not self.pBranches):
				self.apiDst.insertDatasetContents(self.apiSrc.listDatasetContents(path,  blockName), True)
			else:
				fileName = blockName.replace('/', '_').replace('#', '_') + ".xml"
				f = open(fileName, "w");
				f.write(self.apiSrc.listDatasetContents(path,  blockName))
				f.close()
				#print self.pruneBranchesFromFile(fileName)
				self.apiDst.insertDatasetContents(self.pruneBranchesFromFile(fileName), True)
			
		except DbsBadRequest, ex:
			print "----------------------------------------EXCEPTION ---------------------------------------"
			print ex
			if int(ex.getErrorCode()) != 1024:
				raise ex
	
	def migrateBlock(self, path, blockName):
		#Get the parents of the path
		self.checkDatasetStatus(path)
		datasets = self.getParentPathList(self.apiSrc, path)
		if datasets not in [[], None] :
			for dataset in datasets:
				#Dont Check for existance of path if Block is given
				#if not self.doesPathExist(self.apiDst, dataset):
				#print "calling self.migratePath"
				self.migratePath( dataset)
				
		if self.doesPathExistNoForce(self.apiDst, path):
			 found = False
			 blockInDst = self.apiDst.listBlocks(path)
			 for aBlockInDst in blockInDst:
				 if aBlockInDst['Name'] == blockName:
					 found = True
					 #print aBlockInDst
					 if aBlockInDst['OpenForWriting'] == '0':
						 print 'Opening the Block in the destination DBS for writing'
						 self.apiDst.openBlock(aBlockInDst)
						 self.migrateBlockBasic(path, blockName)
						 print 'Closing the Block in the destination DBS'
						 self.apiDst.closeBlock(aBlockInDst)
					 else:
						 self.migrateBlockBasic(path, blockName)
					 break
			 if  not found:
				self.migrateBlockBasic(path, blockName)
		else:
			 self.migrateBlockBasic(path, blockName)
					 
			
		#self.migrateBlockBasic(path, blockName)
		
		
	def getDatasetStatus(self, path):
		tokens = path.split('/')
		datasets = self.apiSrc.listProcessedDatasets(patternPrim = tokens[1], patternProc = tokens[2], patternDT = tokens[3])
		for aDataset in datasets:
			return aDataset['Status']
		
	
	def isDatasetStatusRO(self, path):
		if self.getDatasetStatus(path) == "RO":
			return True
		else:
			return False

	def getInstanceName(self, api):
		return api.getServerInfo()['InstanceName']
		

	
	def migratePathRO(self, path):
		srcInstanceName = self.getInstanceName(self.apiSrc)
		#srcInstanceName = "LOCAL"
		dstInstanceName = self.getInstanceName(self.apiDst)
		#dstInstanceName = "GLOBAL"
		self.checkDatasetStatus(path)
		self.checkInstances(srcInstanceName, dstInstanceName)
		if not self.doesPathExist(self.apiDst, path):
			if dstInstanceName == "GLOBAL" and srcInstanceName == "LOCAL" :
				#One level Migration
				self.migratePathBasic(path)
			else:
				if dstInstanceName == "LOCAL" and srcInstanceName == "GLOBAL" :
					# One level Migraton
					self.migratePathROBasic(path)
					#Set dataset status as RO
					self.setDatasetStatusAsRO(path)
	

	def migrateBlockRO(self, path, blockName):
		srcInstanceName = self.getInstanceName(self.apiSrc)
		#srcInstanceName = "LOCAL"
		dstInstanceName = self.getInstanceName(self.apiDst)
		#dstInstanceName = "GLOBAL"

		self.checkDatasetStatus(path)
		self.checkInstances(srcInstanceName, dstInstanceName)
		if dstInstanceName == "GLOBAL" and srcInstanceName == "LOCAL" :
			#One level Migration
			self.migrateBlockBasic(path, blockName)
		else:
			if dstInstanceName == "LOCAL" and srcInstanceName == "GLOBAL" :
				# One level Migraton
				self.migrateBlockROBasic(path, blockName)
				#Set dataset status as RO
				self.setDatasetStatusAsRO(path)
			
	
	def setDatasetStatusAsRO(self, path):
		self.apiDst.updateProcDSStatus(path, "RO")
	
	def checkDatasetStatus(self, path):
		if self.isDatasetStatusRO(path):
			 raise DbsBadRequest (args = "Read Only dataset " + path + " CANNOT be Migrated.", code = 1222)
	

	def checkInstances(self, srcInstanceName, dstInstanceName):
		if dstInstanceName == "LOCAL" and srcInstanceName == "LOCAL" :
			 raise DbsBadRequest (args = "Local to Local migration is NOT allowed with one level transfer (excluding parentage).\n Either use GLOBAL instance as source DBS or do the complete migration (including parentage) using this API migrateDatasetContents with readOnly option set to False", code = 1223)

		else:
			if dstInstanceName == "GLOBAL" and srcInstanceName == "GLOBAL" :
				raise DbsBadRequest (args = "Global to Global NOT allowed", code = 1224)
	

	def pruneBranchesFromFile(self, fileName):
		f = open(fileName, "r")
		tmp = f.readline()
		content = ""
		while(tmp):
			content += self.pruneBranches(tmp)
			tmp = f.readline()
		return content
		

	def pruneBranches(self, line):
		if(line.find('file_branch') == -1):
			return line
		else:
			return ""
		
"""
usage = "\n****************************************************************" + \
	"\npython dbsMigrateRecursive.py source_url targert_url datasetPath blockName" + \
	"\nIf you do not supply this op parameter then the default is assumed which is both." + \
	"\nExample :" + \
	"\npython dbsMigrateRecursive.py SOURCE_DBS_URL TARGET_DBS_URL  /CSA06-081-os-minbias/DIGI/CMSSW_0_8_1-GEN-SIM-DIGI-1154005302-merged /CSA06-081-os-minbias/DIGI/CMSSW_0_8_1-GEN-SIM-DIGI-1154005302-merged#ABCDEGFH-12345-WERTY" + \
	"\n****************************************************************" 
	
if (len(sys.argv) < 4) :
	print usage
	sys.exit(1)


path = sys.argv[3]
transfer = DbsMigrateApi(sys.argv[1], sys.argv[2], True)
#api = transfer.getAPISrc()
#for block in api.listBlocks(path):
#	transfer.migrateBlock(path, block['Name'])
transfer.migratePath(path)

print "Done"
"""			
