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
	def getParentPathList(self, api, path, ignoreDuplicate = True):
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
                                                		if(ignoreDuplicate) :
									if(aPath not in self.allPaths):
        	                                                		pathList.append(aPath)
								else : pathList.append(aPath)
					else: 
						if(ignoreDuplicate) :
							pathList.append(aPath)
							if(aPath not in self.allPaths):
								self.allPaths.append(aPath)
						else:
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
	

	def sortParentPathList(self, pathList):
		#print 'passed in pathList ', pathList
		finalList = []
		tmpList = []
		for aDataset in pathList:
			#print 'checking ', aDataset
			found = False
			parentsOfDataset = self.getParentPathList(self.apiSrc, aDataset, False)
			for aParentOfDataset in parentsOfDataset:
				#print 'aParentOfDataset ' , aParentOfDataset
				if aParentOfDataset in pathList: 
					if aParentOfDataset not in finalList : finalList.append(aParentOfDataset)
					#print 'FOUND ' 
					found = True
			if not found: 
				if aDataset not in finalList : finalList.append(aDataset)
			else:
				if aDataset not in tmpList : tmpList.append(aDataset)

		for tmpDataset in tmpList:
			if tmpDataset not in finalList : finalList.append(tmpDataset)
		#print 'before retuning ', finalList

		return finalList

	def migrateForNiceBoys(self, path, blockName):
		"""
		Migration based on assumption that if parent block(s) are in destination, parent of parent blocks are also there and in good shape !
		"""
		# Test and see if dataset is migrateable
		self.checkDatasetStatus(path)
		###Here we can get Parents of the Block and then migrate the recurrsively
		parentblocks = self.apiSrc.listBlockParents(block_name=blockName)
		if parentblocks not in [[], None] :
			parentblockNames = [ x['Name'] for x in parentblocks ]   
			# Check to see if the parent(s) are already in DBS target
			parent_path=parentblockNames[0].split('#')[0]
			# See if parent dataset even exists at target, if not we can just migrate it
			if self.doesPathExist(self.apiDst, parent_path):
			    parentBlockInDst = self.apiDst.listBlocks(parent_path)
			    parentBlocksInDstName = [ y['Name'] for y in parentBlockInDst ]
			    for aLocalBlock in parentblockNames:
				if aLocalBlock in parentBlocksInDstName :
				    print "Block %s is already at destination" % aLocalBlock
				    continue
				else:
				    #Migrate This Parent Block
				    self.migrateForNiceBoys(parent_path, aLocalBlock)
			else: # if the parent dataset is NOT at target at all, just migrated the darn thing
			    for aLocalBlock in parentblockNames:
				self.migrateForNiceBoys(parent_path, aLocalBlock)
		# migrate this block
		# if block is not already at destination, only then try to migrate it
		blockInDst = self.apiDst.listBlocks(block_name=blockName)
		if len(blockInDst) > 0:
		    print "Block %s is already at destination" % blockName
		else :
		    self.migrateBlockBasic(path, blockName)
    
				
	def migratePath(self, path):
		
		#Get the parents of the path
		self.checkDatasetStatus(path)
		datasets = self.getParentPathList(self.apiSrc, path)
		if datasets not in [[], None] :
			print 'Sorting the parents because they themselves can be parents of each other'
			tmpDatasets = self.sortParentPathList(datasets)
			datasets = tmpDatasets[:]
			#print 'SORTED LIST ', datasets
			#return
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

		# If no block are found in source dataset
		# we can just simple migrate the DATASET (Processed Dataset et el) with no files	
		if len(blockInSrc) <= 0:
			self.migratePathNoBlocks(path)
		else:
			for aBlockInSrc in blockInSrc:
				if not self.isBlockIn(aBlockInSrc, blockInDst):
					self.migrateBlockBasic(path, aBlockInSrc['Name'])
				else :
					print "-----------------------------------------------------------------------------------"
					print "Ignoring path %s " %path
					print "            block %s " %aBlockInSrc['Name']
					print "because it already exist in the destination DBS and has NOT changed in source DBS"
					print "-----------------------------------------------------------------------------------\n"


	def migratePathNoBlocks(self, path):
		try:
    			token = path.split("/")
			src_ds = self.apiSrc.listProcessedDatasets(token[1], token[3], token[2])
    			proc = src_ds[0]
			# List algo details from source and insert then in dest.
			for algo in proc['AlgoList']:
				for srcAlgo in self.apiSrc.listAlgorithms(
								patternVer=algo['ApplicationVersion'], 
								patternFam=algo['ApplicationFamily'], 
								patternExe=algo['ExecutableName'], 
								patternPS=algo['ParameterSetID']['Hash'] ):
					self.apiDst.insertAlgorithm(srcAlgo)

    			#Grab the parents as well, hopefully the parenats are already migarted, otherwise its naturally n error condition
    			proc['ParentList'] = self.apiSrc.listDatasetParents(path)

    			#Create the dataset
    			self.apiDst.insertProcessedDataset (proc)

    			#Lets grab the Runs as well
    			ds_runs = self.apiSrc.listRuns(path)
    			#And add the to newly created dataset
    			for aRun in ds_runs:
				# Might have to insert the run first
				self.apiDst.insertRun(aRun)
				# Associate run to the procDS
        			self.apiDst.insertRunInPD(proc, aRun['RunNumber'])
			print "-----------------------------------------------------------------------------------"
			print " Migrated path : %s without blocks or files (empty) " % path
			print "-----------------------------------------------------------------------------------"
		except DbsApiException, ex:
			print "Unable to migrate the dataset path %s " %path
			raise ex

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
		"""
		migrateBlock, if their are parents, only the parent Block is migrated

		"""
		#Get the parents of the path
		self.checkDatasetStatus(path)

		###Here we can get Parents of the Block and then migrate the recurrsively
		parentblocks = self.apiSrc.listBlockParents(block_name=blockName)
		if parentblocks not in [[], None] :
			for ablock in parentblocks:
				self.migrateBlock(ablock['Path'], ablock['Name'])
		"""
		datasets = self.getParentPathList(self.apiSrc, path)
		if datasets not in [[], None] :
			for dataset in datasets:
				#Dont Check for existance of path if Block is given
				#if not self.doesPathExist(self.apiDst, dataset):
				#print "calling self.migratePath"
				self.migratePath( dataset)
		"""
		
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
		
		
	def getDatasetStatus(self, api, path):
		tokens = path.split('/')
		datasets = api.listProcessedDatasets(patternPrim = tokens[1], patternProc = tokens[2], patternDT = tokens[3])
		for aDataset in datasets:
			return aDataset['Status']
		
	
	def isDatasetStatusRO(self, api, path):
		if self.getDatasetStatus(api, path) == "VALID-RO":
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
                # ONLY migrate if the dataset is NOT at the destination (NO SHOW of FORCE here !!!)
                # I hate Read only datasets anyways - AA 10/08/2009
                if not self.doesPathExistNoForce(self.apiDst, path):
			if dstInstanceName == "GLOBAL" and srcInstanceName == "LOCAL" :
				#One level Migration
				self.migratePathBasic(path)
			else:
				if dstInstanceName == "LOCAL" and srcInstanceName == "GLOBAL" :
					# One level Migraton
					self.migratePathROBasic(path)
					#Set dataset status as RO
					self.setDatasetStatusAsRO(path)

                # If dataset is already there, give up
                else: raise DbsBadRequest (args = "Dataset already exists at destination, you cannot migrate it as a READ ONLY dataset", code = 1222)

	

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
		self.apiDst.updateProcDSStatus(path, "VALID-RO")
	
	def checkDatasetStatus(self, path):
		if self.isDatasetStatusRO(self.apiSrc, path):
			raise DbsBadRequest (args = "Read Only dataset " + path + " CANNOT be Migrated.", code = 1222)

                # Check the dataset status in Dst first, if it exists and if its status is RO, then we cannot migrate it
                if self.doesPathExistNoForce(self.apiDst, path):
                        if self.isDatasetStatusRO(self.apiDst, path):
                                raise DbsBadRequest (args = "Dataset " + path + " already exists at destination as a Read Only dataset it CANNOT be Re-Migrated.", code = 1225)

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
from dbsApi import DbsApi
def makeAPI(url):
                #args = {}
                #args['url'] = url
                args = {}
                if url.startswith('http'):
                        args['url'] = url
                        args['mode'] = 'POST'

                return DbsApi(args)

apiSrc = makeAPI('http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet')
apiDst  = makeAPI('http://cmssrv48.fnal.gov:8383/DBS/servlet/DBSServlet')
myList = ['/Cosmics/Commissioning08_CRAFT_ALL_V9_225-v2/RECO', '/Cosmics/Commissioning08-v1/RAW'] 
#myList = ['/Cosmics/Commissioning08_CRAFT_ALL_V9_225-v2/RECO'] 
api = DbsMigrateApi(apiSrc, apiDst)
myList = api.sortParentPathList(myList)
print myList
		
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
