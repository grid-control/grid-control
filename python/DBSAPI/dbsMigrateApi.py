import sys
import os
from dbsException import *
from dbsApiException import *
from dbsOptions import DbsOptionParser
from dbsUtil import *

from dbsApi import DbsApi

def makeAPI(url, version = "DBS_2_0_8"):
                args = {}
                if url.startswith('http'):
                        args['url']     = url
                        args['mode']    = 'POST'
                        if version:
                            args['version'] = version

                return DbsApi(args)

class DbsMigrateApi:
	"""
        Migration based on assumption that if parent block(s) are in destination, parent of parent blocks are also there and in good shape !
        """
	
        def __init__(self, srcURL, dstURL):
                self.apiSrc = makeAPI(srcURL)
                self.apiDst = makeAPI(dstURL)
		self.testedDatasets = {}

	def migrateDataset(self, path):
	    """
	    migrate a dataset path, alongwith its parent datasets
	    """
	    #See if dataset has parents, migrate them first
	    #this gurantees that parent blocks have alread made it to DST
	    parentdatasets = self.apiSrc.listPathParents(path)
	    for aparentds in parentdatasets:
	        self.migrateDataset(aparentds['PathList'][0])
	    # Now lets migrate the dataset, block at a time, and making sure only NEW blocks are migrated, if the path is already at dest.
	    blocksInSrc = self.apiSrc.listBlocks(path, nosite=True)
	    blocksInSrcNames = [ y['Name'] for y in blocksInSrc]
	    blocksAtDstNames = []
	    if self.doesPathExist(self.apiDst, path):
		# If the dataset is already at dst
		# get the blocks that are already there, and these will not be migrated
		blocksAtDst = self.apiDst.listBlocks(path, nosite=True)
		blocksAtDstNames = [ y['Name'] for y in blocksAtDst]
	    for aLocalBlock in blocksInSrcNames:
		if aLocalBlock in blocksAtDstNames:
		    print "Block %s is already at destination" % aLocalBlock
		    continue
		else:
		    #Migrate This Block, its parents MUST have already be there
		    self.migrateThisBlock(aLocalBlock)

        def migrateBlock(self, blockName, checkExistence=True):
		# if the block existence at DST is not already checked somewhere, lets check it
		if checkExistence:
                	# if block is not already at destination, only then try to migrate it
                	blockInDst = self.apiDst.listBlocks(block_name=blockName, nosite=True)
                	if len(blockInDst) > 0:
                    		print "Block %s is already at destination" % blockName
                #migrates a block, with its parents, if not already at dst
                parentblocks = self.apiSrc.listBlockParents(block_name=blockName)
                if parentblocks not in [[], None] :
                        parentblockNames = [ x['Name'] for x in parentblocks ]
                        # Check to see if the parent(s) are already in DBS target
                        parent_path=parentblockNames[0].split('#')[0]
                        # See if parent dataset even exists at target, if not we can just migrate it
                        if self.doesPathExist(self.apiDst, parent_path):
                            parentBlockInDst = self.apiDst.listBlocks(parent_path, nosite=True)
                            parentBlocksInDstName = [ y['Name'] for y in parentBlockInDst ]
                            for aLocalBlock in parentblockNames:
                                if aLocalBlock in parentBlocksInDstName :
				    # Block is already at DST, so its parents must also be, why even bother checking them
                                    print "Block %s is already at destination" % aLocalBlock
                                    continue
                                else:
                                    #Migrate This Parent Block
                                    self.migrateBlock(aLocalBlock, checkExistence=False)
                        else: # if the parent dataset is NOT at target at all, just migrated the darn thing
                            for aLocalBlock in parentblockNames:
                                self.migrateBlock(aLocalBlock)
		#just simply migrate the block
		self.migrateThisBlock(blockName)

	def migrateThisBlock(self, blockName):
		"""This is INTERNAL method to this class, do not call it from outside"""
		# Lets just migrate the block
		try:
            		path = blockName.split('#')[0]
                	print "-----------------------------------------------------------------------------------"
                        print "Transferring path %s " %path
                        print "            block %s " %blockName
                        print "-----------------------------------------------------------------------------------\n"
			xmlinput=self.apiSrc.listDatasetContents(path,  blockName)
			#fileName = blockName.replace('/', '_').replace('#', '_') + ".xml"
			#f = open(fileName, "w");
			#f.write(xmlinput)
			#f.close()
                        self.apiDst.insertDatasetContents(xmlinput)
		
                except DbsBadRequest, ex:
                	# If not block excep then raise it again
                	if int(ex.getErrorCode()) != 1024:
                        	raise ex

        def doesPathExist(self, api, path):
		exists = False
		# check in the maintained cache, if this dataset was already tested, why make an API call
		if self.testedDatasets.has_key(path):
			return self.testedDatasets[path]
                tokens = path.split('/')
                datasets = api.listProcessedDatasets(patternPrim = tokens[1], patternProc = tokens[2], patternDT = tokens[3])
                if datasets in [[], None] :
                        exists = False
                else:
                        exists = True;
		#stow away this information for later to be used in this same process, why make this call again, if we have checked once
		self.testedDatasets[path]=exists
		return exists

