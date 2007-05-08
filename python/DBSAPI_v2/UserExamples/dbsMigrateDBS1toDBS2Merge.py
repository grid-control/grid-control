#!/usr/bin/env python
#
#
#

import os, re, string, xml.sax, xml.sax.handler, sys
from xml.sax.saxutils import escape
from cStringIO import StringIO

# DBS specific modules
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsHttpService import DbsHttpService
from DBSAPI.dbsExecService import DbsExecService

from DBSAPI.dbsException import DbsException
from DBSAPI.dbsApiException import *

from DBSAPI.dbsBaseObject import *
from DBSAPI.dbsRun import DbsRun
from DBSAPI.dbsQueryableParameterSet import DbsQueryableParameterSet
from DBSAPI.dbsProcessedDataset import DbsProcessedDataset
from DBSAPI.dbsPrimaryDataset import DbsPrimaryDataset
from DBSAPI.dbsLumiSection import DbsLumiSection
from DBSAPI.dbsFile import DbsFile
from DBSAPI.dbsFileBlock import DbsFileBlock
from DBSAPI.dbsDataTier import DbsDataTier
from DBSAPI.dbsAlgorithm import DbsAlgorithm

from DBSAPI.dbsParent import DbsParent
from DBSAPI.dbsConfig import DbsConfig
from DBSAPI.dbsOptions import DbsOptionParser

from DBSAPIOLD.dbsCgiApi import DbsCgiApi

optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)

fileName =  sys.argv[1]
f = open(fileName, "r")
x = " "
data = f.readline()
while(x):
	x = f.readline()
	data += x
#print data

DEFAULT_URL = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquerytest2"
args = {}
args['instance'] = 'MCGlobal/Writer'
cgiApi = DbsCgiApi(DEFAULT_URL, args)

def makeTierList(tierList) :
	myTier = ''
	useDash = False
	if('GEN' in tierList):
		myTier = 'GEN'
		useDash = True
	
	if('SIM' in tierList):
		if (useDash): myTier += '-'
		myTier += 'SIM'
		useDash = True
		
	if('DIGI' in tierList):
		if (useDash): myTier += '-'
		myTier += 'DIGI'
		useDash = True
	print "MyTier is " + myTier
	return myTier
	

try:
	class Handler (xml.sax.handler.ContentHandler):
		def __init__ (self):
			self.tierList = []
			self.evc = {}
			self.parent = {}
			self.first = True
			self.fileList = []
			self.blockList = []
			#self.algoList = []
			self.block = None
			self.datasetPath = None
			print "Initilized"

		def startElement(self, name, attrs):
			if name == 'primary-dataset':
				print "primary-dataset found %s " % str(attrs['name'])
				self.primary = DbsPrimaryDataset (Name = str(attrs['name']), Type = "MC")
			if name == 'processed-dataset':
				print "processed-dataset found %s " % str(attrs['path'])
				self.datasetPath = str(attrs['path'])
				path = self.datasetPath.split('/')
				tier = path[2]
				"""
				pattren = '/' + path[1] + '/*/' + path[3]
				print "Listing datasets from DBS1 with pattren " + pattren
				datasetDBS1 = cgiApi.listProcessedDatasets (pattren)
				for i in datasetDBS1:
					pathDBS1 = i['datasetPathName']
					tier = pathDBS1.split('/')[2]
					#if((tier != 'RECO') & (tier != 'RAW') & (tier != 'FEVT')):
					if((tier != 'RECO') & (tier != 'RAW')):
						self.tierList.append(tier)
				"""
				if( (tier == 'GEN') | (tier == 'SIM') ):
					self.tierList.append('GEN')
					self.tierList.append('SIM')
					self.datasetPath = '/' + self.primary['Name'] + '/' + path[3] + '/' + 'GEN-SIM'
					
				if( (tier == 'DIGI') | (tier == 'RECO') ):
					self.tierList.append('DIGI')
					self.tierList.append('RECO')
					self.datasetPath = '/' + self.primary['Name'] + '/' + path[3] + '/' + 'GEN-SIM-DIGI-RECO'

				if( tier == 'FEVT' ):
					self.tierList.append('FEVT')
					self.datasetPath = '/' + self.primary['Name'] + '/' + path[3] + '/' + 'GEN-SIM-DIGI-RECO'


				"""
				#self.tierList = [path[2]]
				if('FEVT' in self.tierList) : 
					self.datasetPath = '/' + self.primary['Name'] + '/' + path[3] + '/' + 'FEVT'
				else :
					self.datasetPath = '/' + self.primary['Name'] + '/' + path[3] + '/' + makeTierList(self.tierList)
				
				"""
				#import pdb
				#pdb.set_trace()
				self.processed = DbsProcessedDataset (
						PrimaryDataset = self.primary,
						Name = path[3],
						#PhysicsGroup = "Online Selection",
						PhysicsGroup = "B-physics",
						Status = "VALID",
						TierList = self.tierList
						)
				
			if name == 'event-collection':
				print "event-collection found %s " % str(attrs['name'])
				name = str(attrs['name']).split('/')
				length = len(name) - 1
				if name[length].endswith(".root"):
					self.fileName = name[length]
				else:
					self.fileName = name[length] + '.root'
				self.evc[self.fileName] = int(attrs['events'])
				print "Saving  events for " + self.fileName + " = " + str(self.evc[self.fileName])
				
			if name == 'parent':
				print "parent found %s " % str(attrs['name'])
				name = str(attrs['name']).split('_')[1:]
				pName = str(name[0])
				for tmp in name[1:]:
					pName = pName + '_' + str(tmp)
				pName += '.root'
				length = len(name) - 1
				self.parent[self.fileName] = pName
				
				
			if name == 'processing':
				print "processing found %s " % str(attrs['executable'])
				origHash = str(attrs['hash'])
				tmp = origHash.split(';')
				if(len(tmp) == 1) :
					hash = origHash
				else :
					tmp = tmp[0].split('=')
					if(len(tmp) == 1) :
						hash = tmp[0]
					else :
						hash = tmp[1]
				print hash
				self.algo = DbsAlgorithm (
						ExecutableName = str(attrs['executable']),
						ApplicationVersion = str(attrs['version']),
						ApplicationFamily = str(attrs['family']),
						ParameterSetID = DbsQueryableParameterSet(
							Hash = hash,
							Name = hash,
							Version="NOTKNOWN",
							Type = "NOTKNOWN",
							Annotation = "NOTKNOWN",
							Content = "NOTKNOWN"
							)
						)
				print "Inserting algorithm  %s " % self.algo
				api.insertAlgorithm (self.algo)
				if(self.first == False):
					api.insertAlgoInPD(self.processed, self.algo)

				
			if name == 'block':
				print "block found %s " % str(attrs['name'])
				if(self.first):
					print "Inserting primary %s " % self.primary
					api.insertPrimaryDataset (self.primary)
					#print "Inserting algorithm  %s " % self.algo
					#api.insertAlgorithm (self.algo)
					self.processed['AlgoList'] = [self.algo]
					print "Inserting processed  %s " % self.processed
					api.insertProcessedDataset (self.processed)
					self.first = False
					
				if (self.block != None) :
					if (len(self.fileList) > 0) :
						print "Inserting files  %s " % self.fileList
						print "block  %s " % self.block
						api.insertFiles (self.processed, self.fileList, self.block)
						self.fileList = []
						
				if (attrs['files'] != '0'):
					#ofw = '0';
					if (str(attrs['status']).upper() != 'OPEN'):
						self.blockList.append(str(attrs['name']))
						#ofw = '1'
					self.block = DbsFileBlock (
						Name = str(attrs['name']),
						#OpenForWriting = ofw
						)
					print "Inserting block  %s " % self.block
					print "proc dataset is %s " % self.processed
					api.insertBlock (self.datasetPath, self.block)
				
			if name == 'afile':
				print "afile found %s " % str(attrs['lfn'])
				if (len(self.fileList) == 50) :
					print "Inserting files  %s " % self.fileList
					print "block  %s " % self.block
					api.insertFiles (self.processed, self.fileList, self.block)
					self.fileList = []
					
				name = attrs['lfn'].split('/')
				length = len(name) - 1
				noOfEvents = 0
				rootName = name[length]
				#print "checking no of events for " + rootName
				if (rootName in self.evc.keys()):
					noOfEvents = self.evc[rootName]
					print "Found noOfEvents = " + str(noOfEvents)
					
				parentName = ''
				if (rootName in self.parent.keys()):
					parentName = self.parent[rootName]

					
				tmp =  str(attrs['checksum']).split(':')
				if (len(tmp) == 1):
					checkSum = tmp[0]
				else:
					checkSum = tmp[1]

				fileType = str(attrs['type'])
				if (fileType == 'EVD'): fileType = 'EDM'
				self.dbsfile = DbsFile (
						Checksum = checkSum,
						LogicalFileName = str(attrs['lfn']),
						NumberOfEvents = noOfEvents,
						FileSize = int(attrs['size']),
						Status = str(attrs['status']),
						ValidationStatus = 'VALID',
						FileType = fileType,
						AlgoList = [self.algo],
						TierList = self.tierList
						)
				if len(parentName) > 0 :
					self.dbsfile['ParentList'] =  [parentName]
				self.fileList.append(self.dbsfile)
				
		def endElement(self, name):
			if name == 'processing':
				if (len(self.fileList) > 0) :
					print "Inserting files  %s " % self.fileList
					print "block  %s " % self.block
					api.insertFiles (self.processed, self.fileList, self.block)
				for b in self.blockList:
					tmpBlockFile = open('CloseBlock.txt', 'a+')
					print "Closing block %s " % b
					tmpBlockFile.write(b)
					tmpBlockFile.write("\n")
					tmpBlockFile.close()
					#api.closeBlock (b)
								
	xml.sax.parseString (data, Handler ())
except Exception, ex:
	print "Exception %s " % ex
print "Done"

