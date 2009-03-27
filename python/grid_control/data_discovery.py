from __future__ import generators
import sys, os, gzip, cPickle
from grid_control import AbstractObject, RuntimeError, utils, ConfigError

class DataDiscovery(AbstractObject):
	def _splitJobs(fileList, eventsPerJob, firstEvent):
		nextEvent = firstEvent
		succEvent = nextEvent + eventsPerJob
		curEvent = 0
		lastEvent = 0
		curSkip = 0
		fileListIter = iter(fileList)
		job = { 'skip': 0, 'events': 0, 'files': [] }
		while True:
			if curEvent >= lastEvent:
				try:
					fileList = fileListIter.next();
				except StopIteration:
					if len(job['files']):
						yield job
					break

				nEvents = fileList['events']
				curEvent = lastEvent
				lastEvent = curEvent + nEvents
				curSkip = 0

			if nextEvent >= lastEvent:
				curEvent = lastEvent
				continue

			curSkip += nextEvent - curEvent
			curEvent = nextEvent

			available = lastEvent - curEvent
			if succEvent - nextEvent < available:
				available = succEvent - nextEvent

			if not len(job['files']):
				job['skip'] = curSkip
				
			job['events'] += int(available)
			nextEvent += available

			job['files'].append(fileList['lfn'])

			if nextEvent >= succEvent:
				succEvent += eventsPerJob
				yield job
				job = { 'skip': 0, 'events': 0, 'files': [] }

		else:
			if len(job['files']):
				yield job
	_splitJobs = staticmethod(_splitJobs)


	def run(self, eventsPerJob):
		self.jobFiles = []
		blocks = self._getBlocks()

		for block in blocks:
#			self.jobFiles.extend(self._splitJobs(block['FileList'], eventsPerJob, 0)
			for job in self._splitJobs(block['FileList'], eventsPerJob, 0):
				job['StorageElementList']  =  block['StorageElementList']
				job['DatasetPath'] = self.datasetPath
				self.jobFiles.append(job)


	def getFileRangeForJob(self, jobNr):
		if jobNr >= len(self.jobFiles):
			raise ConfigError("Job %d out of range for available dataset"  % jobNr)	
		return self.jobFiles[jobNr]


	def getSitesForJob(self, jobNr):
		if jobNr >= len(self.jobFiles):
			raise ConfigError("Job %d out of range for available dataset"  % jobNr)	
		return self.jobFiles[jobNr]['StorageElementList']


	def getNumberOfJobs(self):
		return len(self.jobFiles)


	def printDataset(self):
		print "Matching datasets:"
		for block in self._getBlocks():
			print "BlockName: ", block['BlockName']
			print "NumberOfEvents: ", block['NumberOfEvents']
			print "NumberOfFiles : ", block['NumberOfFiles']
			print "SE List       : ", block['StorageElementList']
			print "Files: "
			for fileinfo in block['FileList'] :
				print fileinfo['lfn'], "(",
				infos = []
				for tag in [('Status', 'status'), ('Events', 'events')]:
					if not str(fileinfo[tag[1]]) == "":
						infos.append(tag[0] + ": " + str(fileinfo[tag[1]]))
				print ", ".join(infos), ")"


	def printJobInfo(self):
		jobNum = 0
		for entry in self.jobFiles:
			print "Job number: ", jobNum
			self.printInfoForJob(entry)
			print "------------"			
			jobNum += 1


	def printInfoForJob(self, job):
		if job.get('DatasetPath') != None:
			print "Dataset: ", job['DatasetPath']
		print "Events : ", job['events']
		print "Skip   : ", job['skip']
		print "SEList : ", job['StorageElementList']
		print "Files  : ", "\n          ".join(job['files'])


	def saveState(self, path):
		fp = gzip.GzipFile(os.path.join(path, 'dbscache.dat'), 'wb')
		cPickle.dump(self, fp)
		fp.close()


	def loadState(path):
		fp = gzip.GzipFile(os.path.join(path, 'dbscache.dat'), 'rb')
		dbs = cPickle.load(fp)
		fp.close()
		return dbs
	loadState = staticmethod(loadState)
