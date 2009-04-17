from __future__ import generators
import sys, os, gzip, cPickle
from grid_control import AbstractObject, RuntimeError, utils, ConfigError, DataProvider

class DataSplitter(AbstractObject):
	splitInfos = ('Dataset', 'NEvents', 'SEList', 'FileList', 'Skipped', 'Nickname', 'DatasetID')
	for id, splitInfo in enumerate(splitInfos):
		locals()[splitInfo] = id

	def __init__(self):
		self.jobFiles = None


	def splitDatasetInternal(self, blocks):
		raise AbstractError


	def splitDataset(self, blocks):
		log = utils.ActivityLog('Splitting dataset into jobs')
		if self.jobFiles == None:
			self.jobFiles = self.splitDatasetInternal(blocks)
		return self.jobFiles


	def getFilesForJob(self, jobNr):
		if jobNr >= len(self.jobFiles):
			raise ConfigError("Job %d out of range for available dataset"  % jobNr)	
		return self.jobFiles[jobNr]


	def getSitesForJob(self, jobNr):
		if jobNr >= len(self.jobFiles):
			raise ConfigError("Job %d out of range for available dataset"  % jobNr)	
		return self.jobFiles[jobNr][DataSplitter.SEList]


	def getNumberOfJobs(self):
		return len(self.jobFiles)


	# TODO: job->jobNr ?
	def printInfoForJob(job):
		print "Dataset: ", job[DataSplitter.Dataset]
		print "Events : ", job[DataSplitter.NEvents]
		print "Skip   : ", job[DataSplitter.Skipped]
		print "SEList : ", job[DataSplitter.SEList]
		print "Files  : ", str.join("\n          ", job[DataSplitter.FileList])
	printInfoForJob = staticmethod(printInfoForJob)


	def printAllJobInfo(self):
		for jobNum, entry in enumerate(self.jobFiles):
			print "Job number: ", jobNum
			self.printInfoForJob(entry)
			print "------------"			


	def saveState(self, path):
		fp = gzip.GzipFile(os.path.join(path, 'jobdata.map'), 'wb')
		cPickle.dump(self, fp)
		fp.close()


	def loadState(path):
		fp = gzip.GzipFile(os.path.join(path, 'jobdata.map'), 'rb')
		dbs = cPickle.load(fp)
		fp.close()
		return dbs
	loadState = staticmethod(loadState)


class DefaultSplitter(DataSplitter):
	def __init__(self, eventsPerJob):
		DataSplitter.__init__(self)
		self.eventsPerJob = eventsPerJob


	def _splitJobs(self, fileList, firstEvent):
		nextEvent = firstEvent
		succEvent = nextEvent + self.eventsPerJob
		curEvent = 0
		lastEvent = 0
		curSkip = 0
		fileListIter = iter(fileList)
		job = { DataSplitter.Skipped: 0, DataSplitter.NEvents: 0, DataSplitter.FileList: [] }
		while True:
			if curEvent >= lastEvent:
				try:
					fileList = fileListIter.next();
				except StopIteration:
					if len(job[DataSplitter.FileList]):
						yield job
					break

				nEvents = fileList[DataProvider.NEvents]
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

			if not len(job[DataSplitter.FileList]):
				job[DataSplitter.Skipped] = curSkip

			job[DataSplitter.NEvents] += available
			nextEvent += available

			job[DataSplitter.FileList].append(fileList[DataProvider.lfn])

			if nextEvent >= succEvent:
				succEvent += self.eventsPerJob
				yield job
				job = { DataSplitter.Skipped: 0, DataSplitter.NEvents: 0, DataSplitter.FileList: [] }


	def splitDatasetInternal(self, blocks):
		result = []
		for block in blocks:
			for job in self._splitJobs(block[DataProvider.FileList], 0):
				job[DataSplitter.SEList] = block[DataProvider.SEList]
				job[DataSplitter.Dataset] = block[DataProvider.Dataset]
				if block.has_key(DataProvider.Nickname):
					job[DataSplitter.Nickname] = block[DataProvider.Nickname]
				if block.has_key(DataProvider.DatasetID):
					job[DataSplitter.DatasetID] = block[DataProvider.DatasetID]
				result.append(job)
		return result
