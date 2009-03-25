from __future__ import generators
import sys, os
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
		blocks = self._getBlocks()
		
		self.jobFiles = []

		for block in blocks:
#			self.jobFiles.extend(self._splitJobs(block['FileList'], eventsPerJob, 0)
			for job in self._splitJobs(block['FileList'], eventsPerJob, 0):
				job['StorageElementList']  =  block['StorageElementList']
##				print job
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
			print "BlockName: ",block['BlockName']
			print "NumberOfEvents: ",block['NumberOfEvents']
			print "NumberOfFiles : ",block['NumberOfFiles']
			print "SE List       : ",block['StorageElementList']
			print "Files: "
			for fileinfo in block['FileList'] :
				print fileinfo['lfn'],"( status: ",fileinfo['status'],", Events: ",fileinfo['events'],")"


	def printJobInfo(self):
		jobNum = 0
		for entry in self.jobFiles:
			print "Job number: ",jobNum
			self.printInfoForJob(entry)
			print "------------"			
			jobNum += 1


	def printInfoForJob(self, job):
		print "Events: ",job['events']
		print "Skip  : ",job['skip']
		print "SEList: ",job['StorageElementList']
		print "Files :"
		for thefile in job['files']:
			print thefile
