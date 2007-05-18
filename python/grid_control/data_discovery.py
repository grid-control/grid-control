from __future__ import generators
import sys, os
from grid_control import AbstractObject, RuntimeError, utils


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



	def query(self, nJobs, eventsPerJob, firstEvent):
		jobNr = 0
		for job in self._splitJobs(self.filelist, eventsPerJob, firstEvent):
			if jobNr >= nJobs:
				break
			print "Jobinfo"
			print job
			yield (job['skip'], job['events'], job['files'])
			jobNr += 1


	def printDataset(self):
		print "Matching datasets:"
		for entry in self.filelist:
			print "LFN:",entry['lfn']
			print "status: ",entry['status']," , Events: ",entry['events']
		print "\nSummary:"
		print "NumberOfEvents: ",self.datasetBlockInfo['NumberOfEvents']
		print "NumberOfFiles : ",self.datasetBlockInfo['NumberOfFiles']
		print "SE List       : ",self.datasetBlockInfo['StorageElementList']
		print "\n\n"
