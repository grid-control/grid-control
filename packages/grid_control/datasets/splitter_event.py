from python_compat import *
from grid_control import DatasetError
from splitter_base import DataSplitter
from provider_base import DataProvider

class EventBoundarySplitter(DataSplitter):
	def __init__(self, config, section = None):
		DataSplitter.__init__(self, config, section)
		self.eventsPerJob = self.setup(config.getInt, 'events per job')


	def neededVars(cls):
		return [DataSplitter.FileList, DataSplitter.Skipped, DataSplitter.NEvents]
	neededVars = classmethod(neededVars)


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
					fileObj = next(fileListIter);
				except StopIteration:
					if len(job[DataSplitter.FileList]):
						yield job
					break

				nEvents = fileObj[DataProvider.NEvents]
				if nEvents < 0:
					raise DatasetError('EventBoundarySplitter does not support files with a negative number of events!')
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

			job[DataSplitter.FileList].append(fileObj[DataProvider.lfn])
			if DataProvider.Metadata in fileObj:
				job.setdefault(DataSplitter.Metadata, []).append(fileObj[DataProvider.Metadata])

			if nextEvent >= succEvent:
				succEvent += self.eventsPerJob
				yield job
				job = { DataSplitter.Skipped: 0, DataSplitter.NEvents: 0, DataSplitter.FileList: [] }


	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in blocks:
			for job in self._splitJobs(block[DataProvider.FileList], firstEvent):
				firstEvent = 0
				yield self.finaliseJobSplitting(block, job)
