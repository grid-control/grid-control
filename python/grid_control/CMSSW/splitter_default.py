from splitter_base import DataSplitter
from provider_base import DataProvider

class DefaultSplitter(DataSplitter):
	def __init__(self, parameters):
		DataSplitter.__init__(self, parameters)
		self.eventsPerJob = parameters['eventsPerJob']


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


	def splitDatasetInternal(self, blocks, firstEvent = 0):
		result = []
		for block in blocks:
			for job in self._splitJobs(block[DataProvider.FileList], firstEvent):
				firstEvent = 0
				job[DataSplitter.SEList] = block[DataProvider.SEList]
				job[DataSplitter.Dataset] = block[DataProvider.Dataset]
				if block.has_key(DataProvider.Nickname):
					job[DataSplitter.Nickname] = block[DataProvider.Nickname]
				if block.has_key(DataProvider.DatasetID):
					job[DataSplitter.DatasetID] = block[DataProvider.DatasetID]
				result.append(job)
		return result
