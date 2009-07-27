from splitter_base import DataSplitter
from provider_base import DataProvider

class FileBoundarySplitter(DataSplitter):
	def __init__(self, parameters):
		DataSplitter.__init__(self, parameters)
		self.filesPerJob = parameters['eventsPerJob']


	def splitDatasetInternal(self, blocks, firstEvent = 0):
		result = []
		for block in blocks:
			start = 0
			while start < len(block[DataProvider.FileList]):
				job = dict()
				job[DataSplitter.Skipped] = 0
				job[DataSplitter.SEList] = block[DataProvider.SEList]
				job[DataSplitter.Dataset] = block[DataProvider.Dataset]
				if block.has_key(DataProvider.Nickname):
					job[DataSplitter.Nickname] = block[DataProvider.Nickname]
				if block.has_key(DataProvider.DatasetID):
					job[DataSplitter.DatasetID] = block[DataProvider.DatasetID]

				files = block[DataProvider.FileList][start : start + self.filesPerJob]
				job[DataSplitter.FileList] = map(lambda x: x[DataProvider.lfn], files)
				job[DataSplitter.NEvents] = sum(map(lambda x: x[DataProvider.NEvents], files))
				start += self.filesPerJob
				result.append(job)
		return result
