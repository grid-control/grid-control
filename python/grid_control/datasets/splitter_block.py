from splitter_base import DataSplitter
from provider_base import DataProvider

class BlockBoundarySplitter(DataSplitter):
	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in blocks:
			job = dict()
			job[DataSplitter.Skipped] = 0
			job[DataSplitter.SEList] = block[DataProvider.SEList]
			job[DataSplitter.Dataset] = block[DataProvider.Dataset]
			if DataProvider.Nickname in block:
				job[DataSplitter.Nickname] = block[DataProvider.Nickname]
			if DataProvider.DatasetID in block:
				job[DataSplitter.DatasetID] = block[DataProvider.DatasetID]
			files = block[DataProvider.FileList]
			job[DataSplitter.FileList] = map(lambda x: x[DataProvider.lfn], files)
			job[DataSplitter.NEvents] = sum(map(lambda x: x[DataProvider.NEvents], files))
			yield job
