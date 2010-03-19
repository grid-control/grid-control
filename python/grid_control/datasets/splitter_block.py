from splitter_base import DataSplitter
from provider_base import DataProvider

class BlockBoundarySplitter(DataSplitter):
	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in blocks:
			job = dict()
			job[DataSplitter.Skipped] = 0
			files = block[DataProvider.FileList]
			job[DataSplitter.FileList] = map(lambda x: x[DataProvider.lfn], files)
			job[DataSplitter.NEvents] = sum(map(lambda x: x[DataProvider.NEvents], files))
			yield self.cpBlockToJob(block, job)
