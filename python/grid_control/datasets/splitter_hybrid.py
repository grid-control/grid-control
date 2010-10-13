from splitter_base import DataSplitter
from provider_base import DataProvider

class HybridSplitter(DataSplitter):
	def __init__(self, config, section = None):
		DataSplitter.__init__(self, config, section)
		self.eventsPerJob = self.setup(config.getInt, 'events per job')


	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in blocks:
			(events, fileStack, metaStack) = (0, [], [])

			def returnSplit():
				job = dict()
				job[DataSplitter.Skipped] = 0
				job[DataSplitter.FileList] = fileStack
				job[DataSplitter.NEvents] = events
				if DataProvider.Metadata in block:
					job[DataSplitter.Metadata] = metaStack
				return self.cpBlockInfoToJob(block, job)

			for fileInfo in block[DataProvider.FileList]:
				nextEvents = events + fileInfo[DataProvider.NEvents]
				if (len(fileStack) > 0) and (nextEvents > self.eventsPerJob):
					yield returnSplit()
					(events, fileStack, metaStack) = (0, [], [])
				events += fileInfo[DataProvider.NEvents]
				fileStack.append(fileInfo[DataProvider.lfn])
				if DataProvider.Metadata in block:
					metaStack.append(fileInfo[DataProvider.Metadata])
			yield returnSplit()
