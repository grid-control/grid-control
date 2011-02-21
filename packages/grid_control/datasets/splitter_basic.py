from splitter_base import DataSplitter
from provider_base import DataProvider

# Split only along block boundaries
class BlockBoundarySplitter(DataSplitter):
	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in blocks:
			yield self.finaliseJobSplitting(block, dict(), block[DataProvider.FileList])


# Split dataset along block boundaries into jobs with 'files per job' files
class FileBoundarySplitter(DataSplitter):
	def __init__(self, config, section = None):
		DataSplitter.__init__(self, config, section)
		self.filesPerJob = self.setup(config.getInt, 'files per job')


	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in blocks:
			start = 0
			while start < len(block[DataProvider.FileList]):
				files = block[DataProvider.FileList][start : start + self.filesPerJob]
				start += self.filesPerJob
				yield self.finaliseJobSplitting(block, dict(), files)


# Split dataset along block and file boundaries into jobs with ~ 'events per job' events
# In case of file with #events > 'events per job', use just the single file (=> job has more events!)
class HybridSplitter(DataSplitter):
	def __init__(self, config, section = None):
		DataSplitter.__init__(self, config, section)
		self.eventsPerJob = self.setup(config.getInt, 'events per job')


	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in blocks:
			(events, fileStack) = (0, [])
			for fileInfo in block[DataProvider.FileList]:
				events += fileInfo[DataProvider.NEvents]
				if (len(fileStack) > 0) and (events > self.eventsPerJob):
					yield self.finaliseJobSplitting(block, dict(), fileStack)
					(events, fileStack) = (0, [])
				fileStack.append(fileInfo)
			yield self.finaliseJobSplitting(block, dict(), fileStack, -1)
