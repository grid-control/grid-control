#-#  Copyright 2010-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from splitter_base import DataSplitter
from provider_base import DataProvider

# Base class for (stackable) splitters with file level granularity
class FileLevelSplitter(DataSplitter):
	def splitBlocks(self, blocks):
		raise AbstractError

	def newBlock(self, old, filelist):
		new = dict(old)
		new[DataProvider.FileList] = filelist
		new[DataProvider.NEntries] = sum(map(lambda x: x[DataProvider.NEntries], filelist))
		return new

	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in self.splitBlocks(blocks):
			yield self.finaliseJobSplitting(block, dict(), block[DataProvider.FileList])


class FLSplitStacker(FileLevelSplitter):
	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in blocks:
			splitterList = self.setup(self.config.getList, block, 'splitter stack', ['BlockBoundarySplitter'])
			subSplitter = map(lambda x: FileLevelSplitter.open(x, self.config), splitterList[:-1])
			endSplitter = DataSplitter.open(splitterList[-1], self.config)
			for subBlock in reduce(lambda x, y: y.splitBlocks(x), subSplitter, [block]):
				for splitting in endSplitter.splitDatasetInternal([subBlock]):
					yield splitting


# Split only along block boundaries
class BlockBoundarySplitter(FileLevelSplitter):
	def splitBlocks(self, blocks):
		return blocks


# Split dataset along block boundaries into jobs with 'files per job' files
class FileBoundarySplitter(FileLevelSplitter):
	def splitBlocks(self, blocks):
		for block in blocks:
			start = 0
			filesPerJob = self.setup(self.config.getInt, block, 'files per job')
			while start < len(block[DataProvider.FileList]):
				files = block[DataProvider.FileList][start : start + filesPerJob]
				start += filesPerJob
				yield self.newBlock(block, files)


# Split dataset along block and file boundaries into jobs with (mostly <=) 'events per job' events
# In case of file with #events > 'events per job', use just the single file (=> job has more events!)
class HybridSplitter(FileLevelSplitter):
	def splitBlocks(self, blocks):
		for block in blocks:
			(events, fileStack) = (0, [])
			eventsPerJob = self.setup(self.config.getInt, block, 'events per job')
			for fileInfo in block[DataProvider.FileList]:
				if (len(fileStack) > 0) and (events + fileInfo[DataProvider.NEntries] > eventsPerJob):
					yield self.newBlock(block, fileStack)
					(events, fileStack) = (0, [])
				fileStack.append(fileInfo)
				events += fileInfo[DataProvider.NEntries]
			yield self.newBlock(block, fileStack)
