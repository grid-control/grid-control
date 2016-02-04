#-#  Copyright 2011-2015 Karlsruhe Institute of Technology
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

from grid_control.datasets.provider_base import DataProvider
from grid_control.datasets.splitter_basic import FileLevelSplitter
from hpfwk import AbstractError

# Split dataset along block and metadata boundaries - using equivalence classes of metadata
class MetadataSplitter(FileLevelSplitter):
	def metaCmp(self, metadataNames, fiA, fiB):
		raise AbstractError

	def splitBlocks(self, blocks):
		for block in blocks:
			files = block[DataProvider.FileList]
			files.sort(lambda a, b: self.metaCmp(block[DataProvider.Metadata], block, a, b))
			(fileStack, reprElement) = ([], None)
			for fi in files:
				if reprElement is None:
					reprElement = fi
				if self.metaCmp(block[DataProvider.Metadata], block, fi, reprElement) != 0:
					yield self.newBlock(block, fileStack)
					(fileStack, reprElement) = ([], fi)
				fileStack.append(fi)
			yield self.newBlock(block, fileStack)


class UserMetadataSplitter(MetadataSplitter):
	def metaCmp(self, metadataNames, block, fiA, fiB):
		selMetadataNames = self.setup(self.config.getList, block, 'split metadata', [])
		selMetadataIdx = map(lambda name: metadataNames.index(name), selMetadataNames)
		getMetadata = lambda fi: map(lambda idx: fi[DataProvider.Metadata][idx], selMetadataIdx)
		return cmp(getMetadata(fiA), getMetadata(fiB))


class RunSplitter(MetadataSplitter):
	def metaCmp(self, metadataNames, block, fiA, fiB):
		mdIdx = metadataNames.index('Runs')
		return cmp(fiA[DataProvider.Metadata][mdIdx], fiB[DataProvider.Metadata][mdIdx])
