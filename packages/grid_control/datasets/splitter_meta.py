# | Copyright 2011-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

from grid_control.datasets.provider_base import DataProvider
from grid_control.datasets.splitter_basic import FileLevelSplitter
from hpfwk import AbstractError
from python_compat import imap, sort_inplace

# Split dataset along block and metadata boundaries - using equivalence classes of metadata
class MetadataSplitter(FileLevelSplitter):
	def metaKey(self, metadataNames, block, fi):
		raise AbstractError

	def splitBlocks(self, blocks):
		for block in blocks:
			files = block[DataProvider.FileList]
			sort_inplace(files, key = lambda fi: self.metaKey(block[DataProvider.Metadata], block, fi))
			(fileStack, reprKey) = ([], None)
			for fi in files:
				if reprKey is None:
					reprKey = self.metaKey(block[DataProvider.Metadata], block, fi)
				curKey = self.metaKey(block[DataProvider.Metadata], block, fi)
				if curKey != reprKey:
					yield self.newBlock(block, fileStack)
					(fileStack, reprKey) = ([], curKey)
				fileStack.append(fi)
			yield self.newBlock(block, fileStack)


class UserMetadataSplitter(MetadataSplitter):
	alias = ['metadata']

	def _initConfig(self, config):
		self._metadata = self._configQuery(config.getList, 'split metadata', [])

	def metaKey(self, metadataNames, block, fi):
		selMetadataNames = self._setup(self._metadata, block)
		selMetadataIdx = imap(metadataNames.index, selMetadataNames)
		return tuple(imap(lambda idx: fi[DataProvider.Metadata][idx], selMetadataIdx))
