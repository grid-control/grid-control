# | Copyright 2016 Karlsruhe Institute of Technology
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

from grid_control.datasets.dproc_base import DataProcessor
from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.utils.data_structures import makeEnum
from python_compat import imap, md5_hex, set

# Enum to specify how to react to multiple occurences of something
DatasetUniqueMode = makeEnum(['warn', 'abort', 'skip', 'ignore', 'record'])
DatasetCheckMode = makeEnum(['warn', 'abort', 'ignore'])

class DataChecker(DataProcessor):
	def _handleError(self, msg, mode):
		if mode == DatasetCheckMode.warn:
			self._log.warning(msg)
		elif mode == DatasetCheckMode.abort:
			raise DatasetError(msg)


class EntriesConsistencyDataProcessor(DataChecker):
	alias = ['consistency']

	def __init__(self, config, onChange):
		DataChecker.__init__(self, config, onChange)
		self._mode = config.getEnum('dataset check entry consistency', DatasetCheckMode,
			DatasetCheckMode.abort, onChange = onChange)

	def enabled(self):
		return self._mode != DatasetCheckMode.ignore

	def processBlock(self, block):
		# Check entry consistency
		events = sum(imap(lambda x: x[DataProvider.NEntries], block[DataProvider.FileList]))
		if block.setdefault(DataProvider.NEntries, events) != events:
			self._handleError('Inconsistency in block %s: Number of events doesn\'t match (b:%d != f:%d)' % (
				DataProvider.bName(block), block[DataProvider.NEntries], events), self._mode)
		return block


class NickNameConsistencyProcessor(DataChecker):
	alias = ['nickconsistency']

	def __init__(self, config, onChange):
		DataChecker.__init__(self, config, onChange)
		# Ensure the same nickname is used consistently in all blocks of a dataset
		self._checkConsistency = config.getEnum('dataset check nickname consistency', DatasetCheckMode,
			DatasetCheckMode.abort, onChange = onChange)
		self._checkConsistencyData = {}
		# Check if two different datasets have the same nickname
		self._checkCollision = config.getEnum('dataset check nickname collision', DatasetCheckMode,
			DatasetCheckMode.abort, onChange = onChange)
		self._checkCollisionData = {}

	def enabled(self):
		return (self._checkConsistency != DatasetCheckMode.ignore) or (self._checkCollision != DatasetCheckMode.ignore)

	# Get nickname and check for collisions
	def processBlock(self, block):
		blockDS = block[DataProvider.Dataset]
		nick = block[DataProvider.Nickname]
		# Check if nickname is used consistenly in all blocks of a datasets
		if self._checkConsistency != DatasetCheckMode.ignore:
			if self._checkConsistencyData.setdefault(blockDS, nick) != nick:
				self._handleError('Different blocks of dataset "%s" have different nicknames: ["%s", "%s"]' % (
					blockDS, self._checkConsistencyData[blockDS], nick), self._checkConsistency)
		if self._checkCollision != DatasetCheckMode.ignore:
			if self._checkCollisionData.setdefault(nick, blockDS) != blockDS:
				self._handleError('Multiple datasets use the same nickname "%s": ["%s", "%s"]' % (
					nick, self._checkCollisionData[nick], blockDS), self._checkCollision)
		return block


class UniqueDataProcessor(DataChecker):
	alias = ['unique']

	def __init__(self, config, onChange):
		DataChecker.__init__(self, config, onChange)
		self._checkURL = config.getEnum('dataset check unique url', DatasetUniqueMode, DatasetUniqueMode.abort, onChange = onChange)
		self._checkBlock = config.getEnum('dataset check unique block', DatasetUniqueMode, DatasetUniqueMode.abort, onChange = onChange)

	def enabled(self):
		return (self._checkURL != DatasetUniqueMode.ignore) or (self._checkBlock != DatasetUniqueMode.ignore)

	def process(self, blockIter):
		self._recordedURL = set()
		self._recordedBlock = set()
		return DataProcessor.process(self, blockIter)

	def processBlock(self, block):
		# Check uniqueness of URLs
		recordedBlockURL = []
		if self._checkURL != DatasetUniqueMode.ignore:
			def processFI(fiList):
				for fi in fiList:
					urlHash = md5_hex(repr((fi[DataProvider.URL], fi[DataProvider.NEntries], fi.get(DataProvider.Metadata))))
					if urlHash in self._recordedURL:
						msg = 'Multiple occurences of URL: %r!' % fi[DataProvider.URL]
						msg += ' (This check can be configured with %r)' % 'dataset check unique url'
						if self._checkURL == DatasetUniqueMode.warn:
							self._log.warning(msg)
						elif self._checkURL == DatasetUniqueMode.abort:
							raise DatasetError(msg)
						elif self._checkURL == DatasetUniqueMode.skip:
							continue
					self._recordedURL.add(urlHash)
					recordedBlockURL.append(urlHash)
					yield fi
			block[DataProvider.FileList] = list(processFI(block[DataProvider.FileList]))
			recordedBlockURL.sort()

		# Check uniqueness of blocks
		if self._checkBlock != DatasetUniqueMode.ignore:
			blockHash = md5_hex(repr((block.get(DataProvider.Dataset), block[DataProvider.BlockName],
				recordedBlockURL, block[DataProvider.NEntries],
				block[DataProvider.Locations], block.get(DataProvider.Metadata))))
			if blockHash in self._recordedBlock:
				msg = 'Multiple occurences of block: "%s"!' % DataProvider.bName(block)
				msg += ' (This check can be configured with %r)' % 'dataset check unique block'
				if self._checkBlock == DatasetUniqueMode.warn:
					self._log.warning(msg)
				elif self._checkBlock == DatasetUniqueMode.abort:
					raise DatasetError(msg)
				elif self._checkBlock == DatasetUniqueMode.skip:
					return None
			self._recordedBlock.add(blockHash)
		return block
