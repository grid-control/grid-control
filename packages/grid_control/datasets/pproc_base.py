#-#  Copyright 2015-2016 Karlsruhe Institute of Technology
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

from grid_control.backends import WMS
from grid_control.datasets.splitter_base import DataSplitter
from grid_control.parameters import ParameterInfo, ParameterMetadata
from grid_control.utils import filterBlackWhite
from hpfwk import Plugin
from python_compat import any, set

# Class used by DataParameterSource to convert dataset splittings into parameter data
class PartitionProcessor(Plugin):
	def __init__(self, config):
		pass

	def getKeys(self):
		raise AbstractError

	def process(self, pNum, splitInfo, result):
		raise AbstractError


class MultiPartitionProcessor(PartitionProcessor):
	def __init__(self, processorProxyList, config):
		PartitionProcessor.__init__(self, config)
		self._processorList = map(lambda p: p.getInstance(config), processorProxyList)

	def getKeys(self):
		return reduce(list.__add__, map(lambda p: p.getKeys(), self._processorList), [])

	def process(self, pNum, splitInfo, result):
		for processor in self._processorList:
			processor.process(pNum, splitInfo, result)


class BasicPartitionProcessor(PartitionProcessor):
	def _formatFileList(self, fl):
		return str.join(' ', fl)

	def getKeys(self):
		return map(lambda k: ParameterMetadata(k, untracked=True), ['FILE_NAMES', 'MAX_EVENTS',
			'SKIP_EVENTS', 'DATASETID', 'DATASETPATH', 'DATASETBLOCK', 'DATASETNICK'])

	def process(self, pNum, splitInfo, result):
		result.update({
			'FILE_NAMES': self._formatFileList(splitInfo[DataSplitter.FileList]),
			'MAX_EVENTS': splitInfo[DataSplitter.NEntries],
			'SKIP_EVENTS': splitInfo.get(DataSplitter.Skipped, 0),
			'DATASETID': splitInfo.get(DataSplitter.DatasetID, None),
			'DATASETPATH': splitInfo.get(DataSplitter.Dataset, None),
			'DATASETBLOCK': splitInfo.get(DataSplitter.BlockName, None),
			'DATASETNICK': splitInfo.get(DataSplitter.Nickname, None),
			'DATASETSPLIT': pNum,
		})
		result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and not splitInfo.get(DataSplitter.Invalid, False)


class LocationPartitionProcessor(PartitionProcessor):
	def __init__(self, config):
		PartitionProcessor.__init__(self, config)
		self._filter = config.getList('partition location filter', [], onChange = None)
		self._preference = config.getList('partition location preference', [], onChange = None)
		self._reqs = config.getBool('partition location requirement', True, onChange = None)
		self._disable = config.getBool('partition location check', True, onChange = None)

	def getKeys(self):
		return []

	def process(self, pNum, splitInfo, result):
		locations = splitInfo.get(DataSplitter.Locations)
		if locations is not None:
			locations = filterBlackWhite(locations, self._filter, addUnmatched = True)
		if self._preference:
			if not locations: # [] or None
				locations = self._preference
			elif any(map(lambda x: x in self._preference, locations)): # preferred location available
				locations = filter(lambda x: x in self._preference, locations)
		if self._reqs and (locations is not None):
			result[ParameterInfo.REQS].append((WMS.STORAGE, locations))
		if self._disable:
			result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and (locations != [])


class MetaPartitionProcessor(PartitionProcessor):
	def __init__(self, config):
		PartitionProcessor.__init__(self, config)
		self._metadata = config.getList('partition metadata', [])

	def getKeys(self):
		return map(lambda k: ParameterMetadata(k, untracked=True), self._metadata)

	def process(self, pNum, splitInfo, result):
		for idx, mkey in enumerate(splitInfo.get(DataSplitter.MetadataHeader, [])):
			if mkey in self._metadata:
				tmp = set(map(lambda x: x[idx], splitInfo[DataSplitter.Metadata]))
				if len(tmp) == 1:
					result[mkey] = tmp.pop()
