#-#  Copyright 2015 Karlsruhe Institute of Technology
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
from grid_control.parameters.psource_base import ParameterInfo, ParameterMetadata
from hpfwk import Plugin
from python_compat import set

# Class used by DataParameterSource to convert dataset splittings into parameter data
class DataSplitProcessor(Plugin):
	def __init__(self, config):
		pass

	def getKeys(self):
		raise AbstractError

	def process(self, pNum, splitInfo, result):
		raise AbstractError


class MultiDataSplitProcessor(DataSplitProcessor):
	def __init__(self, processorProxyList, config):
		DataSplitProcessor.__init__(self, config)
		self._processorList = map(lambda p: p.getInstance(config), processorProxyList)

	def getKeys(self):
		return reduce(list.__add__, map(lambda p: p.getKeys(), self._processorList), [])

	def process(self, pNum, splitInfo, result):
		for processor in self._processorList:
			processor.process(pNum, splitInfo, result)


class BasicDataSplitProcessor(DataSplitProcessor):
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
		if splitInfo.get(DataSplitter.Locations) != None:
			result[ParameterInfo.REQS].append((WMS.STORAGE, splitInfo.get(DataSplitter.Locations)))
		result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and not splitInfo.get(DataSplitter.Invalid, False)


class SECheckSplitProcessor(DataSplitProcessor):
	def __init__(self, config):
		DataSplitProcessor.__init__(self, config)
		self._checkSE = config.getBool('dataset storage check', True, onChange = None)

	def getKeys(self):
		return []

	def process(self, pNum, splitInfo, result):
		if self._checkSE:
			result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and (splitInfo.get(DataSplitter.Locations) != [])


class MetadataSplitProcessor(DataSplitProcessor):
	def __init__(self, config):
		DataSplitProcessor.__init__(self, config)
		self._metadata = config.getList('dataset metadata', [])

	def getKeys(self):
		return map(lambda k: ParameterMetadata(k, untracked=True), self._metadata)

	def process(self, pNum, splitInfo, result):
		for idx, mkey in enumerate(splitInfo[DataSplitter.MetadataHeader]):
			if mkey in self._metadata:
				tmp = set(map(lambda x: x[idx], splitInfo[DataSplitter.Metadata]))
				if len(tmp) == 1:
					result[mkey] = tmp.pop()
