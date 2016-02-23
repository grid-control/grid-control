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
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.parameters import ParameterInfo, ParameterMetadata
from grid_control.utils.gc_itertools import lchain
from hpfwk import AbstractError
from python_compat import any, imap, lfilter, lmap, set

# Class used by DataParameterSource to convert dataset splittings into parameter data
class PartitionProcessor(ConfigurablePlugin):
	def getKeys(self):
		raise AbstractError

	def process(self, pNum, splitInfo, result):
		raise AbstractError


class MultiPartitionProcessor(PartitionProcessor):
	def __init__(self, config, processorList):
		PartitionProcessor.__init__(self, config)
		self._processorList = processorList

	def getKeys(self):
		return lchain(imap(lambda p: p.getKeys(), self._processorList))

	def process(self, pNum, splitInfo, result):
		for processor in self._processorList:
			processor.process(pNum, splitInfo, result)


class BasicPartitionProcessor(PartitionProcessor):
	def _formatFileList(self, fl):
		return str.join(' ', fl)

	def getKeys(self):
		result = lmap(lambda k: ParameterMetadata(k, untracked = True), ['FILE_NAMES', 'MAX_EVENTS',
			'SKIP_EVENTS', 'DATASETID', 'DATASETPATH', 'DATASETBLOCK', 'DATASETNICK'])
		result.append(ParameterMetadata('DATASETSPLIT', untracked = False))
		return result

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
		if self._filter == []:
			self._filter = None
		self._preference = config.getList('partition location preference', [], onChange = None)
		self._reqs = config.getBool('partition location requirement', True, onChange = None)
		self._disable = config.getBool('partition location check', True, onChange = None)

	def getKeys(self):
		return []

	def process(self, pNum, splitInfo, result):
		locations = splitInfo.get(DataSplitter.Locations)
#		if locations is not None:
#			locations = filterBlackWhite(locations, self._filter, addUnmatched = True)
		if self._preference:
			if not locations: # [] or None
				locations = self._preference
			elif any(imap(lambda x: x in self._preference, locations)): # preferred location available
				locations = lfilter(lambda x: x in self._preference, locations)
		if self._reqs and (locations is not None):
			result[ParameterInfo.REQS].append((WMS.STORAGE, locations))
		if self._disable:
			result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and (locations != [])


class MetaPartitionProcessor(PartitionProcessor):
	def __init__(self, config):
		PartitionProcessor.__init__(self, config)
		self._metadata = config.getList('partition metadata', [])

	def getKeys(self):
		return lmap(lambda k: ParameterMetadata(k, untracked=True), self._metadata)

	def process(self, pNum, splitInfo, result):
		for idx, mkey in enumerate(splitInfo.get(DataSplitter.MetadataHeader, [])):
			if mkey in self._metadata:
				tmp = set(imap(lambda x: x[idx], splitInfo[DataSplitter.Metadata]))
				if len(tmp) == 1:
					result[mkey] = tmp.pop()
