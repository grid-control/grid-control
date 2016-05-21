# | Copyright 2015-2016 Karlsruhe Institute of Technology
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

import logging
from grid_control.backends import WMS
from grid_control.datasets.splitter_base import DataSplitter
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.parameters import ParameterInfo, ParameterMetadata
from grid_control.utils import display_selection
from grid_control.utils.gc_itertools import lchain
from hpfwk import AbstractError
from python_compat import any, imap, lfilter, lmap, set

# Class used by DataParameterSource to convert dataset splittings into parameter data
class PartitionProcessor(ConfigurablePlugin):
	def __init__(self, config):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('partproc')

	def enabled(self):
		return True

	def getKeys(self):
		return []

	def getNeededKeys(self, splitter):
		return []

	def process(self, pNum, splitInfo, result):
		raise AbstractError


class MultiPartitionProcessor(PartitionProcessor):
	def __init__(self, config, processorList):
		PartitionProcessor.__init__(self, config)
		(self._processorList, processorNames) = ([], [])
		for proc in processorList:
			if proc.enabled() and proc.__class__.__name__ not in processorNames:
				self._processorList.append(proc)
				processorNames.append(proc.__class__.__name__)
		display_selection(self._log, processorList, self._processorList,
			'Removed %d inactive partition processors!', lambda item: item.__class__.__name__)

	def getKeys(self):
		return lchain(imap(lambda p: p.getKeys(), self._processorList))

	def getNeededKeys(self, splitter):
		return lchain(imap(lambda p: p.getNeededKeys(splitter), self._processorList))

	def process(self, pNum, splitInfo, result):
		for processor in self._processorList:
			processor.process(pNum, splitInfo, result)


class BasicPartitionProcessor(PartitionProcessor):
	alias = ['basic']

	def _formatFileList(self, fl):
		return str.join(' ', fl)

	def getKeys(self):
		result = lmap(lambda k: ParameterMetadata(k, untracked = True), ['FILE_NAMES', 'MAX_EVENTS',
			'SKIP_EVENTS', 'DATASETID', 'DATASETPATH', 'DATASETBLOCK', 'DATASETNICK'])
		result.append(ParameterMetadata('DATASETSPLIT', untracked = False))
		return result

	def getNeededKeys(self, splitter):
		enumMap = {
			DataSplitter.FileList: 'FILE_NAMES',
			DataSplitter.NEntries: 'MAX_EVENTS',
			DataSplitter.Skipped: 'SKIP_EVENTS'}
		for enum in splitter.neededEnums():
			yield enumMap[enum]

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
	alias = ['location']

	def __init__(self, config):
		PartitionProcessor.__init__(self, config)
		self._filter = config.getFilter('partition location filter', '', onChange = None,
			defaultMatcher = 'blackwhite', defaultFilter = 'weak')
		self._preference = config.getList('partition location preference', [], onChange = None)
		self._reqs = config.getBool('partition location requirement', True, onChange = None)
		self._disable = config.getBool('partition location check', True, onChange = None)

	def enabled(self):
		return self._filter.getSelector() or self._preference or self._reqs or self._disable

	def process(self, pNum, splitInfo, result):
		locations = self._filter.filterList(splitInfo.get(DataSplitter.Locations))
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
	alias = ['metadata']

	def __init__(self, config):
		PartitionProcessor.__init__(self, config)
		self._metadata = config.getList('partition metadata', [], onChange = None)

	def getKeys(self):
		return lmap(lambda k: ParameterMetadata(k, untracked=True), self._metadata)

	def enabled(self):
		return self._metadata != []

	def process(self, pNum, splitInfo, result):
		for idx, mkey in enumerate(splitInfo.get(DataSplitter.MetadataHeader, [])):
			if mkey in self._metadata:
				def getMetadataProtected(x):
					if idx < len(x):
						return x[idx]
				tmp = set(imap(getMetadataProtected, splitInfo[DataSplitter.Metadata]))
				if len(tmp) == 1:
					value = tmp.pop()
					if value is not None:
						result[mkey] = value


class TFCPartitionProcessor(PartitionProcessor):
	alias = ['tfc']

	def __init__(self, config):
		PartitionProcessor.__init__(self, config)
		self._tfc = config.getLookup('partition tfc', {}, onChange = None)

	def enabled(self):
		return not self._tfc.empty()

	def _lookup(self, fn, location):
		prefix = self._tfc.lookup(location, is_selector = False)
		if prefix:
			return prefix + fn
		return fn

	def process(self, pNum, splitInfo, result):
		fl = splitInfo[DataSplitter.FileList]
		locations = splitInfo.get(DataSplitter.Locations)
		if not locations:
			splitInfo[DataSplitter.FileList] = lmap(lambda fn: self._lookup(fn, None), fl)
		else:
			for location in locations:
				splitInfo[DataSplitter.FileList] = lmap(lambda fn: self._lookup(fn, location), fl)
