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

from grid_control.backends import WMS
from grid_control.datasets.pproc_base import PartitionProcessor
from grid_control.datasets.splitter_base import DataSplitter
from grid_control.parameters import ParameterInfo, ParameterMetadata
from python_compat import any, imap, lfilter, lmap, set

class BasicPartitionProcessor(PartitionProcessor):
	alias = ['basic']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._vn_file_names = config.get(['partition variable file names', '%s partition variable file names' % datasource_name], 'FILE_NAMES', onChange = None)
		self._vn_max_events = config.get(['partition variable max events', '%s partition variable max events' % datasource_name], 'MAX_EVENTS', onChange = None)
		self._vn_skip_events = config.get(['partition variable skip events', '%s partition variable skip events' % datasource_name], 'SKIP_EVENTS', onChange = None)
		self._vn_prefix = config.get(['partition variable prefix', '%s partition variable prefix' % datasource_name], 'DATASET', onChange = None)

	def _formatFileList(self, fl):
		return str.join(' ', fl)

	def getKeys(self):
		result = lmap(lambda k: ParameterMetadata(k, untracked = True), [
			self._vn_file_names, self._vn_max_events, self._vn_skip_events,
			self._vn_prefix + 'PATH', self._vn_prefix + 'BLOCK', self._vn_prefix + 'NICK'])
		result.append(ParameterMetadata(self._vn_prefix + 'SPLIT', untracked = False))
		return result

	def getNeededKeys(self, splitter):
		enumMap = {
			DataSplitter.FileList: self._vn_file_names,
			DataSplitter.NEntries: self._vn_max_events,
			DataSplitter.Skipped: self._vn_skip_events}
		for enum in splitter.neededEnums():
			yield enumMap[enum]

	def process(self, pNum, splitInfo, result):
		result.update({
			self._vn_file_names: self._formatFileList(splitInfo[DataSplitter.FileList]),
			self._vn_max_events: splitInfo[DataSplitter.NEntries],
			self._vn_skip_events: splitInfo.get(DataSplitter.Skipped, 0),
			self._vn_prefix + 'PATH': splitInfo.get(DataSplitter.Dataset, None),
			self._vn_prefix + 'BLOCK': splitInfo.get(DataSplitter.BlockName, None),
			self._vn_prefix + 'NICK': splitInfo.get(DataSplitter.Nickname, None),
		})
		if self._datasource_name == 'dataset':
			result[self._vn_prefix + 'SPLIT'] = pNum
		else:
			result[self._vn_prefix + 'SPLIT'] = '%s:%d' % (self._datasource_name, pNum)
		result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and not splitInfo.get(DataSplitter.Invalid, False)


class LocationPartitionProcessor(PartitionProcessor):
	alias = ['location']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._filter = config.getFilter(['partition location filter', '%s partition location filter' % datasource_name],
			'', onChange = None, defaultMatcher = 'blackwhite', defaultFilter = 'weak')
		self._preference = config.getList(['partition location preference', '%s partition location preference' % datasource_name], [], onChange = None)
		self._reqs = config.getBool(['partition location requirement', '%s partition location requirement' % datasource_name], True, onChange = None)
		self._disable = config.getBool(['partition location check', '%s partition location check' % datasource_name], True, onChange = None)

	def enabled(self):
		return self._filter.getSelector() or self._preference or self._reqs or self._disable

	def process(self, pNum, splitInfo, result):
		locations = self._filter.filterList(splitInfo.get(DataSplitter.Locations))
		if self._preference:
			if not locations: # [] or None
				locations = self._preference
			elif any(imap(lambda x: x in self._preference, locations)): # preferred location available
				locations = lfilter(lambda x: x in self._preference, locations)
		if (splitInfo.get(DataSplitter.Locations) is None) and not locations:
			return
		if self._reqs and (locations is not None):
			result[ParameterInfo.REQS].append((WMS.STORAGE, locations))
		if self._disable:
			result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and (locations != [])


class MetaPartitionProcessor(PartitionProcessor):
	alias = ['metadata']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._metadata = config.getList(['partition metadata', '%s partition metadata' % datasource_name],
			[], onChange = None)

	def getKeys(self):
		return lmap(lambda k: ParameterMetadata(k, untracked = True), self._metadata)

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

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._tfc = config.getLookup(['partition tfc', '%s partition tfc' % datasource_name], {}, onChange = None)

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


class RequirementsPartitionProcessor(PartitionProcessor):
	alias = ['reqs']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._wtfactor = config.getFloat(['partition walltime factor', '%s partition walltime factor' % datasource_name], -1., onChange = None)
		self._wtoffset = config.getFloat(['partition walltime offset', '%s partition walltime offset' % datasource_name], 0., onChange = None)
		self._ctfactor = config.getFloat(['partition cputime factor', '%s partition cputime factor' % datasource_name], -1., onChange = None)
		self._ctoffset = config.getFloat(['partition cputime offset', '%s partition cputime offset' % datasource_name], 0., onChange = None)
		self._memfactor = config.getFloat(['partition memory factor', '%s partition memory factor' % datasource_name], -1., onChange = None)
		self._memoffset = config.getFloat(['partition memory offset', '%s partition memory offset' % datasource_name], 0., onChange = None)

	def enabled(self):
		return any(imap(lambda x: x > 0, [self._wtfactor, self._ctfactor, self._memfactor,
			self._wtoffset, self._ctoffset, self._memoffset]))

	def _addReq(self, result, splitInfo, factor, offset, enum):
		value = offset
		if factor > 0:
			value += factor * splitInfo[DataSplitter.NEntries]
		if value > 0:
			result[ParameterInfo.REQS].append((enum, int(value)))

	def process(self, pNum, splitInfo, result):
		self._addReq(result, splitInfo, self._wtfactor, self._wtoffset, WMS.WALLTIME)
		self._addReq(result, splitInfo, self._ctfactor, self._ctoffset, WMS.CPUTIME)
		self._addReq(result, splitInfo, self._memfactor, self._memoffset, WMS.MEMORY)
