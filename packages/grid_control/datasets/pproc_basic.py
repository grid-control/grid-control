# | Copyright 2015-2017 Karlsruhe Institute of Technology
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
	alias_list = ['basic']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._vn_file_names = config.get(self._get_pproc_opt('variable file names'), 'FILE_NAMES')
		self._fn_list_delim = config.get(self._get_pproc_opt('file names delimeter'), '') or ' '
		self._fn_list_format = config.get(self._get_pproc_opt('file names format'), '%s')
		self._vn_max_events = config.get(self._get_pproc_opt('variable max events'), 'MAX_EVENTS')
		self._vn_skip_events = config.get(self._get_pproc_opt('variable skip events'), 'SKIP_EVENTS')
		self._vn_prefix = config.get(self._get_pproc_opt('variable prefix'), datasource_name.upper())

	def get_needed_vn_list(self, splitter):
		map_splitter_enum2vn = {
			DataSplitter.FileList: self._vn_file_names,
			DataSplitter.NEntries: self._vn_max_events,
			DataSplitter.Skipped: self._vn_skip_events}
		for enum in splitter.get_needed_enums():
			yield map_splitter_enum2vn[enum]

	def get_partition_metadata(self):
		result = lmap(lambda k: ParameterMetadata(k, untracked=True), [
			self._vn_file_names, self._vn_max_events, self._vn_skip_events,
			self._vn_prefix + 'PATH', self._vn_prefix + 'BLOCK', self._vn_prefix + 'NICK'])
		result.append(ParameterMetadata(self._vn_prefix + 'SPLIT', untracked=False))
		return result

	def process(self, pnum, partition_info, result):
		result.update({
			self._vn_file_names: self._format_fn_list(partition_info[DataSplitter.FileList]),
			self._vn_max_events: partition_info[DataSplitter.NEntries],
			self._vn_skip_events: partition_info.get(DataSplitter.Skipped, 0),
			self._vn_prefix + 'PATH': partition_info.get(DataSplitter.Dataset),
			self._vn_prefix + 'BLOCK': partition_info.get(DataSplitter.BlockName),
			self._vn_prefix + 'NICK': partition_info.get(DataSplitter.Nickname),
		})
		if self._datasource_name == 'dataset':
			result[self._vn_prefix + 'SPLIT'] = pnum
		else:
			result[self._vn_prefix + 'SPLIT'] = '%s:%d' % (self._datasource_name, pnum)
		is_invalid = partition_info.get(DataSplitter.Invalid, False)
		result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and not is_invalid

	def _format_fn_list(self, url_list):
		return str.join(self._fn_list_delim, imap(lambda fn: self._fn_list_format % fn, url_list))


class LocationPartitionProcessor(PartitionProcessor):
	alias_list = ['location']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._filter = config.get_filter(self._get_pproc_opt('location filter'),
			default='', default_matcher='BlackWhiteMatcher', default_filter='WeakListFilter')
		self._preference = config.get_list(self._get_pproc_opt('location preference'), [])
		self._reqs = config.get_bool(self._get_pproc_opt('location requirement'), True)
		self._disable = config.get_bool(self._get_pproc_opt('location check'), True)

	def __repr__(self):
		result = self._repr_base('preference = %s, reqs = %s, check = %s' % (
			self._preference, self._reqs, self._disable))
		if self._filter.get_selector():
			result = 'filter = %s ' % self._filter + result
		return result

	def enabled(self):
		return self._filter.get_selector() or self._preference or self._reqs or self._disable

	def process(self, pnum, partition_info, result):
		locations = self._filter.filter_list(partition_info.get(DataSplitter.Locations))
		if self._preference:
			if not locations:  # [] or None
				locations = self._preference
			elif any(imap(self._preference.__contains__, locations)):  # preferred location available
				locations = lfilter(self._preference.__contains__, locations)
		if (partition_info.get(DataSplitter.Locations) is None) and not locations:
			return
		if self._reqs and (locations is not None):
			result[ParameterInfo.REQS].append((WMS.STORAGE, locations))
		if self._disable:
			result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and (locations != [])


class MetaPartitionProcessor(PartitionProcessor):
	alias_list = ['metadata']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._metadata_list = config.get_list(self._get_pproc_opt('metadata'), [])

	def __repr__(self):
		return self._repr_base(str.join(', ', self._metadata_list))

	def enabled(self):
		return self._metadata_list != []

	def get_partition_metadata(self):
		return lmap(lambda k: ParameterMetadata(k, untracked=True), self._metadata_list)

	def process(self, pnum, partition_info, result):
		for idx, metadata_name in enumerate(partition_info.get(DataSplitter.MetadataHeader, [])):
			if metadata_name in self._metadata_list:
				def _get_metadata_protected(metadata_list):
					if idx < len(metadata_list):
						return metadata_list[idx]
				tmp = set(imap(_get_metadata_protected, partition_info[DataSplitter.Metadata]))
				if len(tmp) == 1:
					value = tmp.pop()
					if value is not None:
						result[metadata_name] = value


class RequirementsPartitionProcessor(PartitionProcessor):
	alias_list = ['reqs']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._wt_offset = config.get_float(self._get_pproc_opt('walltime offset'), 0.)
		self._wt_factor = config.get_float(self._get_pproc_opt('walltime factor'), 0.)
		self._ct_offset = config.get_float(self._get_pproc_opt('cputime offset'), 0.)
		self._ct_factor = config.get_float(self._get_pproc_opt('cputime factor'), 0.)
		self._mem_offset = config.get_float(self._get_pproc_opt('memory offset'), 0.)
		self._mem_factor = config.get_float(self._get_pproc_opt('memory factor'), 0.)

	def enabled(self):
		return any(imap(lambda x: x > 0, [self._wt_factor, self._ct_factor, self._mem_factor,
			self._wt_offset, self._ct_offset, self._mem_offset]))

	def process(self, pnum, partition_info, result):
		self._add_requirement(result, partition_info, self._wt_factor, self._wt_offset, WMS.WALLTIME)
		self._add_requirement(result, partition_info, self._ct_factor, self._ct_offset, WMS.CPUTIME)
		self._add_requirement(result, partition_info, self._mem_factor, self._mem_offset, WMS.MEMORY)

	def _add_requirement(self, result, partition_info, factor, offset, enum):
		value = offset
		if factor > 0:
			value += factor * partition_info[DataSplitter.NEntries]
		if value > 0:
			result[ParameterInfo.REQS].append((enum, int(value)))


class TFCPartitionProcessor(PartitionProcessor):
	alias_list = ['tfc']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._tfc = config.get_lookup(self._get_pproc_opt('tfc'), {})

	def __repr__(self):
		return self._repr_base(repr(self._tfc))

	def enabled(self):
		return not self._tfc.empty()

	def process(self, pnum, partition_info, result):
		url_list = partition_info[DataSplitter.FileList]
		locations = partition_info.get(DataSplitter.Locations)
		if not locations:
			partition_info[DataSplitter.FileList] = lmap(lambda url: self._lookup(url, None), url_list)
		else:
			for location in locations:
				partition_info[DataSplitter.FileList] = lmap(lambda url: self._lookup(url, location), url_list)

	def _lookup(self, url, location):
		prefix = self._tfc.lookup(location, is_selector=False)
		if prefix:
			return prefix + url
		return url
