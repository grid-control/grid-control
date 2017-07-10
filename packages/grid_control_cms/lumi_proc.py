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

from grid_control.datasets import DataProcessor, DataProvider, DataSplitter, DatasetError, PartitionProcessor  # pylint:disable=line-too-long
from grid_control.parameters import ParameterMetadata
from grid_control.utils.algos import safe_index
from grid_control.utils.data_structures import make_enum
from grid_control_cms.lumi_tools import filter_lumi_filter, format_lumi, parse_lumi_filter, select_lumi, select_run, str_lumi  # pylint:disable=line-too-long
from python_compat import any, ichain, imap, izip, set


LumiKeep = make_enum(['RunLumi', 'Run', 'none'])  # pylint:disable=invalid-name
LumiMode = make_enum(['strict', 'weak'])  # pylint:disable=invalid-name


class LumiDataProcessor(DataProcessor):
	alias_list = ['lumi']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		self._lumi_filter = config.get_lookup(['lumi filter', '%s lumi filter' % datasource_name],
			default={}, parser=parse_lumi_filter, strfun=str_lumi)
		if self._lumi_filter.empty():
			lumi_keep_default = LumiKeep.none
		else:
			lumi_keep_default = LumiKeep.Run
			config.set_bool('%s lumi metadata' % datasource_name, True)
			self._log.info('Runs/lumi section filter enabled!')
		self._lumi_keep = config.get_enum(['lumi keep', '%s lumi keep' % datasource_name],
			LumiKeep, lumi_keep_default)
		self._lumi_strict = config.get_enum(
			['lumi filter strictness', '%s lumi filter strictness' % datasource_name],
			LumiMode, LumiMode.strict)

	def __repr__(self):
		return self._repr_base('filter=%s, keep=%s, strict=%s' % (self._lumi_filter,
			LumiKeep.enum2str(self._lumi_keep), LumiMode.enum2str(self._lumi_strict)))

	def process_block(self, block):
		if self._lumi_filter.empty():
			if (self._lumi_keep == LumiKeep.RunLumi) or (DataProvider.Metadata not in block):
				return block
		idx_runs = safe_index(block.get(DataProvider.Metadata, []), 'Runs')
		idx_lumi = safe_index(block.get(DataProvider.Metadata, []), 'Lumi')
		if not self._lumi_filter.empty():
			self._check_lumi_filter(block, idx_runs, idx_lumi)

		block[DataProvider.FileList] = list(self._process_fi(block, idx_runs, idx_lumi))
		if not block[DataProvider.FileList]:
			return
		block[DataProvider.NEntries] = sum(imap(lambda fi: fi[DataProvider.NEntries],
			block[DataProvider.FileList]))
		# Prune metadata
		if self._lumi_keep == LumiKeep.RunLumi:
			return block
		elif self._lumi_keep == LumiKeep.Run:
			idx_runs = None
		_remove_run_lumi(block[DataProvider.Metadata], idx_runs, idx_lumi)
		return block

	def _accept_lumi(self, block, fi, idx_runs, idx_lumi, lumi_filter):
		if (idx_runs is None) or (idx_lumi is None):
			return True
		return any(imap(lambda run_lumi: select_lumi(run_lumi, lumi_filter),
			izip(fi[DataProvider.Metadata][idx_runs], fi[DataProvider.Metadata][idx_lumi])))

	def _accept_run(self, block, fi, idx_runs, lumi_filter):
		if idx_runs is None:
			return True
		return any(imap(lambda run: select_run(run, lumi_filter), fi[DataProvider.Metadata][idx_runs]))

	def _check_lumi_filter(self, block, idx_runs, idx_lumi):
		lumi_filter = self._lumi_filter.lookup(block[DataProvider.Nickname], is_selector=False)
		if not lumi_filter:
			return
		if (self._lumi_strict == LumiMode.strict) and ((idx_runs is None) or (idx_lumi is None)):
			raise DatasetError('Strict lumi filter active but ' +
				'dataset %s does not provide lumi information!' % DataProvider.get_block_id(block))
		elif (self._lumi_strict == LumiMode.weak) and (idx_runs is None):
			raise DatasetError('Weak lumi filter active but ' +
				'dataset %s does not provide run information!' % DataProvider.get_block_id(block))

	def _process_fi(self, block, idx_runs, idx_lumi):
		for fi in block[DataProvider.FileList]:
			if self._skip_fi(block, idx_runs, idx_lumi, fi):
				continue
			# Prune metadata
			if (self._lumi_keep == LumiKeep.Run) and (idx_lumi is not None):
				if idx_runs is not None:
					fi[DataProvider.Metadata][idx_runs] = list(set(fi[DataProvider.Metadata][idx_runs]))
				fi[DataProvider.Metadata].pop(idx_lumi)
			elif self._lumi_keep == LumiKeep.none:
				_remove_run_lumi(fi[DataProvider.Metadata], idx_runs, idx_lumi)
			yield fi

	def _skip_fi(self, block, idx_runs, idx_lumi, fi):
		if not self._lumi_filter.empty():  # Filter files by run / lumi
			lumi_filter = self._lumi_filter.lookup(block[DataProvider.Nickname], is_selector=False)
			if self._lumi_strict == LumiMode.strict:
				if not self._accept_lumi(block, fi, idx_runs, idx_lumi, lumi_filter):
					return True
			elif self._lumi_strict == LumiMode.weak:
				if not self._accept_run(block, fi, idx_runs, lumi_filter):
					return True


class LumiPartitionProcessor(PartitionProcessor):
	alias_list = ['lumi']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		self._lumi_filter = config.get_lookup(['lumi filter', '%s lumi filter' % datasource_name],
			default={}, parser=parse_lumi_filter, strfun=str_lumi)

	def __repr__(self):
		return self._repr_base(str(self._lumi_filter))

	def enabled(self):
		return not self._lumi_filter.empty()

	def get_needed_vn_list(self, splitter):
		if self.enabled():
			return ['LUMI_RANGE']

	def get_partition_metadata(self):
		if self.enabled():
			return [ParameterMetadata('LUMI_RANGE', untracked=True)]

	def process(self, pnum, partition, result):
		if self.enabled():
			lumi_filter = self._lumi_filter.lookup(partition[DataSplitter.Nickname], is_selector=False)
			if lumi_filter:
				idx_runs = partition[DataSplitter.MetadataHeader].index('Runs')
				iter_run = ichain(imap(lambda m: m[idx_runs], partition[DataSplitter.Metadata]))
				short_lumi_filter = filter_lumi_filter(list(iter_run), lumi_filter)
				iter_lumi_range_str = imap(lambda lr: '"%s"' % lr, format_lumi(short_lumi_filter))
				result['LUMI_RANGE'] = str.join(',', iter_lumi_range_str)


def _remove_run_lumi(value, idx_runs, idx_lumi):
	if (idx_runs is not None) and (idx_lumi is not None):
		value.pop(max(idx_runs, idx_lumi))
		value.pop(min(idx_runs, idx_lumi))
	elif idx_lumi is not None:
		value.pop(idx_lumi)
	elif idx_runs is not None:
		value.pop(idx_runs)
