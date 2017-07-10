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

import logging
from grid_control.datasets.dproc_base import DataProcessor
from grid_control.datasets.provider_base import DataProvider
from python_compat import imap, itemgetter, lfilter


class EmptyDataProcessor(DataProcessor):
	alias_list = ['empty']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		self._empty_files = config.get_bool(self._get_dproc_opt('remove empty files'), True)
		self._empty_block = config.get_bool(self._get_dproc_opt('remove empty blocks'), True)
		(self._removed_files, self._removed_blocks) = (0, 0)

	def __repr__(self):
		return self._repr_base('files=%s, blocks=%s' % (self._empty_files, self._empty_block))

	def process_block(self, block):
		if self._empty_files:
			def _has_entries(fi):
				return fi[DataProvider.NEntries] != 0
			n_files = len(block[DataProvider.FileList])
			block[DataProvider.FileList] = lfilter(_has_entries, block[DataProvider.FileList])
			self._removed_files += n_files - len(block[DataProvider.FileList])
		if self._empty_block:
			if (block[DataProvider.NEntries] == 0) or not block[DataProvider.FileList]:
				self._removed_blocks += 1
				return
		return block

	def _enabled(self):
		return self._empty_block or self._empty_files

	def _finished(self):
		if self._removed_files or self._removed_blocks:
			self._log.log(logging.INFO1, 'Empty files removed: %d, Empty blocks removed %d',
				self._removed_files, self._removed_blocks)
		(self._removed_files, self._removed_blocks) = (0, 0)


class EntriesCountDataProcessor(DataProcessor):
	alias_list = ['events', 'EventsCountDataProcessor']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		self._limit_entries = config.get_int(self._get_dproc_opt(['limit events', 'limit entries']), -1)

	def process_block(self, block):
		if self.enabled():
			block[DataProvider.NEntries] = 0

			def _filter_events(fi):
				if self._limit_entries == 0:  # already got all requested events
					return False
				# truncate file to requested #entries if file has more events than needed
				if fi[DataProvider.NEntries] > self._limit_entries:
					fi[DataProvider.NEntries] = self._limit_entries
				block[DataProvider.NEntries] += fi[DataProvider.NEntries]
				self._limit_entries -= fi[DataProvider.NEntries]
				return True
			block[DataProvider.FileList] = lfilter(_filter_events, block[DataProvider.FileList])
		return block

	def _enabled(self):
		return self._limit_entries != -1


class LocationDataProcessor(DataProcessor):
	alias_list = ['location']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		self._location_filter = config.get_filter(self._get_dproc_opt('location filter'), '',
			default_matcher='BlackWhiteMatcher', default_filter='StrictListFilter')

	def process_block(self, block):
		if block[DataProvider.Locations] is not None:
			sites = self._location_filter.filter_list(block[DataProvider.Locations])
			if (sites is not None) and (len(sites) == 0) and (len(block[DataProvider.FileList]) != 0):
				error_msg = 'Block %s is not available ' % DataProvider.get_block_id(block)
				if not len(block[DataProvider.Locations]):
					self._log.warning(error_msg + 'at any site!')
				elif not len(sites):
					self._log.warning(error_msg + 'at any selected site!')
			block[DataProvider.Locations] = sites
		return block


class URLCountDataProcessor(DataProcessor):
	alias_list = ['files', 'FileCountDataProcessor']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		self._limit_files = config.get_int(self._get_dproc_opt(['limit files', 'limit urls']), -1)
		self._limit_files_fraction = config.get_float(
			self._get_dproc_opt(['limit files fraction', 'limit urls fraction']), -1.)
		(self._limit_files_per_ds, self._files_per_ds) = ({}, {})

	def process(self, block_iter):
		(self._limit_files_per_ds, self._files_per_ds) = ({}, {})  # reset counters
		if self._limit_files_fraction >= 0:
			block_list = list(DataProcessor.process(self, block_iter))
			goal_per_ds = {}  # calculate file limit per dataset
			for (dataset_name, fn_list_len) in self._files_per_ds.items():
				goal_per_ds[dataset_name] = int(self._limit_files_fraction * fn_list_len) or 1
			for block in block_list:
				self._reduce_fn_list(block, goal_per_ds)
				yield block
		else:
			for block in DataProcessor.process(self, block_iter):
				yield block

	def process_block(self, block):
		if self._limit_files >= 0:  # truncate the number of files
			self._limit_files_per_ds.setdefault(block[DataProvider.Dataset], self._limit_files)
			self._reduce_fn_list(block, self._limit_files_per_ds)
		if self._limit_files_fraction >= 0:  # count the number of files per dataset
			self._files_per_ds.setdefault(block[DataProvider.Dataset], 0)
			self._files_per_ds[block[DataProvider.Dataset]] += len(block[DataProvider.FileList])
		return block

	def _enabled(self):
		return (self._limit_files >= 0) or (self._limit_files_fraction >= 0)

	def _reduce_fn_list(self, block, fn_list_limit_map):
		dataset_name = block[DataProvider.Dataset]
		fn_list_limit = fn_list_limit_map[dataset_name]
		fi_list_removed = block[DataProvider.FileList][fn_list_limit:]
		nentry_removed_iter = imap(itemgetter(DataProvider.NEntries), fi_list_removed)
		block[DataProvider.NEntries] -= sum(nentry_removed_iter)
		block[DataProvider.FileList] = block[DataProvider.FileList][:fn_list_limit]
		fn_list_limit_map[dataset_name] -= len(block[DataProvider.FileList])


class URLDataProcessor(DataProcessor):
	alias_list = ['ignore', 'FileDataProcessor']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		config.set('%s ignore urls matcher case sensitive' % datasource_name, 'False')
		self._url_filter = config.get_filter(self._get_dproc_opt(['ignore files', 'ignore urls']),
			'', negate=True, default_matcher='BlackWhiteMatcher', default_filter='WeakListFilter',
			filter_parser=lambda value: self._parse_filter(config, value),
			filter_str=lambda value: str.join('\n', value.split()))

	def process_block(self, block):
		if self.enabled():
			block[DataProvider.FileList] = self._url_filter.filter_list(block[DataProvider.FileList],
				itemgetter(DataProvider.URL))
		return block

	def _enabled(self):
		return self._url_filter.get_selector() is not None

	def _parse_filter(self, config, value):
		dataset_proc = DataProcessor.create_instance('NullDataProcessor')

		def _get_filter_entries():
			for pat in value.split():
				if ':' not in pat.lstrip(':'):
					yield pat
				else:
					block_iter = DataProvider.iter_blocks_from_expr(config, ':%s' % pat.lstrip(':'),
						dataset_proc=dataset_proc)
					for block in block_iter:
						for fi in block[DataProvider.FileList]:
							yield fi[DataProvider.URL]
		return str.join('\n', _get_filter_entries())
