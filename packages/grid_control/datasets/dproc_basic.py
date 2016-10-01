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
from grid_control.datasets.dproc_base import DataProcessor
from grid_control.datasets.provider_base import DataProvider
from python_compat import imap, itemgetter, lfilter


class EmptyDataProcessor(DataProcessor):
	alias_list = ['empty']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		self._empty_files = config.get_bool('%s remove empty files' % datasource_name, True)
		self._empty_block = config.get_bool('%s remove empty blocks' % datasource_name, True)
		(self._removed_files, self._removed_blocks) = (0, 0)

	def enabled(self):
		return self._empty_block or self._empty_files

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

	def _finished(self):
		if self._removed_files or self._removed_blocks:
			self._log.log(logging.INFO1, 'Empty files removed: %d, Empty blocks removed %d',
				self._removed_files, self._removed_blocks)
		(self._removed_files, self._removed_blocks) = (0, 0)


class EntriesCountDataProcessor(DataProcessor):
	alias_list = ['events', 'EventsCountDataProcessor']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		self._limit_entries = config.get_int(
			['%s limit events' % datasource_name, '%s limit entries' % datasource_name], -1)

	def enabled(self):
		return self._limit_entries != -1

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


class LocationDataProcessor(DataProcessor):
	alias_list = ['location']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		self._location_filter = config.get_filter('%s location filter' % datasource_name, '',
			default_matcher='blackwhite', default_filter='strict')

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
		self._limit_files = config.get_int(
			['%s limit files' % datasource_name, '%s limit urls' % datasource_name], -1)

	def enabled(self):
		return self._limit_files != -1

	def process_block(self, block):
		if self.enabled():
			fi_list_removed = block[DataProvider.FileList][self._limit_files:]
			nentry_removed_iter = imap(itemgetter(DataProvider.NEntries), fi_list_removed)
			block[DataProvider.NEntries] -= sum(nentry_removed_iter)
			block[DataProvider.FileList] = block[DataProvider.FileList][:self._limit_files]
			self._limit_files -= len(block[DataProvider.FileList])
		return block


class URLDataProcessor(DataProcessor):
	alias_list = ['ignore', 'FileDataProcessor']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		internal_config = config.change_view(view_class='SimpleConfigView', setSections=['dataprocessor'])
		internal_config.set('%s processor' % datasource_name, 'NullDataProcessor')
		config.set('%s ignore urls matcher case sensitive' % datasource_name, 'False')
		self._url_filter = config.get_filter(
			['%s ignore files' % datasource_name, '%s ignore urls' % datasource_name], '', negate=True,
			filter_parser=lambda value: self._parse_filter(internal_config, value),
			filter_str=lambda value: str.join('\n', value.split()),
			default_matcher='blackwhite', default_filter='weak')

	def enabled(self):
		return self._url_filter.get_selector() is not None

	def process_block(self, block):
		if self.enabled():
			block[DataProvider.FileList] = self._url_filter.filter_list(block[DataProvider.FileList],
				itemgetter(DataProvider.URL))
		return block

	def _parse_filter(self, config, value):
		def _get_filter_entries():
			for pat in value.split():
				if ':' not in pat.lstrip(':'):
					yield pat
				else:
					for block in DataProvider.iter_blocks_from_expr(config, ':%s' % pat.lstrip(':')):
						for fi in block[DataProvider.FileList]:
							yield fi[DataProvider.URL]
		return str.join('\n', _get_filter_entries())
