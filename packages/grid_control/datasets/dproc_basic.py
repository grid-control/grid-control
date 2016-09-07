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

class URLDataProcessor(DataProcessor):
	alias = ['ignore', 'FileDataProcessor']

	def __init__(self, config, datasource_name, onChange):
		DataProcessor.__init__(self, config, datasource_name, onChange)
		internal_config = config.changeView(viewClass = 'SimpleConfigView', setSections = ['dataprocessor'])
		internal_config.set('%s processor' % datasource_name, 'NullDataProcessor')
		config.set('%s ignore urls matcher case sensitive' % datasource_name, 'False')
		self._url_filter = config.getFilter(['%s ignore files' % datasource_name, '%s ignore urls' % datasource_name], '', negate = True,
			filterParser = lambda value: self._parseFilter(internal_config, value),
			filterStr = lambda value: str.join('\n', value.split()),
			defaultMatcher = 'blackwhite', defaultFilter = 'weak',
			onChange = onChange)

	def _parseFilter(self, config, value):
		def getFilterEntries():
			for pat in value.split():
				if ':' not in pat.lstrip(':'):
					yield pat
				else:
					for block in DataProvider.getBlocksFromExpr(config, ':%s' % pat.lstrip(':')):
						for fi in block[DataProvider.FileList]:
							yield fi[DataProvider.URL]
		return str.join('\n', getFilterEntries())

	def enabled(self):
		return self._url_filter.getSelector() is not None

	def process_block(self, block):
		if self.enabled():
			block[DataProvider.FileList] = self._url_filter.filterList(block[DataProvider.FileList], itemgetter(DataProvider.URL))
		return block


class URLCountDataProcessor(DataProcessor):
	alias = ['files', 'FileCountDataProcessor']

	def __init__(self, config, datasource_name, onChange):
		DataProcessor.__init__(self, config, datasource_name, onChange)
		self._limit_files = config.getInt(['%s limit files' % datasource_name, '%s limit urls' % datasource_name], -1, onChange = onChange)

	def enabled(self):
		return self._limit_files != -1

	def process_block(self, block):
		if self.enabled():
			block[DataProvider.NEntries] -= sum(imap(itemgetter(DataProvider.NEntries), block[DataProvider.FileList][self._limit_files:]))
			block[DataProvider.FileList] = block[DataProvider.FileList][:self._limit_files]
			self._limit_files -= len(block[DataProvider.FileList])
		return block


class EntriesCountDataProcessor(DataProcessor):
	alias = ['events', 'EventsCountDataProcessor']

	def __init__(self, config, datasource_name, onChange):
		DataProcessor.__init__(self, config, datasource_name, onChange)
		self._limit_entries = config.getInt(['%s limit events' % datasource_name, '%s limit entries' % datasource_name], -1, onChange = onChange)

	def enabled(self):
		return self._limit_entries != -1

	def process_block(self, block):
		if self.enabled():
			block[DataProvider.NEntries] = 0
			def filterEvents(fi):
				if self._limit_entries == 0: # already got all requested events
					return False
				# truncate file to requested #entries if file has more events than needed
				if fi[DataProvider.NEntries] > self._limit_entries:
					fi[DataProvider.NEntries] = self._limit_entries
				block[DataProvider.NEntries] += fi[DataProvider.NEntries]
				self._limit_entries -= fi[DataProvider.NEntries]
				return True
			block[DataProvider.FileList] = lfilter(filterEvents, block[DataProvider.FileList])
		return block


class EmptyDataProcessor(DataProcessor):
	alias = ['empty']

	def __init__(self, config, datasource_name, onChange):
		DataProcessor.__init__(self, config, datasource_name, onChange)
		self._empty_files = config.getBool('%s remove empty files' % datasource_name, True, onChange = onChange)
		self._empty_block = config.getBool('%s remove empty blocks' % datasource_name, True, onChange = onChange)
		(self._removed_files, self._removed_blocks) = (0, 0)

	def enabled(self):
		return self._empty_block or self._empty_files

	def process_block(self, block):
		if self._empty_files:
			n_files = len(block[DataProvider.FileList])
			block[DataProvider.FileList] = lfilter(lambda fi: fi[DataProvider.NEntries] != 0, block[DataProvider.FileList])
			self._removed_files += n_files - len(block[DataProvider.FileList])
		if self._empty_block:
			if (block[DataProvider.NEntries] == 0) or not block[DataProvider.FileList]:
				self._removed_blocks += 1
				return
		return block

	def _finished(self):
		if self._removed_files or self._removed_blocks:
			self._log.log(logging.INFO1, 'Empty files removed: %d, Empty blocks removed %d', self._removed_files, self._removed_blocks)
		(self._removed_files, self._removed_blocks) = (0, 0)


class LocationDataProcessor(DataProcessor):
	alias = ['location']

	def __init__(self, config, datasource_name, onChange):
		DataProcessor.__init__(self, config, datasource_name, onChange)
		self._location_filter = config.getFilter('%s location filter' % datasource_name, '',
			defaultMatcher = 'blackwhite', defaultFilter = 'strict',
			onChange = onChange)

	def process_block(self, block):
		if block[DataProvider.Locations] is not None:
			sites = self._location_filter.filterList(block[DataProvider.Locations])
			if (sites is not None) and (len(sites) == 0) and (len(block[DataProvider.FileList]) != 0):
				if not len(block[DataProvider.Locations]):
					self._log.warning('Block %s is not available at any site!', DataProvider.bName(block))
				elif not len(sites):
					self._log.warning('Block %s is not available at any selected site!', DataProvider.bName(block))
			block[DataProvider.Locations] = sites
		return block
