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
from python_compat import itemgetter, lfilter

class URLDataProcessor(DataProcessor):
	alias = ['ignore', 'FileDataProcessor']

	def __init__(self, config, onChange):
		DataProcessor.__init__(self, config, onChange)
		internal_config = config.changeView(viewClass = 'SimpleConfigView', setSections = ['dataprocessor'])
		internal_config.set('dataset processor', 'NullDataProcessor')
		config.set('dataset ignore urls matcher case sensitive', 'False')
		self._url_filter = config.getFilter(['dataset ignore files', 'dataset ignore urls'], '', negate = True,
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

	def processBlock(self, block):
		if self.enabled():
			block[DataProvider.FileList] = self._url_filter.filterList(block[DataProvider.FileList], itemgetter(DataProvider.URL))
		return block


class URLCountDataProcessor(DataProcessor):
	alias = ['files', 'FileCountDataProcessor']

	def __init__(self, config, onChange):
		DataProcessor.__init__(self, config, onChange)
		self._limitFiles = config.getInt(['dataset limit files', 'dataset limit urls'], -1,
			onChange = onChange)

	def enabled(self):
		return self._limitFiles != -1

	def processBlock(self, block):
		if self.enabled():
			block[DataProvider.FileList] = block[DataProvider.FileList][:self._limitFiles]
			self._limitFiles -= len(block[DataProvider.FileList])
		return block


class EntriesCountDataProcessor(DataProcessor):
	alias = ['events', 'EventsCountDataProcessor']

	def __init__(self, config, onChange):
		DataProcessor.__init__(self, config, onChange)
		self._limitEntries = config.getInt(['dataset limit events', 'dataset limit entries'], -1,
			onChange = onChange)

	def enabled(self):
		return self._limitEntries != -1

	def processBlock(self, block):
		if self.enabled():
			block[DataProvider.NEntries] = 0
			def filterEvents(fi):
				if self._limitEntries == 0: # already got all requested events
					return False
				# truncate file to requested #entries if file has more events than needed
				if fi[DataProvider.NEntries] > self._limitEntries:
					fi[DataProvider.NEntries] = self._limitEntries
				block[DataProvider.NEntries] += fi[DataProvider.NEntries]
				self._limitEntries -= fi[DataProvider.NEntries]
				return True
			block[DataProvider.FileList] = lfilter(filterEvents, block[DataProvider.FileList])
		return block


class EmptyDataProcessor(DataProcessor):
	alias = ['empty']

	def __init__(self, config, onChange):
		DataProcessor.__init__(self, config, onChange)
		self._emptyFiles = config.getBool('dataset remove empty files', True, onChange = onChange)
		self._emptyBlock = config.getBool('dataset remove empty blocks', True, onChange = onChange)
		(self._removedFiles, self._removedBlocks) = (0, 0)

	def enabled(self):
		return self._emptyBlock or self._emptyFiles

	def processBlock(self, block):
		if self._emptyFiles:
			n_files = len(block[DataProvider.FileList])
			block[DataProvider.FileList] = lfilter(lambda fi: fi[DataProvider.NEntries] != 0, block[DataProvider.FileList])
			self._removedFiles += n_files - len(block[DataProvider.FileList])
		if self._emptyBlock:
			if (block[DataProvider.NEntries] == 0) or not block[DataProvider.FileList]:
				self._removedBlocks += 1
				return
		return block

	def _finished(self):
		if self._removedFiles or self._removedBlocks:
			self._log.log(logging.INFO1, 'Empty files removed: %d, Empty blocks removed %d', self._removedFiles, self._removedBlocks)
		(self._removedFiles, self._removedBlocks) = (0, 0)


class LocationDataProcessor(DataProcessor):
	alias = ['location']

	def __init__(self, config, onChange):
		DataProcessor.__init__(self, config, onChange)
		self._locationfilter = config.getFilter('dataset location filter', '',
			defaultMatcher = 'blackwhite', defaultFilter = 'strict',
			onChange = onChange)

	def processBlock(self, block):
		if block[DataProvider.Locations] is not None:
			sites = self._locationfilter.filterList(block[DataProvider.Locations])
			if (sites is not None) and (len(sites) == 0) and (len(block[DataProvider.FileList]) != 0):
				if not len(block[DataProvider.Locations]):
					self._log.warning('Block %s is not available at any site!', DataProvider.bName(block))
				elif not len(sites):
					self._log.warning('Block %s is not available at any selected site!', DataProvider.bName(block))
			block[DataProvider.Locations] = sites
		return block
