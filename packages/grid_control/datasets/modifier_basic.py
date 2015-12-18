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

import re
from grid_control.datasets.modifier_base import DatasetModifier
from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.utils import doBlackWhiteList, makeEnum
from python_compat import md5, set

class EntriesConsistencyFilter(DatasetModifier):
	def processBlock(self, block):
		# Check entry consistency
		events = sum(map(lambda x: x[DataProvider.NEntries], block[DataProvider.FileList]))
		if block.setdefault(DataProvider.NEntries, events) != events:
			self._log.warning('WARNING: Inconsistency in block %s#%s: Number of events doesn\'t match (b:%d != f:%d)'
				% (block[DataProvider.Dataset], block[DataProvider.BlockName], block[DataProvider.NEntries], events))
		return block

class URLFilter(DatasetModifier):
	def __init__(self, config, name):
		DatasetModifier.__init__(self, config, name)
		self._ignoreURLs = config.getList(['ignore urls', 'ignore files'], [])

	def _matchURL(self, url):
		return url not in self._ignoreURLs

	def processBlock(self, block):
		block[DataProvider.FileList] = filter(lambda x: self._matchURL(x[DataProvider.URL]), block[DataProvider.FileList])
		return block

class FileFilter(URLFilter):
	pass


class URLRegexFilter(URLFilter):
	def __init__(self, config, name):
		URLFilter.__init__(self, config, name)
		self._ignoreREs = map(re.compile, self._ignoreURLs)

	def _matchURL(self, url):
		for matcher in self._ignoreREs:
			if matcher(url):
				return True
		return False

class FileRegexFilter(URLRegexFilter):
	pass


class URLCountFilter(DatasetModifier):
	def __init__(self, config, name):
		DatasetModifier.__init__(self, config, name)
		self._limitFiles = config.getInt(['limit urls', 'limit files'], -1)

	def processBlock(self, block):
		if self._limitFiles != -1:
			block[DataProvider.FileList] = block[DataProvider.FileList][:self._limitFiles]
			self._limitFiles -= len(block[DataProvider.FileList])
		return block

class FileCountFilter(URLCountFilter):
	pass


class EntriesCountFilter(DatasetModifier):
	def __init__(self, config, name):
		DatasetModifier.__init__(self, config, name)
		self._limitEntries = config.getInt(['limit entries', 'limit events'], -1)

	def processBlock(self, block):
		if self._limitEntries != -1:
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
			block[DataProvider.FileList] = filter(filterEvents, block[DataProvider.FileList])
		return block

class EventsCountFilter(EntriesCountFilter):
	pass


class EmptyFilter(DatasetModifier):
	def __init__(self, config, name):
		DatasetModifier.__init__(self, config, name)
		self._emptyFiles = config.getBool('remove empty files', True)
		self._emptyBlock = config.getBool('remove empty blocks', True)

	def processBlock(self, block):
		if self._emptyFiles:
			block[DataProvider.FileList] = filter(lambda fi: fi[DataProvider.NEntries] != 0, block[DataProvider.FileList])
		if self._emptyBlock:
			if (block[DataProvider.NEntries] == 0) or not block[DataProvider.FileList]:
				return
		return block


class LocationFilter(DatasetModifier):
	def __init__(self, config, name):
		DatasetModifier.__init__(self, config, name)
		self._sitefilter = config.getList('sites', [])

	def processBlock(self, block):
		if block[DataProvider.Locations] != None:
			sites = doBlackWhiteList(block[DataProvider.Locations], self._sitefilter, onEmpty = [], preferWL = False)
			if len(sites) == 0 and len(block[DataProvider.FileList]) != 0:
				if not len(block[DataProvider.Locations]):
					self._log.warning('WARNING: Block %s#%s is not available at any site!'
						% (block[DataProvider.Dataset], block[DataProvider.BlockName]))
				elif not len(sites):
					self._log.warning('WARNING: Block %s#%s is not available at any selected site!'
						% (block[DataProvider.Dataset], block[DataProvider.BlockName]))
			block[DataProvider.Locations] = sites
		return block


# Enum to specify how to react to multiple occurences of something
DatasetUniqueMode = makeEnum(['warn', 'abort', 'skip', 'ignore', 'record'], useHash = True)

class UniqueFilter(DatasetModifier):
	def __init__(self, config, name):
		DatasetModifier.__init__(self, config, name)
		self._checkURL = config.getEnum('check unique url', DatasetUniqueMode, DatasetUniqueMode.abort)
		self._checkBlock = config.getEnum('check unique block', DatasetUniqueMode, DatasetUniqueMode.abort)
		self._recordedURL = set()
		self._recordedBlock = set()

	def processBlock(self, block):
		# Check uniqueness of URLs
		recordedBlockURL = []
		if self._checkURL != DatasetUniqueMode.ignore:
			def processFI(fiList):
				for fi in fiList:
					urlHash = md5(repr((fi[DataProvider.URL], fi[DataProvider.NEntries], fi[DataProvider.Metadata]))).digest()
					if urlHash in self._recordedURL:
						msg = 'Multiple occurences of URL: "%s"!' % fi[DataProvider.URL]
						if self._checkURL == DatasetUniqueMode.warn:
							self._log.warning(msg)
						elif self._checkURL == DatasetUniqueMode.abort:
							raise DatasetError(msg)
						elif self._checkURL == DatasetUniqueMode.skip:
							continue
					self._recordedURL.add(urlHash)
					recordedBlockURL.append(urlHash)
					yield fi
			block[DataProvider.FileList] = list(processFI(block[DataProvider.FileList]))
			recordedBlockURL.sort()

		# Check uniqueness of blocks
		if self._checkBlock != DatasetUniqueMode.ignore:
			blockHash = md5(repr((block[DataProvider.Dataset], block[DataProvider.BlockName],
				recordedBlockURL, block[DataProvider.NEntries],
				block[DataProvider.Locations], block[DataProvider.Metadata]))).digest()
			if blockHash in self._recordedBlock:
				msg = 'Multiple occurences of block: "%s#%s"!' % (fi[DataProvider.Dataset], fi[DataProvider.BlockName])
				if self._checkBlock == DatasetUniqueMode.warn:
					self._log.warning(msg)
				elif self._checkBlock == DatasetUniqueMode.abort:
					raise DatasetError(msg)
				elif self._checkBlock == DatasetUniqueMode.skip:
					return None
			self._recordedBlock.add(blockHash)
		return block
