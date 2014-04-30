#-#  Copyright 2010-2014 Karlsruhe Institute of Technology
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

from python_compat import rsplit
from grid_control import QM, utils, ConfigError
from provider_base import DataProvider

# Provides information about a single file
# required format: <path to data file>|<number of events>[@SE1,SE2]
class FileProvider(DataProvider):
	DataProvider.providers.update({'FileProvider': 'file'})
	def __init__(self, config, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)

		(self._path, self._events, selist) = utils.optSplit(datasetExpr, '|@')
		self._selist = utils.parseList(selist, delimeter = ',', onEmpty = None)
		if not (self._path and self._events):
			raise ConfigError('Invalid dataset expression!\nCorrect: /local/path/to/file|events[@SE1,SE2]')


	def getBlocksInternal(self):
		yield {
			DataProvider.Dataset: self._path,
			DataProvider.Locations: self._selist,
			DataProvider.FileList: [{
				DataProvider.URL: self._path, DataProvider.NEntries: int(self._events)
			}]
		}


# Takes dataset information from an configuration file
# required format: <path to list of data files>[@<forced prefix>][%[/]<selected dataset>[#<selected block>][#]]
class ListProvider(DataProvider):
	DataProvider.providers.update({'ListProvider': 'list'})
	def __init__(self, config, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)

		(path, self._forcePrefix, self._filter) = utils.optSplit(datasetExpr, '@%')
		self._filename = config.resolvePath(path, True, 'Error resolving dataset file: %s' % path)


	def getBlocksInternal(self):
		def doFilter(block):
			if self._filter:
				name = '/%s#%s#' % (block[DataProvider.Dataset], block.get(DataProvider.BlockName, ''))
				return self._filter in name
			return True

		(blockinfo, commonMetadata) = (None, [])
		for line in open(self._filename, 'rb'):
			# Found start of block:
			line = line.strip()
			if line.startswith(';'):
				continue
			elif line.startswith('['):
				if blockinfo and doFilter(blockinfo):
					yield blockinfo
				blockinfo = { DataProvider.Locations: None, DataProvider.FileList: [] }
				blockname = line.lstrip('[').rstrip(']').split('#')
				if len(blockname) > 0:
					blockinfo[DataProvider.Dataset] = blockname[0]
				if len(blockname) > 1:
					blockinfo[DataProvider.BlockName] = blockname[1]
				commonprefix = self._forcePrefix
				commonMetadata = []
			elif line != '':
				tmp = map(str.strip, QM('[' in line, line.split(' = ', 1), rsplit(line, '=', 1)))
				if len(tmp) != 2:
					raise ConfigError('Malformed entry in dataset file:\n%s' % line)
				key, value = tmp
				if key.lower() == 'nickname':
					blockinfo[DataProvider.Nickname] = value
				elif key.lower() == 'id':
					blockinfo[DataProvider.DatasetID] = int(value)
				elif key.lower() == 'events':
					blockinfo[DataProvider.NEntries] = int(value)
				elif key.lower() == 'metadata':
					blockinfo[DataProvider.Metadata] = eval(value)
				elif key.lower() == 'metadata common':
					commonMetadata = eval(value)
				elif key.lower() == 'se list':
					blockinfo[DataProvider.Locations] = utils.parseList(value)
				elif key.lower() == 'prefix':
					if not self._forcePrefix:
						commonprefix = value
				else:
					if commonprefix:
						key = '%s/%s' % (commonprefix, key)
					value = value.split(' ', 1)
					data = { DataProvider.URL: key, DataProvider.NEntries: int(value[0]) }
					if commonMetadata:
						data[DataProvider.Metadata] = commonMetadata
					if len(value) > 1:
						data[DataProvider.Metadata] = data.get(DataProvider.Metadata, []) + eval(value[1])
					blockinfo[DataProvider.FileList].append(data)
		else:
			if blockinfo and doFilter(blockinfo):
				yield blockinfo
