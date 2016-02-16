#-#  Copyright 2009-2016 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.config import ConfigError
from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.utils.parsing import parseList
from python_compat import lmap, rsplit

# Provides information about a single file
# required format: <path to data file>|<number of events>[@SE1,SE2]
class FileProvider(DataProvider):
	alias = ['file']

	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)

		(self._path, self._events, selist) = utils.optSplit(datasetExpr, '|@')
		self._selist = parseList(selist, ',') or None
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
	alias = ['list']

	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)

		(path, self._forcePrefix, self._filter) = utils.optSplit(datasetExpr, '@%')
		self._filename = config.resolvePath(path, True, 'Error resolving dataset file: %s' % path)


	def getBlocksInternal(self):
		def doFilter(block):
			if self._filter:
				name = '/%s#%s#' % (block[DataProvider.Dataset], block.get(DataProvider.BlockName, ''))
				return self._filter in name
			return True

		def try_apply(fun, value, desc):
			try:
				return fun(value)
			except Exception:
				raise DatasetError('Unable to parse %s: %s' % (desc, repr(value)))

		(blockinfo, commonMetadata) = (None, [])
		fp = open(self._filename, 'r')
		for idx, line in enumerate(fp):
			try:
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
					tmp = lmap(str.strip, utils.QM('[' in line, line.split(' = ', 1), rsplit(line, '=', 1)))
					if len(tmp) != 2:
						raise ConfigError('Malformed entry in dataset file:\n%s' % line)
					key, value = tmp
					if key.lower() == 'nickname':
						blockinfo[DataProvider.Nickname] = value
					elif key.lower() == 'id':
						blockinfo[DataProvider.DatasetID] = try_apply(int, value, 'dataset ID')
					elif key.lower() == 'events':
						blockinfo[DataProvider.NEntries] = try_apply(int, value, 'block entry counter')
					elif key.lower() == 'metadata':
						blockinfo[DataProvider.Metadata] = try_apply(eval, value, 'metadata description')
					elif key.lower() == 'metadata common':
						commonMetadata = try_apply(eval, value, 'common metadata')
					elif key.lower() == 'se list':
						blockinfo[DataProvider.Locations] = try_apply(lambda value: parseList(value, ','), value, 'block location')
					elif key.lower() == 'prefix':
						if not self._forcePrefix:
							commonprefix = value
					else:
						if commonprefix:
							key = '%s/%s' % (commonprefix, key)
						value = value.split(' ', 1)
						data = { DataProvider.URL: key,
							DataProvider.NEntries: try_apply(int, value[0], 'entries of file %s' % repr(key))}
						if commonMetadata:
							data[DataProvider.Metadata] = commonMetadata
						if len(value) > 1:
							fileMetadata = try_apply(eval, value[1], 'metadata of file %s' % repr(key))
							data[DataProvider.Metadata] = data.get(DataProvider.Metadata, []) + fileMetadata
						blockinfo[DataProvider.FileList].append(data)
			except Exception:
				fp.close()
				raise DatasetError('Unable to parse %r:%d' % (self._filename, idx))
		if blockinfo and doFilter(blockinfo):
			yield blockinfo
		fp.close()
