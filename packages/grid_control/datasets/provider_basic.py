# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.config import ConfigError
from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.utils.parsing import parseJSON, parseList
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


def try_apply(value, fun, desc):
	try:
		return fun(value)
	except Exception:
		raise DatasetError('Unable to parse %s: %s' % (desc, repr(value)))


# Takes dataset information from an configuration file
# required format: <path to list of data files>[@<forced prefix>][%[/]<selected dataset>[#<selected block>][#]]
class ListProvider(DataProvider):
	alias = ['list']

	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)
		self._CommonPrefix = max(self.enumValues) + 1
		self._CommonMetadata = max(self.enumValues) + 2

		self._handleEntry = {
			'events': (DataProvider.NEntries, int, 'block entry counter'),
			'id': (DataProvider.DatasetID, int, 'dataset ID'),
			'metadata': (DataProvider.Metadata, parseJSON, 'metadata description'),
			'metadata common': (self._CommonMetadata, parseJSON, 'common metadata'),
			'nickname': (DataProvider.Nickname, str, 'dataset nickname'),
			'prefix': (self._CommonPrefix, str, 'common prefix'),
			'se list': (DataProvider.Locations, lambda value: parseList(value, ','), 'block location'),
		}

		(path, self._forcePrefix, self._filter) = utils.optSplit(datasetExpr, '@%')
		self._filename = config.resolvePath(path, True, 'Error resolving dataset file: %s' % path)

	def _createBlock(self, name):
		result = {
			DataProvider.Locations: None,
			DataProvider.FileList: [],
			self._CommonPrefix: None,
			self._CommonMetadata: [],
		}
		blockName = name.lstrip('[').rstrip(']').split('#')
		if len(blockName) > 0:
			result[DataProvider.Dataset] = blockName[0]
		if len(blockName) > 1:
			result[DataProvider.BlockName] = blockName[1]
		return result

	def _finishBlock(self, block):
		block.pop(self._CommonPrefix)
		block.pop(self._CommonMetadata)
		return block

	def _parseEntry(self, block, url, value):
		if self._forcePrefix:
			url = '%s/%s' % (self._forcePrefix, url)
		elif block[self._CommonPrefix]:
			url = '%s/%s' % (block[self._CommonPrefix], url)
		value = value.split(' ', 1)
		result = {
			DataProvider.URL: url,
			DataProvider.NEntries: try_apply(value[0], int, 'entries of file %s' % repr(url))
		}
		if len(value) > 1:
			fileMetadata = try_apply(value[1], parseJSON, 'metadata of file %s' % repr(url))
		else:
			fileMetadata = []
		if block[self._CommonMetadata] or fileMetadata:
			result[DataProvider.Metadata] = block[self._CommonMetadata] + fileMetadata
		return result

	def _parseFile(self, iterator):
		block = None
		for idx, line in enumerate(iterator):
			try:
				# Found start of block:
				line = line.strip()
				if line.startswith(';'):
					continue
				elif line.startswith('['):
					if block:
						yield self._finishBlock(block)
					block = self._createBlock(line)
				elif line != '':
					tmp = lmap(str.strip, utils.QM('[' in line, line.split(' = ', 1), rsplit(line, '=', 1)))
					if len(tmp) != 2:
						raise DatasetError('Malformed entry in dataset file:\n%s' % line)
					key, value = tmp
					handlerInfo = self._handleEntry.get(key.lower(), None)
					if handlerInfo:
						(prop, parser, msg) = handlerInfo
						block[prop] = try_apply(value, parser, msg)
					else:
						block[DataProvider.FileList].append(self._parseEntry(block, key, value))
			except Exception:
				raise DatasetError('Unable to parse %s:%d\n\t%s' % (repr(self._filename), idx, repr(line)))
		if block:
			yield self._finishBlock(block)

	def getBlocksInternal(self):
		def _filterBlock(block):
			if self._filter:
				name = '/%s#%s#' % (block[DataProvider.Dataset], block.get(DataProvider.BlockName, ''))
				return self._filter in name
			return True
		try:
			fp = open(self._filename, 'r')
		except Exception:
			raise DatasetError('Unable to open dataset file %s' % repr(self._filename))
		try:
			for block in self._parseFile(fp):
				if _filterBlock(block):
					yield block
			fp.close()
		except Exception:
			fp.close()
			raise
