from grid_control import utils, ConfigError
from provider_base import DataProvider

# Provides information about a single file
# required format: <path to data file>|<number of events>[@SE1,SE2]
class FileProvider(DataProvider):
	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, section, datasetExpr, datasetNick, datasetID)
		DataProvider.providers.update({'FileProvider': 'file'})

		(self._path, self._events, selist) = utils.optSplit(datasetExpr, "|@")
		self._selist = None
		if selist:
			self._selist = map(str.strip, selist.split(','))
		if not (self._path and self._events):
			raise ConfigError('Invalid dataset expression!\nCorrect: /local/path/to/file|events[@SE1,SE2]')


	def getBlocksInternal(self):
		return [{
			DataProvider.Dataset: self._path,
			DataProvider.BlockName: 'fileblock0',
			DataProvider.NEvents: int(self._events),
			DataProvider.SEList: self._selist,
			DataProvider.FileList: [{
				DataProvider.lfn: self._path,
				DataProvider.NEvents: int(self._events)
			}]
		}]


# Takes dataset information from an configuration file
# required format: <path to list of data files>[@<forced prefix>][%<selected dataset>[#<selected block>]]
class ListProvider(DataProvider):
	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config,section, datasetExpr, datasetNick, datasetID)
		DataProvider.providers.update({'ListProvider': 'list'})

		(path, self._forcePrefix, self._filter) = utils.optSplit(datasetExpr, "@%")
		self._filename = utils.resolvePath(path)


	def getBlocksInternal(self):
		result = []
		blockinfo = None

		def doFilter(blockinfo):
			name = self._filter
			if self._filter:
				name = blockinfo[DataProvider.Dataset]
				if DataProvider.BlockName in blockinfo and "#" in self._filter:
					name = "%s#%s" % (name, blockinfo[DataProvider.BlockName])
			if name.startswith(self._filter):
				return True
			return False

		for line in open(self._filename, 'rb'):
			# Found start of block:
			line = line.strip()
			if line.startswith(';'):
				continue
			elif line.startswith('['):
				if blockinfo and doFilter(blockinfo):
					result.append(blockinfo)
				blockinfo = dict()
				blockname = line.lstrip('[').rstrip(']').split('#')
				if len(blockname) > 0:
					blockinfo[DataProvider.Dataset] = blockname[0]
				if len(blockname) > 1:
					blockinfo[DataProvider.BlockName] = blockname[1]
				else:
					blockinfo[DataProvider.BlockName] = "0"
				blockinfo[DataProvider.SEList] = None
				blockinfo[DataProvider.FileList] = []
				commonprefix = self._forcePrefix
			elif line != '':
				tmp = map(str.strip, line.split('=', 1))
				if len(tmp) != 2:
					raise ConfigError('Malformed dataset configuration line:\n%s' % line)
				key, value = tmp
				if key.lower() == 'nickname':
					blockinfo[DataProvider.Nickname] = value
				elif key.lower() == 'id':
					blockinfo[DataProvider.DatasetID] = int(value)
				elif key.lower() == 'events':
					blockinfo[DataProvider.NEvents] = int(value)
				elif key.lower() == 'se list':
					if value.lower().strip() != 'none':
						tmp = filter(lambda x: x != '', map(str.strip, value.split(',')))
						blockinfo[DataProvider.SEList] = tmp
				elif key.lower() == 'prefix':
					if not self._forcePrefix:
						commonprefix = value
				else:
					if commonprefix:
						key = "%s/%s" % (commonprefix, key)
					blockinfo[DataProvider.FileList].append({
						DataProvider.lfn: key,
						DataProvider.NEvents: int(value)
					})
		else:
			if blockinfo and doFilter(blockinfo):
				result.append(blockinfo)
		return result
