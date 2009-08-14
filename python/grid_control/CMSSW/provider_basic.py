from grid_control import utils, AbstractObject, RuntimeError, ConfigError
from provider_base import DataProvider

# Provides information about a single file
# required format: /local/path/to/file|events[@SE1,SE2]
class FileProvider(DataProvider):
	def __init__(self, config, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)

		tmp = datasetExpr.split('@')
		if len(tmp) == 1:
			self._selist = []
		elif len(tmp) == 2:
			self._selist = tmp[1].split(',')
			datasetExpr = tmp[0]
		try:
			self._path, self._events = datasetExpr.split('|')
		except:
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
class ListProvider(DataProvider):
	def __init__(self, config, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)

		tmp = map(str.strip, datasetExpr.split('%'))
		self._filename = config.getPath("CMSSW", "dataset file", tmp[0])
		self._filter = None
		if len(tmp) == 2:
			self._filter = tmp[1]

	def getBlocksInternal(self):
		result = []
		blockinfo = None

		def doFilter(blockinfo):
			name = self._filter
			if self._filter:
				name = blockinfo[DataProvider.Dataset]
				if blockinfo.has_key(DataProvider.BlockName):
					name = "%s#%s" % (name, blockinfo[DataProvider.BlockName])
			if name == self._filter:
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
				blockinfo[DataProvider.SEList] = []
				blockinfo[DataProvider.FileList] = []
				commonprefix = None
			elif line != '':
				tmp = map(str.strip, line.split('=',1))
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
					blockinfo[DataProvider.SEList] = map(str.strip, value.split(','))
				elif key.lower() == 'prefix':
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
