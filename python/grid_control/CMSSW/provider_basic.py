from __future__ import generators
import sys, os, gzip, cStringIO, copy
from grid_control import utils, AbstractObject, RuntimeError, ConfigError
from provider_base import DataProvider

# Provides information about a single file
# required format: /local/path/to/file|events[@SE1,SE2]
class FileProvider(DataProvider):
	def __init__(self, datasetExpr, datasetNick, datasetID = 1):
		DataProvider.__init__(self, datasetExpr, datasetNick, datasetID)

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
	def __init__(self, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, datasetExpr, datasetNick, datasetID)
		self._filename = datasetExpr


	def getBlocksInternal(self):
		result = []
		blockinfo = None

		for line in open(self._filename, 'rb'):
			# Found start of block:
			line = line.strip()
			if line.startswith(';'):
				continue
			elif line.startswith('['):
				if blockinfo:
					result.append(blockinfo)
				blockinfo = dict()
				dataset, blockname = line.lstrip('[').rstrip(']').split('#')
				blockinfo[DataProvider.Dataset] = dataset
				blockinfo[DataProvider.BlockName] = blockname
				blockinfo[DataProvider.SEList] = []
				blockinfo[DataProvider.FileList] = []
			elif line != '':
				tmp = map(str.strip, line.split('='))
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
					blockinfo[DataProvider.SEList] = value.split(',')
				else:
					blockinfo[DataProvider.FileList].append({
						DataProvider.lfn: key,
						DataProvider.NEvents: int(value)
					})
		else:
			if blockinfo:
				result.append(blockinfo)
		return result
