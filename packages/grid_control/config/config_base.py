from python_compat import *
from grid_control import *

# Config interface class accessing data using an interface supplied by another class
class ConfigBase(object):
	def __init__(self, rawSet, rawGet, rawIter, rawBasePath):
		# required options:
		#   rawSet(...) unspecific - 
		#   rawGet(desc, obj2str, str2obj, ...)
		(self._rawSet, self._rawGet, self._rawIter) = (rawSet, rawGet, rawIter)
		self._rawBasePath = rawBasePath

	def getOptions(self, *args, **kwargs):
		return self._rawIter(*args, **kwargs)

	def set(self, *args, **kwargs):
		return self._rawSet(*args, **kwargs)

	def get(self, *args, **kwargs):
		return self._rawGet('string', str, str, *args, **kwargs)

	def getInt(self, *args, **kwargs):
		return self._rawGet('int', str, int, *args, **kwargs)

	def getBool(self, *args, **kwargs):
		obj2str = lambda value: QM(value, 'true', 'false')
		def str2obj(value):
			result = utils.parseBool(value)
			assert(result != None)
			return result
		return self._rawGet('bool', obj2str, str2obj, *args, **kwargs)

	def getTime(self, *args, **kwargs):
		return self._rawGet('time', utils.strTimeShort, utils.parseTime, *args, **kwargs)

	def getList(self, *args, **kwargs):
		obj2str = lambda value: '\n' + str.join('\n', map(str, value))
		str2obj = lambda value: utils.parseList(value, None)
		return self._rawGet('list', obj2str, str2obj, *args, **kwargs)

	def getDict(self, *args, **kwargs):
		parser = kwargs.pop('parser', lambda x: x)
		obj2str = lambda value: str.join('\n\t', map(lambda kv: '%s => %s' % kv, value.items()))
		str2obj = lambda value: utils.parseDict(value, parser)
		return self._rawGet('dictionary', obj2str, str2obj, *args, **kwargs)

	def getPath(self, *args, **kwargs):
		mustExist = kwargs.pop('mustExist', True)
		return self._rawGet('path', str, lambda p: self.parsePath(p, mustExist), *args, **kwargs)

	def getPaths(self, *args, **kwargs):
		mustExist = kwargs.pop('mustExist', True)
		obj2str = lambda value: '\n' + str.join('\n', value)
		str2obj = lambda value: map(lambda p: self.parsePath(p, mustExist), utils.parseList(value, None, onEmpty = []))
		return self._rawGet('paths', obj2str, str2obj, *args, **kwargs)

	def parsePath(self, value, mustExist):
		if value == '':
			return ''
		try:
			return utils.resolvePath(value, [self._rawBasePath], mustExist, ConfigError)
		except:
			raise RethrowError('Error resolving path %s' % value, ConfigError)
