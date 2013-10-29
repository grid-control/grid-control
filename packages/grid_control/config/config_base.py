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

	def set(self, *args, **kwargs): # Only set string values!
		return self._rawSet(*args, **kwargs)

	def get(self, *args, **kwargs): # Surrounding spaces will get discarded
		return self._rawGet('string', str, str, *args, **kwargs)

	def getInt(self, *args, **kwargs): # Using strict integer (de-)serialization
		return self._rawGet('int', int.__str__, int, *args, **kwargs)

	def getBool(self, *args, **kwargs):
		obj2str = lambda value: {True: 'true', False: 'false'}[value]
		def str2obj(value): # Feature: true and false are not the only valid expressions ...
			result = utils.parseBool(value)
			if result == None:
				raise ConfigError('Valid boolean expressions are: "true", "false"')
			return result
		return self._rawGet('bool', obj2str, str2obj, *args, **kwargs)

	def getTime(self, *args, **kwargs): # Get time in seconds - input base is hours
		def str2obj(value):
			try:
				return utils.parseTime(value) # empty or negative values are mapped to -1
			except:
				raise ConfigError('Valid time expressions have the format: hh[:mm[:ss]]')
		return self._rawGet('time', utils.strTimeShort, str2obj, *args, **kwargs)

	def getList(self, *args, **kwargs): # Get whitespace separated list (space, tab, newline)
		obj2str = lambda value: '\n' + str.join('\n', map(str, value))
		str2obj = lambda value: utils.parseList(value, None)
		return self._rawGet('list', obj2str, str2obj, *args, **kwargs)

	# Returns a tuple with (<dictionary>, <keys>) - the keys are sorted by order of appearance
	def getDict(self, *args, **kwargs): # Default key is accessed via key == None (None is never in keys!)
		parser = kwargs.pop('parser', lambda x: x) # currently only support value parsers
		obj2str = lambda value: str.join('\n\t', map(lambda kv: '%s => %s' % kv, value.items()))
		str2obj = lambda value: utils.parseDict(value, parser)
		return self._rawGet('dictionary', obj2str, str2obj, *args, **kwargs)

	# Return resolved path (search paths: $PWD, <gc directory>, <base path from constructor>)
	def getPath(self, *args, **kwargs):
		mustExist = kwargs.pop('mustExist', True) # throw exception if file is not found
		def obj2str(value):
			assert(isinstance(value, str)) # Catch 'None' default values etc.
			return str(value)
		return self._rawGet('path', obj2str, lambda p: self.parsePath(p, mustExist), *args, **kwargs)

	# Return multiple resolved paths (each line processed same as getPath)
	def getPaths(self, *args, **kwargs):
		mustExist = kwargs.pop('mustExist', True)
		obj2str = lambda value: '\n' + str.join('\n', value)
		str2obj = lambda value: map(lambda p: self.parsePath(p, mustExist), utils.parseList(value, None, onEmpty = []))
		return self._rawGet('paths', obj2str, str2obj, *args, **kwargs)

	# Resolve path - with special handling of empty strings
	def parsePath(self, value, mustExist):
		if value == '':
			return ''
		try:
			return utils.resolvePath(value, [self._rawBasePath], mustExist, ConfigError)
		except:
			raise RethrowError('Error resolving path %s' % value, ConfigError)
