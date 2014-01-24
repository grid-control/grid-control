from python_compat import *
from grid_control import *
import glob

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
		obj2str = kwargs.pop('obj2str', str.__str__)
		str2obj = kwargs.pop('str2obj', str)
		return self._rawGet('string', obj2str, str2obj, None, *args, **kwargs)

	def getInt(self, *args, **kwargs): # Using strict integer (de-)serialization
		return self._rawGet('int', int.__str__, int, None, *args, **kwargs)

	def getBool(self, *args, **kwargs):
		def str2obj(value): # Feature: true and false are not the only valid expressions ...
			result = utils.parseBool(value)
			if result == None:
				raise ConfigError('Valid boolean expressions are: "true", "false"')
			return result
		return self._rawGet('bool', bool.__str__, str2obj, None, *args, **kwargs)

	def getTime(self, *args, **kwargs): # Get time in seconds - input base is hours
		def str2obj(value):
			try:
				return utils.parseTime(value) # empty or negative values are mapped to -1
			except:
				raise ConfigError('Valid time expressions have the format: hh[:mm[:ss]]')
		return self._rawGet('time', utils.strTimeShort, str2obj, None, *args, **kwargs)

	# Returns a tuple with (<dictionary>, <keys>) - the keys are sorted by order of appearance
	def getDict(self, *args, **kwargs): # Default key is accessed via key == None (None is never in keys!)
		parser = kwargs.pop('parser', lambda x: x) # currently only support value parsers
		def obj2str(value):
			(srcdict, srckeys) = value
			getmax = lambda src: max(map(lambda x: len(str(x)), src) + [0])
			result = srcdict.get(None, '')
			fmt = '\n\t%%%ds => %%%ds' % (getmax(srckeys), getmax(srcdict.values()))
			return result + str.join('', map(lambda k: fmt % (k, srcdict[k]), srckeys))
		str2obj = lambda value: utils.parseDict(value, parser)
		def2obj = lambda value: (value, value.keys())
		return self._rawGet('dictionary', obj2str, str2obj, def2obj, *args, **kwargs)

	# Get whitespace separated list (space, tab, newline)
	def getList(self, *args, **kwargs):
		parseItem = kwargs.pop('parseItem', lambda x: x)
		obj2str = lambda value: '\n' + str.join('\n', map(str, value))
		str2obj = lambda value: map(parseItem, utils.parseList(value, None))
		return self._rawGet('list', obj2str, str2obj, None, *args, **kwargs)

	# Return resolved path (search paths: $PWD, <gc directory>, <base path from constructor>)
	def getPath(self, *args, **kwargs):
		mustExist = kwargs.pop('mustExist', True) # throw exception if file is not found
		def parsePath(value):
			if value == '':
				return ''
			try:
				return utils.resolvePath(value, [self._rawBasePath], mustExist, ConfigError)
			except:
				raise RethrowError('Error resolving path %s' % value, ConfigError)
		return self._rawGet('path', str.__str__, parsePath, None, *args, **kwargs)

	# Return multiple resolved paths (each line processed same as getPath)
	def getPaths(self, *args, **kwargs):
		def patlist2pathlist(value, mustExist):
			result = []
			for pattern in value:
				try:
					result.extend(utils.resolvePaths(pattern, [self._rawBasePath], mustExist, ConfigError))
				except:
					raise RethrowError('Error resolving pattern %s' % pattern, ConfigError)
			return result

		mustExist = kwargs.pop('mustExist', True)
		str2obj = lambda value: patlist2pathlist(utils.parseList(value, None, onEmpty = []), mustExist)
		obj2str = lambda value: '\n' + str.join('\n', patlist2pathlist(value, False))
		return self._rawGet('paths', obj2str, str2obj, None, *args, **kwargs)

	# Needed by getClass / getClasses to wrap the fixed arguments to the instantiation / name of the instance
	class _ClassProxy:
		def __init__(self, baseClass, value, config):
			(clsName, instName) = utils.optSplit(value, ':')
			if instName == '':
				instName = clsName.split('.')[-1] # Default: (non fully qualified) class name as instance name
			(self._baseClass, self._config, self._clsName, self._instName) = \
				(baseClass, config, clsName, instName)
		def __eq__(self, other): # Used to check for changes compared to old
			return str(self) == str(other)
		def __str__(self):  # Used to serialize config setting
			clsName = self._clsName.split('.')[-1] # take care of fully qualified class names
			return QM(clsName == self._instName, self._clsName, '%s:%s' % (self._clsName, self._instName))
		def __call__(self, *args, **kwargs): # Instantiate wrapped class with config and instance name as args
			cls = self._baseClass.getClass(self._clsName) # Get class to collect config sections
			sections = cls.getAllConfigSections(self._instName)
			config = self._config
			if sections:
				config = self._config.getScoped(sections)
			return self._baseClass.getInstance(self._clsName, config, self._instName, *args, **kwargs)

	# Return class - default class is also given in string form!
	def getClass(self, *args, **kwargs):
		baseClass = kwargs.pop('cls')
		configScope = kwargs.pop('scope', [])
		config = self
		if configScope != []:
			config = self.getScoped(configScope)
		str2obj = lambda value: ConfigBase._ClassProxy(baseClass, value, config)
		return self._rawGet('class', str, str2obj, str2obj, *args, **kwargs)

	# Return classes - default class is also given in string form!
	def getClassList(self, *args, **kwargs):
		baseClass = kwargs.pop('cls')
		configScope = kwargs.pop('scope', [])
		config = self
		if configScope != []:
			config = self.getScoped(configScope)
		parseSingle = lambda x: ConfigBase._ClassProxy(baseClass, x, config)
		str2obj = lambda value: map(parseSingle, utils.parseList(value, None, onEmpty = []))
		obj2str = lambda value: str.join('\n', map(str, value))
		return self._rawGet('class', obj2str, str2obj, str2obj, *args, **kwargs)
