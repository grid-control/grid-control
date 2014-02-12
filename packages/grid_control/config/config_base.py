import os, logging
from grid_control import utils, APIError, ConfigError, RethrowError, ClassWrapper
from container_base import noDefault
from config_handlers import changeImpossible

# Default selector takes section and option as first 2 arguments - rest is forwarded
def defaultSelectorFilter(section, option, *args, **kwargs):
	if not isinstance(section, list):
		section = [section]
	if not isinstance(option, list):
		option = [option]
	return ((section, option, [], []), args, kwargs)

# Find config caller
def fmtStack():
	import inspect
	for frame in inspect.stack():
		if '/packages/grid_control/config/' not in frame[1]:
			return frame[0].f_locals.get('self', None).__class__.__name__
	return 'main'


# Config interface class accessing data using an interface supplied by another class
class ConfigBase(object):
	def __init__(self, confName, curCfg, pathBase, oldCfg = None, pathWork = None, selectorFilter = defaultSelectorFilter):
		self._logger = logging.getLogger(('config.%s' % confName).rstrip('.'))
		(self._confName, self._curCfg, self._oldCfg) = (confName, curCfg, oldCfg)
		(self._pathBase, self._pathWork) = (pathBase, pathWork)
		self._selectorFilter = selectorFilter

	def init(self, oldCfg = None, pathWork = None):
		if oldCfg:
			self._oldCfg = oldCfg
		if pathWork:
			self._pathWork = pathWork

	# Get cloned ConfigBase (or derived class) with current settings and new/current selectorFilter
	def clone(self, selectorFilter = None, ClassTemplate = None):
		if not selectorFilter:
			selectorFilter = defaultSelectorFilter
		if not ClassTemplate:
			ClassTemplate = ConfigBase
		return ClassTemplate(self._confName, self._curCfg, self._pathBase, self._oldCfg, self._pathWork, selectorFilter)

	def getWorkPath(self, *fnList):
		return os.path.join(self._pathWork, *fnList)

	# Get all selected options
	def getOptions(self, *args, **kwargs):
		if 'option' in kwargs:
			raise APIError('Invalid parameters!')
		kwargs['option'] = []
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		if args or kwargs:
			raise APIError('Invalid parameters!')
		return self._curCfg.getOptions(selector)

	# Get a typed config value from the container
	def _getTyped(self, desc, obj2str, str2obj, def2obj, selector, default_obj = noDefault,
			onChange = changeImpossible, onValid = None, persistent = False, markDefault = True):
		self._logger.log(logging.DEBUG2, 'Config query from: %s' % fmtStack())
		self._logger.log(logging.DEBUG1, 'Config query for config option "%s"' % str(selector))

		# First transform default into string if applicable
		default_str = noDefault
		if default_obj != noDefault:
			try:
				if def2obj:
					default_obj = def2obj(default_obj)
			except:
				raise APIError('Unable to convert default object: %r' % default_obj)
			try:
				default_str = obj2str(default_obj)
			except:
				raise APIError('Unable to get string representation of default object: %r' % default_obj)

		old_entry = None
		if self._oldCfg:
			old_entry = self._oldCfg.get(selector, default_str, raiseMissing = False)
			if old_entry and persistent: # Override current default value with stored value
				default_str = old_entry.value
				self._logger.log(logging.INFO2, 'Applying persistent %s' % old_entry.format(printSection = True))
		cur_entry = self._curCfg.get(selector, default_str, markDefault = markDefault)
		try:
			cur_obj = str2obj(cur_entry.value)
			cur_entry.value = obj2str(cur_obj)
		except:
			raise RethrowError('Unable to parse %s: [%s] %s = %s' % (desc,
				cur_entry.section, cur_entry.option, cur_entry.value), ConfigError)

		# Notify about changes
		if onChange and old_entry:
			try:
				old_obj = str2obj(old_entry.value)
			except:
				raise RethrowError('Unable to parse stored %s: [%s] %s = %s' % (desc,
					old_entry.section, old_entry.option, old_entry.value), ConfigError)
			if not (old_obj == cur_obj):
				# Main reason for caller support is to localize reinits to affected modules
				cur_obj = onChange(self, old_obj, cur_obj, cur_entry, obj2str)
				cur_entry.value = obj2str(cur_obj)
		if onValid:
			return onValid(cur_entry.section, cur_entry.option, cur_obj)
		return cur_obj

	def get(self, *args, **kwargs): # Surrounding spaces will get discarded
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		obj2str = kwargs.pop('obj2str', str.__str__)
		str2obj = kwargs.pop('str2obj', str)
		return self._getTyped('string', obj2str, str2obj, None, selector, *args, **kwargs)

	def getInt(self, *args, **kwargs): # Using strict integer (de-)serialization
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		return self._getTyped('int', int.__str__, int, None, selector, *args, **kwargs)

	def getBool(self, *args, **kwargs):
		def str2obj(value): # Feature: true and false are not the only valid expressions ...
			result = utils.parseBool(value)
			if result == None:
				raise ConfigError('Valid boolean expressions are: "true", "false"')
			return result
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		return self._getTyped('bool', bool.__str__, str2obj, None, selector, *args, **kwargs)

	def getTime(self, *args, **kwargs): # Get time in seconds - input base is hours
		def str2obj(value):
			try:
				return utils.parseTime(value) # empty or negative values are mapped to -1
			except:
				raise ConfigError('Valid time expressions have the format: hh[:mm[:ss]]')
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		return self._getTyped('time', utils.strTimeShort, str2obj, None, selector, *args, **kwargs)

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
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		return self._getTyped('dictionary', obj2str, str2obj, def2obj, selector, *args, **kwargs)

	# Get whitespace separated list (space, tab, newline)
	def getList(self, *args, **kwargs):
		parseItem = kwargs.pop('parseItem', lambda x: x)
		obj2str = lambda value: '\n' + str.join('\n', map(str, value))
		str2obj = lambda value: map(parseItem, utils.parseList(value, None))
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		return self._getTyped('list', obj2str, str2obj, None, selector, *args, **kwargs)

	# Return resolved path (search paths: $PWD, <gc directory>, <base path from constructor>)
	def getPath(self, *args, **kwargs):
		mustExist = kwargs.pop('mustExist', True) # throw exception if file is not found
		def parsePath(value):
			if value == '':
				return ''
			try:
				return utils.resolvePath(value, [self._pathBase], mustExist, ConfigError)
			except:
				raise RethrowError('Error resolving path %s' % value, ConfigError)
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		return self._getTyped('path', str.__str__, parsePath, None, selector, *args, **kwargs)

	# Return multiple resolved paths (each line processed same as getPath)
	def getPaths(self, *args, **kwargs):
		def patlist2pathlist(value, mustExist):
			result = []
			for pattern in value:
				try:
					result.extend(utils.resolvePaths(pattern, [self._pathBase], mustExist, ConfigError))
				except:
					raise RethrowError('Error resolving pattern %s' % pattern, ConfigError)
			return result

		mustExist = kwargs.pop('mustExist', True)
		str2obj = lambda value: patlist2pathlist(utils.parseList(value, None, onEmpty = []), mustExist)
		obj2str = lambda value: '\n' + str.join('\n', patlist2pathlist(value, False))
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		return self._getTyped('paths', obj2str, str2obj, None, selector, *args, **kwargs)

	# Return class - default class is also given in string form!
	def getClass(self, *args, **kwargs):
		baseClass = kwargs.pop('cls')
		tags = kwargs.pop('tags', [])
		inherit = kwargs.pop('inherit', False)
		str2obj = lambda value: ClassWrapper(baseClass, value, self, tags, inherit)
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		return self._getTyped('class', str, str2obj, str2obj, selector, *args, **kwargs)

	# Return classes - default classes are also given in string form!
	def getClassList(self, *args, **kwargs):
		baseClass = kwargs.pop('cls')
		tags = kwargs.pop('tags', [])
		inherit = kwargs.pop('inherit', False)
		parseSingle = lambda value: ClassWrapper(baseClass, value, self, tags, inherit)
		str2obj = lambda value: map(parseSingle, utils.parseList(value, None, onEmpty = []))
		obj2str = lambda value: str.join('\n', map(str, value))
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		return self._getTyped('class', obj2str, str2obj, str2obj, selector, *args, **kwargs)

	# Setter
	def set(self, *args, **kwargs): # Only set string values!
		(selector, args, kwargs) = self._selectorFilter(*args, **kwargs)
		def setChecked(selector, value, override = True, append = False, source = '<dynamic>'):
			(section, option, names, tags) = selector
			if not override:
				option = map(lambda o: o + '?', option)
			elif append:
				option = map(lambda o: o + '+', option)
			entry = self._curCfg.set((section, option, names, tags), value, source, markAccessed = True)
			self._logger.log(logging.INFO3, 'Setting dynamic key [%s] %s = %s' % (entry.section, entry.option, value))
			return entry
		return setChecked(selector, *args, **kwargs)

	# Write settings to file
	def write(self, *args, **kwargs):
		self._curCfg.write(*args, **kwargs)
