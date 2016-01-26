#-#  Copyright 2014-2016 Karlsruhe Institute of Technology
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

import os, sys
from grid_control import utils
from grid_control.config.cinterface_base import ConfigInterface
from grid_control.config.config_entry import ConfigError, noDefault
from grid_control.config.cview_base import SimpleConfigView
from grid_control.config.cview_tagged import TaggedConfigView
from hpfwk import APIError, AbstractError, NamedPlugin, Plugin, PluginError
from python_compat import user_input

def appendOption(option, suffix):
	if isinstance(option, (list, tuple)):
		return map(lambda x: appendOption(x, suffix), option)
	return option.rstrip() + ' ' + suffix

# Needed by getPlugin / getCompositePlugin to wrap the fixed arguments to the instantiation / name of the instance
class ClassWrapper(object):
	def __init__(self, baseClass, value, config, tags, inherit, defaultName, pluginPaths):
		(self._baseClass, self._config, self._tags, self._inherit, self._pluginPaths) = \
			(baseClass, config, tags, inherit, pluginPaths)
		(self._instClassName, self._instName) = utils.optSplit(value, ':')
		if self._instName == '':
			if not defaultName:
				self._instName = self._instClassName.split('.')[-1] # Default: (non fully qualified) class name as instance name
			else:
				self._instName = defaultName

	def __eq__(self, other): # Used to check for changes compared to old
		return str(self) == str(other)

	def __repr__(self):
		return '<class wrapper for %r (base: %r)>' % (str(self), self._baseClass.__name__)

	def __str__(self):  # Used to serialize config setting
		if self._instName == self._instClassName.split('.')[-1]: # take care of fully qualified class names
			return self._instClassName
		return '%s:%s' % (self._instClassName, self._instName)

	def getObjectName(self):
		return self._instName

	def getClass(self):
		return self._baseClass.getClass(self._instClassName, self._pluginPaths)

	def getInstance(self, *args, **kwargs):
		cls = self.getClass()
		if issubclass(cls, NamedPlugin):
			config = self._config.changeView(viewClass = TaggedConfigView,
				setClasses = [cls], setSections = None, setNames = [self._instName],
				addTags = self._tags, inheritSections = self._inherit)
			args = [config, self._instName] + list(args)
		try:
			return cls(*args, **kwargs)
		except Exception:
			raise PluginError('Error while creating instance of type %s (%s)' % (cls, str(self)))

# General purpose class factory
class CompositedClassWrapper(object):
	def __init__(self, clsCompositor, clsList):
		(self._clsCompositor, self._clsList) = (clsCompositor, clsList)

	# Get single instance by merging multiple sub instances if necessary
	def getInstance(self, *args, **kwargs):
		return self._clsCompositor.getInstance(self._clsList, *args, **kwargs)

# Config interface class accessing typed data using an string interface provided by configView
class TypedConfigInterface(ConfigInterface):
	# Function to retrieve the plugin search paths
	def _getPluginPaths(self):
		if self._configView.pathDict.get('plugin_paths') == None:
			pluginCfg = self.changeView(viewClass = SimpleConfigView, setSections = ['global'])
			pluginPaths = pluginCfg.getPaths('plugin paths', mustExist = False, onChange = None)
			self._configView.pathDict['plugin_paths'] = pluginPaths
		return self._configView.pathDict['plugin_paths']

	# Handling integer config options - using strict integer (de-)serialization
	def getInt(self, option, default = noDefault, **kwargs):
		return self._getInternal('int', int.__str__, int, None, option, default, **kwargs)
	def setInt(self, option, value, opttype = '=', source = None):
		return self._setInternal('int', int.__str__, option, value, opttype, source)

	# Handling boolean config options - feature: true and false are not the only valid expressions
	def getBool(self, option, default = noDefault, **kwargs):
		def str2obj(value):
			result = utils.parseBool(value)
			if result == None:
				raise ConfigError('Valid boolean expressions are: "true", "false"')
			return result
		return self._getInternal('bool', bool.__str__, str2obj, None, option, default, **kwargs)
	def setBool(self, option, value, opttype = '=', source = None):
		return self._setInternal('bool', bool.__str__, option, value, opttype, source)

	# Get time in seconds - input base is hours
	def getTime(self, option, default = noDefault, **kwargs):
		def str2obj(value):
			try:
				return utils.parseTime(value) # empty or negative values are mapped to -1
			except Exception:
				raise ConfigError('Valid time expressions have the format: hh[:mm[:ss]]')
		return self._getInternal('time', utils.strTimeShort, str2obj, None, option, default, **kwargs)
	def setTime(self, option, value, opttype = '=', source = None):
		return self._setInternal('time', utils.strTimeShort, option, value, opttype, source)

	# Returns a tuple with (<dictionary>, <keys>) - the keys are sorted by order of appearance
	# Default key is accessed via key == None (None is never in keys!)
	def getDict(self, option, default = noDefault, parser = lambda x: x, strfun = lambda x: x, **kwargs):
		def obj2str(value):
			(srcdict, srckeys) = value
			getmax = lambda src: max(map(lambda x: len(str(x)), src) + [0])
			result = ''
			if srcdict.get(None) != None:
				result = strfun(srcdict.get(None, parser('')))
			fmt = '\n\t%%%ds => %%%ds' % (getmax(srckeys), getmax(srcdict.values()))
			return result + str.join('', map(lambda k: fmt % (k, strfun(srcdict[k])), srckeys))
		str2obj = lambda value: utils.parseDict(value, parser)
		def2obj = lambda value: (value, value.keys())
		return self._getInternal('dictionary', obj2str, str2obj, def2obj, option, default, **kwargs)

	# Get whitespace separated list (space, tab, newline)
	def getList(self, option, default = noDefault, parseItem = lambda x: x, **kwargs):
		obj2str = lambda value: '\n' + str.join('\n', map(str, value))
		str2obj = lambda value: map(parseItem, utils.parseList(value, None))
		return self._getInternal('list', obj2str, str2obj, None, option, default, **kwargs)

	# Resolve path
	def resolvePath(self, value, mustExist, errorMsg):
		try:
			return utils.resolvePath(value, self._configView.pathDict.get('search_paths', []), mustExist, ConfigError)
		except Exception:
			raise ConfigError(errorMsg)

	# Return resolved path (search paths given in pathDict['search_paths'])
	def getPath(self, option, default = noDefault, mustExist = True, storeRelative = False, **kwargs):
		def parsePath(value):
			if value == '':
				return ''
			return self.resolvePath(value, mustExist, 'Error resolving path %s' % value)
		obj2str = str.__str__
		str2obj = parsePath
		if storeRelative:
			obj2str = lambda value: os.path.relpath(value, self.getWorkPath())
			str2obj = lambda value: os.path.join(self.getWorkPath(), parsePath(value))
		return self._getInternal('path', obj2str, str2obj, None, option, default, **kwargs)

	# Return multiple resolved paths (each line processed same as getPath)
	def getPaths(self, option, default = noDefault, mustExist = True, **kwargs):
		def patlist2pathlist(value, mustExist):
			try:
				for pattern in value:
					for fn in utils.resolvePaths(pattern, self._configView.pathDict.get('search_paths', []), mustExist, ConfigError):
						yield fn
			except Exception:
				raise ConfigError('Error resolving pattern %s' % pattern)

		str2obj = lambda value: list(patlist2pathlist(utils.parseList(value, None, onEmpty = []), mustExist))
		obj2str = lambda value: '\n' + str.join('\n', patlist2pathlist(value, False))
		return self._getInternal('paths', obj2str, str2obj, None, option, default, **kwargs)

	# Return class - default class is also given in string form!
	def getPlugin(self, option, default = noDefault, cls = Plugin, tags = [], inherit = False, defaultName = '', **kwargs):
		str2obj = lambda value: ClassWrapper(cls, value, self, tags, inherit, defaultName, self._getPluginPaths())
		return self._getInternal('plugin', str, str2obj, str2obj, option, default, **kwargs)

	# Return composite class - default classes are also given in string form!
	def getCompositePlugin(self, option, default = noDefault, default_compositor = noDefault, option_compositor = None,
			cls = Plugin, tags = [], inherit = False, defaultName = '', **kwargs):
		parseSingle = lambda value: ClassWrapper(cls, value, self, tags, inherit, defaultName, self._getPluginPaths())
		str2obj = lambda value: map(parseSingle, utils.parseList(value, None, onEmpty = []))
		obj2str = lambda value: str.join('\n', map(str, value))
		clsList = self._getInternal('composite plugin', obj2str, str2obj, str2obj, option, default, **kwargs)
		if len(clsList) == 1:
			return clsList[0]
		if not option_compositor:
			option_compositor = appendOption(option, 'manager')
		clsCompositor = self.getPlugin(option_compositor, default_compositor, cls, tags, inherit, defaultName, **kwargs)
		return CompositedClassWrapper(clsCompositor, clsList)


# Filter expression class
class FilterBase(Plugin):
	def __init__(self, filterExpr):
		pass

	def filterList(self, value):
		raise AbstractError

class SimpleConfigInterface(TypedConfigInterface):
	def getFilter(self, option, pluginName):
		filterExpr = self.getList(option, [])
		filterCls = self.getPlugin(appendOption(option, 'plugin'), pluginName, cls = FilterBase)
		return filterCls.getInstance(filterExpr)

	# Get state - bool stored in hidden "state" section - any given detail overrides global state
	def getState(self, statename, detail = '', default = False):
		view = self.changeView(viewClass = SimpleConfigView, setSections = ['state'])
		state = view.getBool('#%s' % statename, default, onChange = None)
		if detail:
			state = view.getBool('#%s %s' % (statename, detail), state, onChange = None)
		return state
	# Set state - bool stored in hidden "state" section
	def setState(self, value, statename, detail = ''):
		option = ('#%s %s' % (statename, detail)).strip()
		view = self.changeView(viewClass = SimpleConfigView, setSections = ['state'])
		return view.set(option, str(value), '=')

	def getChoice(self, option, choices, default = noDefault,
			obj2str = str.__str__, str2obj = str, def2obj = None, onValid = None, **kwargs):
		default_str = self._getDefaultStr(default, def2obj, obj2str)
		capDefault = lambda value: utils.QM(value == default_str, value.upper(), value.lower())
		choices_str = str.join('/', map(capDefault, map(obj2str, choices)))
		if (default != noDefault) and (default not in choices):
			raise APIError('Invalid default choice "%s" [%s]!' % (default, choices_str))
		if 'interactive' in kwargs:
			kwargs['interactive'] += (' [%s]' % choices_str)
		def myOnValid(loc, obj):
			if obj not in choices:
				raise ConfigError('Invalid choice "%s" [%s]!' % (obj, choices_str))
			if onValid:
				return onValid(loc, obj)
			return obj
		return self._getInternal('choice', obj2str, str2obj, def2obj, option, default,
			onValid = myOnValid, interactiveDefault = False, **kwargs)
	def setChoice(self, option, value, opttype = '=', source = None, obj2str = str.__str__):
		return self._setInternal('choice', obj2str, option, value, opttype, source)

	def getChoiceYesNo(self, option, default = noDefault, **kwargs):
		return self.getChoice(option, [True, False], default,
			obj2str = lambda obj: {True: 'yes', False: 'no'}.get(obj), str2obj = utils.parseBool, **kwargs)

	def getEnum(self, option, enum, default = noDefault, subset = None, **kwargs):
		choices = enum.enumValues
		if subset:
			choices = subset
		return self.getChoice(option, choices, default, obj2str = enum.enum2str, str2obj = enum.str2enum, **kwargs)

	def _getInternal(self, desc, obj2str, str2obj, def2obj, option, default_obj,
			interactive = None, interactiveDefault = True, **kwargs):
		if (not interactive) or (option in self.getOptions()):
			return TypedConfigInterface._getInternal(self, desc, obj2str, str2obj, def2obj, option, default_obj, **kwargs)
		prompt = interactive
		if (default_obj != noDefault) and interactiveDefault:
			prompt += (' [%s]' % self._getDefaultStr(default_obj, def2obj, obj2str))
		while True:
			try:
				userInput = user_input('%s: ' % prompt)
			except Exception:
				sys.stdout.write('\n')
				sys.exit(os.EX_DATAERR)
			if userInput == '':
				obj = default_obj
			else:
				try:
					obj = str2obj(userInput)
				except Exception:
					raise UserError('Unable to parse %s: %s' % (desc, userInput))
					continue
			break
		return TypedConfigInterface._getInternal(self, desc, obj2str, str2obj, def2obj, option, obj, **kwargs)
