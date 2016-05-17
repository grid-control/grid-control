# | Copyright 2014-2016 Karlsruhe Institute of Technology
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

import os, sys, signal
from grid_control import utils
from grid_control.config.cinterface_base import ConfigInterface
from grid_control.config.config_entry import ConfigError, appendOption, noDefault
from grid_control.config.cview_base import SimpleConfigView
from grid_control.config.matcher_base import DictLookup, ListFilter, ListOrder, Matcher
from grid_control.utils.data_structures import makeEnum
from grid_control.utils.parsing import parseBool, parseDict, parseList, parseTime, strDictLong, strTimeShort
from hpfwk import APIError, ExceptionCollector, Plugin
from python_compat import identity, ifilter, imap, lmap, relpath, sorted, user_input

# Config interface class accessing typed data using an string interface provided by configView
class TypedConfigInterface(ConfigInterface):
	# Handling integer config options - using strict integer (de-)serialization
	def getInt(self, option, default = noDefault, **kwargs):
		return self._getInternal('int', int.__str__, int, None, option, default, **kwargs)
	def setInt(self, option, value, opttype = '=', source = None):
		return self._setInternal('int', int.__str__, option, value, opttype, source)

	# Handling boolean config options - feature: true and false are not the only valid expressions
	def getBool(self, option, default = noDefault, **kwargs):
		def str2obj(value):
			result = parseBool(value)
			if result is None:
				raise ConfigError('Valid boolean expressions are: "true", "false"')
			return result
		return self._getInternal('bool', bool.__str__, str2obj, None, option, default, **kwargs)
	def setBool(self, option, value, opttype = '=', source = None):
		return self._setInternal('bool', bool.__str__, option, value, opttype, source)

	# Get time in seconds - input base is hours
	def getTime(self, option, default = noDefault, **kwargs):
		def str2obj(value):
			try:
				return parseTime(value) # empty or negative values are mapped to -1
			except Exception:
				raise ConfigError('Valid time expressions have the format: hh[:mm[:ss]]')
		return self._getInternal('time', strTimeShort, str2obj, None, option, default, **kwargs)
	def setTime(self, option, value, opttype = '=', source = None):
		return self._setInternal('time', strTimeShort, option, value, opttype, source)

	# Returns a tuple with (<dictionary>, <keys>) - the keys are sorted by order of appearance
	# Default key is accessed via key == None (None is never in keys!)
	def getDict(self, option, default = noDefault, parser = identity, strfun = str, **kwargs):
		obj2str = lambda value: strDictLong(value, parser, strfun)
		str2obj = lambda value: parseDict(value, parser)
		def2obj = lambda value: (value, sorted(ifilter(lambda key: key is not None, value.keys())))
		return self._getInternal('dictionary', obj2str, str2obj, def2obj, option, default, **kwargs)

	# Get whitespace separated list (space, tab, newline)
	def getList(self, option, default = noDefault, parseItem = identity, **kwargs):
		obj2str = lambda value: '\n' + str.join('\n', imap(str, value))
		str2obj = lambda value: lmap(parseItem, parseList(value, None))
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
			obj2str = lambda value: relpath(value, self.getWorkPath())
			str2obj = lambda value: os.path.join(self.getWorkPath(), parsePath(value))
		return self._getInternal('path', obj2str, str2obj, None, option, default, **kwargs)

	# Return multiple resolved paths (each line processed same as getPath)
	def getPaths(self, option, default = noDefault, mustExist = True, **kwargs):
		def patlist2pathlist(value, mustExist):
			ec = ExceptionCollector()
			for pattern in value:
				try:
					for fn in utils.resolvePaths(pattern, self._configView.pathDict.get('search_paths', []), mustExist, ConfigError):
						yield fn
				except Exception:
					ec.collect()
			ec.raise_any(ConfigError('Error resolving paths'))

		str2obj = lambda value: list(patlist2pathlist(parseList(value, None), mustExist))
		obj2str = lambda value: '\n' + str.join('\n', patlist2pathlist(value, False))
		return self._getInternal('paths', obj2str, str2obj, None, option, default, **kwargs)

	def _getPluginFactories(self, option, default = noDefault,
			cls = Plugin, tags = None, inherit = False, requirePlugin = True, singlePlugin = False,
			desc = 'plugin factories', **kwargs):
		if isinstance(cls, str):
			cls = Plugin.getClass(cls)
		def str2obj(value):
			objList = list(cls.bind(value, config = self, inherit = inherit, tags = tags or []))
			if singlePlugin and len(objList) > 1:
				raise ConfigError('This option only allows to specify a single plugin!')
			if requirePlugin and not objList:
				raise ConfigError('This option requires to specify a valid plugin!')
			return objList
		obj2str = lambda value: str.join('\n', imap(lambda obj: obj.bindValue(), value))
		return self._getInternal(desc, obj2str, str2obj, str2obj, option, default, **kwargs)

	# Return class - default class is also given in string form!
	def getPlugin(self, option, default = noDefault,
			cls = Plugin, tags = None, inherit = False, requirePlugin = True, pargs = None, pkwargs = None, **kwargs):
		factories = self._getPluginFactories(option, default, cls, tags, inherit, requirePlugin,
			singlePlugin = True, desc = 'plugin', **kwargs)
		if factories:
			return factories[0].getBoundInstance(*(pargs or ()), **(pkwargs or {}))

	# Return composite class - default classes are also given in string form!
	def getCompositePlugin(self, option, default = noDefault,
			default_compositor = noDefault, option_compositor = None,
			cls = Plugin, tags = None, inherit = False, requirePlugin = True,
			pargs = None, pkwargs = None, **kwargs):
		clsList = []
		for factory in self._getPluginFactories(option, default, cls, tags, inherit, requirePlugin,
				singlePlugin = False, desc = 'composite plugin', **kwargs):
			clsList.append(factory.getBoundInstance(*(pargs or ()), **(pkwargs or {})))
		if len(clsList) == 1:
			return clsList[0]
		elif not clsList: # requirePlugin == False
			return None
		if not option_compositor:
			option_compositor = appendOption(option, 'manager')
		return self.getPlugin(option_compositor, default_compositor, cls, tags, inherit,
			pargs = tuple([clsList] + list(pargs or [])), **kwargs)


CommandType = makeEnum(['executable', 'command'])

class SimpleConfigInterface(TypedConfigInterface):
	def getCommand(self, option, default = noDefault, **kwargs):
		scriptType = self.getEnum(appendOption(option, 'type'), CommandType, CommandType.executable, **kwargs)
		if scriptType == CommandType.executable:
			return self.getPath(option, default, **kwargs)
		return os.path.expandvars(self.get(option, default, **kwargs))

	def getLookup(self, option, default = noDefault,
			defaultMatcher = 'start', single = True, includeDefault = False, **kwargs):
		matcherArgs = {}
		if 'onChange' in kwargs:
			matcherArgs['onChange'] = kwargs['onChange']
		matcherOpt = appendOption(option, 'matcher')
		matcherObj = self.getPlugin(matcherOpt, defaultMatcher, cls = Matcher, pargs = (matcherOpt,), **matcherArgs)
		(sourceDict, sourceOrder) = self.getDict(option, default, **kwargs)
		return DictLookup(sourceDict, sourceOrder, matcherObj, single, includeDefault)

	def getFilter(self, option, default = noDefault, matchKey = None, negate = False, filterParser = str, filterStr = str.__str__,
			defaultMatcher = 'start', defaultFilter = 'strict', defaultOrder = ListOrder.source, **kwargs):
		matcherOpt = appendOption(option, 'matcher')
		matcherObj = self.getPlugin(matcherOpt, defaultMatcher, cls = Matcher, pargs = (matcherOpt,))
		filterExpr = self.get(option, default, str2obj = filterParser, obj2str = filterStr, **kwargs)
		filterOrder = self.getEnum(appendOption(option, 'order'), ListOrder, defaultOrder)
		return self.getPlugin(appendOption(option, 'filter'), defaultFilter, cls = ListFilter,
			pargs = (filterExpr, matcherObj, filterOrder, matchKey, negate))

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
			obj2str = str.__str__, str2obj = str, def2obj = None, **kwargs):
		default_str = self._getDefaultStr(default, def2obj, obj2str)
		capDefault = lambda value: utils.QM(value == default_str, value.upper(), value.lower())
		choices_str = str.join('/', imap(capDefault, imap(obj2str, choices)))
		if (default != noDefault) and (default not in choices):
			raise APIError('Invalid default choice "%s" [%s]!' % (default, choices_str))
		if 'interactive' in kwargs:
			kwargs['interactive'] += (' [%s]' % choices_str)
		def checked_str2obj(value):
			obj = str2obj(value)
			if obj not in choices:
				raise ConfigError('Invalid choice "%s" [%s]!' % (value, choices_str))
			return obj
		return self._getInternal('choice', obj2str, checked_str2obj, def2obj, option, default,
			interactiveDefault = False, **kwargs)
	def setChoice(self, option, value, opttype = '=', source = None, obj2str = str.__str__):
		return self._setInternal('choice', obj2str, option, value, opttype, source)

	def getChoiceYesNo(self, option, default = noDefault, **kwargs):
		return self.getChoice(option, [True, False], default,
			obj2str = {True: 'yes', False: 'no'}.get, str2obj = parseBool, **kwargs)

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
			handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
			try:
				userInput = user_input('%s: ' % prompt)
			except Exception:
				sys.stdout.write('\n')
				sys.exit(os.EX_DATAERR)
			signal.signal(signal.SIGINT, handler)
			if userInput == '':
				obj = default_obj
			else:
				try:
					obj = str2obj(userInput)
				except Exception:
					raise ConfigError('Unable to parse %s: %s' % (desc, userInput))
			break
		return TypedConfigInterface._getInternal(self, desc, obj2str, str2obj, def2obj, option, obj, **kwargs)
