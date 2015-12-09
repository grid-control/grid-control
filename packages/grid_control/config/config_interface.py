#-#  Copyright 2014-2015 Karlsruhe Institute of Technology
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

import os, sys, inspect, logging
from grid_control import utils
from grid_control.abstract import ClassWrapper, LoadableObject
from grid_control.config.chandlers_base import changeImpossible
from grid_control.config.config_entry import noDefault, standardConfigForm
from grid_control.config.cview_base import SimpleConfigView
from grid_control.exceptions import APIError, ConfigError, RethrowError
from python_compat import user_input

# Config interface class accessing typed data using an string interface provided by configView
class TypedConfigInterface(object):
	defaultOnChange = changeImpossible
	defaultOnValid = None

	def __init__(self, configView):
		self._configView = configView
		self._log = logging.getLogger('config.%s' % self._configView.configName)

	def __repr__(self):
		return '<%s(view = %s)>' % (self.__class__.__name__, self._configView)

	def changeView(self, interfaceClass = None, **kwargs):
		if not interfaceClass:
			interfaceClass = self.__class__
		return interfaceClass(self._configView.getView(**kwargs))

	def getConfigName(self):
		return self._configView.configName

	def getWorkPath(self, *fnList):
		return os.path.join(self._configView.pathDict['<WORKDIR>'], *fnList)

	# Get all selected options
	def getOptions(self):
		result = []
		for entry in self._configView.iterContent():
			if entry.option not in result:
				result.append(entry.option)
		return result

	# Write settings to file
	def write(self, stream, **kwargs):
		return self._configView.write(stream, self._configView.iterContent(), **kwargs)

	# Find config caller
	def _getCaller(self):
		for frame in inspect.stack():
			if ('/packages/grid_control/config/' not in frame[1]) or frame[0].f_locals.get('self'):
				if isinstance(frame[0].f_locals.get('self'), self.__class__):
					continue
				if frame[0].f_locals.get('self'):
					return '%s::%s' % (frame[0].f_locals.get('self').__class__.__name__, frame[3])
				return frame[3]

	def _getDefaultStr(self, default_obj, def2obj, obj2str):
		# First transform default into string if applicable
		if default_obj != noDefault:
			try:
				if def2obj:
					default_obj = def2obj(default_obj)
			except Exception:
				raise APIError('Unable to convert default object: %s' % repr(default_obj))
			try:
				return obj2str(default_obj)
			except Exception:
				raise APIError('Unable to get string representation of default object: %s' % repr(default_obj))
		return noDefault

	def _processEntries(self, old_entry, cur_entry, desc, obj2str, str2obj, onChange, onValid):
		# Wrap parsing of object
		def parseEntry(entry, entry_desc = ''):
			try:
				obj = str2obj(entry.value)
				entry.value = obj2str(obj) # Update value of entry with formatted data
				return obj
			except Exception:
				raise RethrowError('Unable to parse %s: %s' % (entry_desc + desc,
					entry.format(printSection = True)), ConfigError)
		cur_obj = parseEntry(cur_entry)

		# Notify about changes
		if onChange and old_entry:
			old_obj = parseEntry(old_entry, 'stored ')
			if not (old_obj == cur_obj):
				# Passing self as first argument allows to limit reinits to current config view
				cur_obj = onChange(self, old_obj, cur_obj, cur_entry, obj2str)
				cur_entry.value = obj2str(cur_obj)
		if onValid:
			return onValid(cur_entry.format_opt(), cur_obj)
		return cur_obj

	def _getInternal(self, desc, obj2str, str2obj, def2obj, option, default_obj,
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		self._log.log(logging.DEBUG2, 'Config query from: "%s"' % self._getCaller())
		default_str = self._getDefaultStr(default_obj, def2obj, obj2str)

		# Make sure option is in a consistent format
		option_list = standardConfigForm(option)
		self._log.log(logging.DEBUG1, 'Config query for config option "%s"' % str.join(' / ', option_list))
		(old_entry, cur_entry) = self._configView.get(option_list, default_str, persistent = persistent)
		return self._processEntries(old_entry, cur_entry, desc, obj2str, str2obj, onChange, onValid)

	def _setInternal(self, desc, obj2str, option, set_obj, opttype, source):
		mode = {'?=': 'default', '+=': 'append', '^=': 'prepend', '=': 'override'}.get(opttype, 'set')
		if not source:
			source = '<%s by %s>' % (desc, self._getCaller())
		try:
			value = obj2str(set_obj)
		except Exception:
			raise APIError('Unable to get string representation of set value: %s' % repr(set_obj))
		entry = self._configView.set(standardConfigForm(option), value, opttype, source)
		self._log.log(logging.INFO2, 'Setting %s %s %s ' % (desc, mode, entry.format(printSection = True)))
		return entry

	# Handling string config options - whitespace around the value will get discarded
	def get(self, option, default = noDefault, obj2str = str.__str__, str2obj = str, **kwargs):
		return self._getInternal('string', obj2str, str2obj, None, option, default, **kwargs)
	def set(self, option, value, opttype = '=', source = None, obj2str = str.__str__):
		return self._setInternal('string', obj2str, option, value, opttype, source)

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
			raise RethrowError(errorMsg, ConfigError)

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
				raise RethrowError('Error resolving pattern %s' % pattern, ConfigError)

		str2obj = lambda value: list(patlist2pathlist(utils.parseList(value, None, onEmpty = []), mustExist))
		obj2str = lambda value: '\n' + str.join('\n', patlist2pathlist(value, False))
		return self._getInternal('paths', obj2str, str2obj, None, option, default, **kwargs)

	# Return class - default class is also given in string form!
	def getClass(self, option, default = noDefault, cls = LoadableObject, tags = [], inherit = False, defaultName = '', **kwargs):
		str2obj = lambda value: ClassWrapper(cls, value, self, tags, inherit, defaultName)
		return self._getInternal('class', str, str2obj, str2obj, option, default, **kwargs)

	# Return classes - default classes are also given in string form!
	def getClassList(self, option, default = noDefault, cls = LoadableObject, tags = [], inherit = False, defaultName = '', **kwargs):
		parseSingle = lambda value: ClassWrapper(cls, value, self, tags, inherit, defaultName)
		str2obj = lambda value: map(parseSingle, utils.parseList(value, None, onEmpty = []))
		obj2str = lambda value: str.join('\n', map(str, value))
		return self._getInternal('class', obj2str, str2obj, str2obj, option, default, **kwargs)


class SimpleConfigInterface(TypedConfigInterface):
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
