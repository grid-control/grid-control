#-#  Copyright 2014 Karlsruhe Institute of Technology
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

import os, logging, inspect
from grid_control import utils, APIError, ConfigError, RethrowError, ClassWrapper, LoadableObject
from config_entry import standardConfigForm, noDefault
from chandlers_base import changeImpossible
from cview_base import SimpleConfigView

# Config interface class accessing typed data using an string interface provided by configView
class TypedConfigInterface(object):
	defaultOnChange = changeImpossible
	defaultOnValid = None

	def __init__(self, configView):
		self._configView = configView
		self._log = logging.getLogger('config.%s' % self._configView.configName)

	def __str__(self):
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
			except:
				raise APIError('Unable to convert default object: %s' % repr(default_obj))
			try:
				return obj2str(default_obj)
			except:
				raise APIError('Unable to get string representation of default object: %s' % repr(default_obj))
		return noDefault

	def _getTyped(self, desc, obj2str, str2obj, def2obj, option, default_obj = noDefault,
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		self._log.log(logging.DEBUG2, 'Config query from: "%s"' % self._getCaller())
		default_str = self._getDefaultStr(default_obj, def2obj, obj2str)

		# Make sure option is in a consistent format
		option_list = standardConfigForm(option)
		self._log.log(logging.DEBUG1, 'Config query for config option "%s"' % str.join(' / ', option_list))
		(old_entry, cur_entry) = self._configView.get(option_list, default_str, persistent = persistent)

		# Wrap parsing of object
		def parseEntry(entry, entry_desc = ''):
			try:
				obj = str2obj(entry.value)
				entry.value = obj2str(obj) # Update value of entry with formatted data
				return obj
			except:
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

	def _setTyped(self, desc, obj2str, option, set_obj, opttype, source):
		mode = {'?=': 'default', '+=': 'append', '^=': 'prepend', '=': 'override'}.get(opttype, 'set')
		if not source:
			source = '<%s by %s>' % (desc, self._getCaller())
		try:
			value = obj2str(set_obj)
		except:
			raise APIError('Unable to get string representation of set value: %s' % repr(set_obj))
		entry = self._configView.set(standardConfigForm(option), value, opttype, source)
		self._log.log(logging.INFO2, 'Setting %s %s %s ' % (desc, mode, entry.format(printSection = True)))
		return entry

	def get(self, option, default = noDefault, obj2str = str.__str__, str2obj = str,
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		return self._getTyped('string', obj2str, str2obj, None, option, default,
			onChange, onValid, persistent) # surrounding spaces will get discarded
	def set(self, option, value, opttype = '=', source = None):
		return self._setTyped('string', str.__str__, option, value, opttype, source)

	def getInt(self, option, default = noDefault,
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		return self._getTyped('int', int.__str__, int, None, option, default,
			onChange, onValid, persistent) # using strict integer (de-)serialization
	def setInt(self, option, value, opttype = '=', source = None):
		return self._setTyped('int', int.__str__, option, value, opttype, source)

	def getBool(self, option, default = noDefault,
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		def str2obj(value): # Feature: true and false are not the only valid expressions ...
			result = utils.parseBool(value)
			if result == None:
				raise ConfigError('Valid boolean expressions are: "true", "false"')
			return result
		return self._getTyped('bool', bool.__str__, str2obj, None, option, default,
			onChange, onValid, persistent)
	def setBool(self, option, value, opttype = '=', source = None):
		return self._setTyped('bool', bool.__str__, option, value, opttype, source)

	# Get time in seconds - input base is hours
	def getTime(self, option, default = noDefault,
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		def str2obj(value):
			try:
				return utils.parseTime(value) # empty or negative values are mapped to -1
			except:
				raise ConfigError('Valid time expressions have the format: hh[:mm[:ss]]')
		return self._getTyped('time', utils.strTimeShort, str2obj, None, option, default,
			onChange, onValid, persistent)
	def setTime(self, option, value, opttype = '=', source = None):
		return self._setTyped('time', utils.strTimeShort, option, value, opttype, source)

	# Returns a tuple with (<dictionary>, <keys>) - the keys are sorted by order of appearance
	# Default key is accessed via key == None (None is never in keys!)
	def getDict(self, option, default = noDefault, parser = lambda x: x, strfun = lambda x: x,
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
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
		return self._getTyped('dictionary', obj2str, str2obj, def2obj, option, default,
			onChange, onValid, persistent)

	# Get whitespace separated list (space, tab, newline)
	def getList(self, option, default = noDefault, parseItem = lambda x: x,
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		obj2str = lambda value: '\n' + str.join('\n', map(str, value))
		str2obj = lambda value: map(parseItem, utils.parseList(value, None))
		return self._getTyped('list', obj2str, str2obj, None, option, default,
			onChange, onValid, persistent)

	# Resolve path
	def resolvePath(self, value, mustExist, errorMsg):
		try:
			return utils.resolvePath(value, self._configView.pathDict.get('search', []), mustExist, ConfigError)
		except:
			raise RethrowError(errorMsg, ConfigError)

	# Return resolved path (search paths: $PWD, <gc directory>, <base path from constructor>)
	def getPath(self, option, default = noDefault, mustExist = True,
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		def parsePath(value):
			if value == '':
				return ''
			return self.resolvePath(value, mustExist, 'Error resolving path %s' % value)
		return self._getTyped('path', str.__str__, parsePath, None, option, default,
			onChange, onValid, persistent)

	# Return multiple resolved paths (each line processed same as getPath)
	def getPaths(self, option, default = noDefault, mustExist = True,
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		def patlist2pathlist(value, mustExist):
			try:
				for pattern in value:
					for fn in utils.resolvePaths(pattern, self._configView.pathDict.get('search', []), mustExist, ConfigError):
						yield fn
			except:
				raise RethrowError('Error resolving pattern %s' % pattern, ConfigError)

		str2obj = lambda value: list(patlist2pathlist(utils.parseList(value, None, onEmpty = []), mustExist))
		obj2str = lambda value: '\n' + str.join('\n', patlist2pathlist(value, False))
		return self._getTyped('paths', obj2str, str2obj, None, option, default,
			onChange, onValid, persistent)

	# Return class - default class is also given in string form!
	def getClass(self, option, default = noDefault, cls = LoadableObject, tags = [], inherit = False, defaultName = '',
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		str2obj = lambda value: ClassWrapper(cls, value, self, tags, inherit, defaultName)
		return self._getTyped('class', str, str2obj, str2obj, option, default,
			onChange, onValid, persistent)

	# Return classes - default classes are also given in string form!
	def getClassList(self, option, default = noDefault, cls = LoadableObject, tags = [], inherit = False, defaultName = '',
			onChange = defaultOnChange, onValid = defaultOnValid, persistent = False):
		parseSingle = lambda value: ClassWrapper(cls, value, self, tags, inherit, defaultName)
		str2obj = lambda value: map(parseSingle, utils.parseList(value, None, onEmpty = []))
		obj2str = lambda value: str.join('\n', map(str, value))
		return self._getTyped('class', obj2str, str2obj, str2obj, option, default,
			onChange, onValid, persistent)

	# Get state - bool stored in hidden "state" section - any given detail overrides global state
	def getState(self, statename = 'init', detail = '', default = False):
		view = self.changeView(viewClass = SimpleConfigView, setSections = ['state'])
		state = view.getBool('#%s' % statename, default, onChange = None)
		if detail:
			state = view.getBool('#%s %s' % (statename, detail), state, onChange = None)
		return state

	# Set state - bool stored in hidden "state" section
	def setState(self, value, statename = 'init', detail = ''):
		option = ('#%s %s' % (statename, detail)).strip()
		view = self.changeView(viewClass = SimpleConfigView, setSections = ['state'])
		return view.set(option, str(value), '=')
