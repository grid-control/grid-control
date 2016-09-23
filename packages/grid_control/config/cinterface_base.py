# | Copyright 2016 Karlsruhe Institute of Technology
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

import os, inspect, logging
from grid_control.config.chandlers_base import changeImpossible
from grid_control.config.config_entry import ConfigEntry, ConfigError, norm_config_locations
from hpfwk import APIError
from python_compat import unspecified, when_unspecified


# Config interface class accessing typed data using an string interface provided by configView
class ConfigInterface(object):
	def __init__(self, configView, default_on_change = unspecified, default_on_valid = unspecified):
		self._config_view = configView
		self._default_on_change = when_unspecified(default_on_change, changeImpossible)
		self._default_on_valid = when_unspecified(default_on_valid, None)
		self._log = logging.getLogger('config.%s' % self._config_view.configName.lower())

	def __repr__(self):
		return '<%s(view = %s)>' % (self.__class__.__name__, self._config_view)

	def changeView(self, interfaceClass = None, default_on_change = unspecified, default_on_valid = unspecified, **kwargs):
		interfaceClass = interfaceClass or self.__class__
		return interfaceClass(self._config_view.getView(**kwargs),
			when_unspecified(default_on_change, self._default_on_change),
			when_unspecified(default_on_valid, self._default_on_valid))

	def getConfigName(self):
		return self._config_view.configName

	def getWorkPath(self, *fnList):
		return os.path.join(self._config_view.config_vault['path:workdir'], *fnList)

	# Get all selected options
	def getOptions(self):
		result = []
		for entry in self._config_view.iterContent():
			if entry.option not in result:
				result.append(entry.option)
		return result

	# Write settings to file
	def write(self, stream, **kwargs):
		return self._config_view.write(stream, **kwargs)

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
		if not unspecified(default_obj):
			try:
				if def2obj:
					default_obj = def2obj(default_obj)
			except Exception:
				raise APIError('Unable to convert default object: %s' % repr(default_obj))
			try:
				result = obj2str(default_obj)
				assert(isinstance(result, str))
				return result
			except Exception:
				raise APIError('Unable to get string representation of default object: %s' % repr(default_obj))
		return unspecified

	def _processEntries(self, old_entry, cur_entry, desc, obj2str, str2obj, onChange, onValid):
		# Wrap parsing of object
		def parseEntry(entry, entry_desc = ''):
			try:
				obj = str2obj(entry.value)
				entry.value = obj2str(obj) # Update value of entry with formatted data
				assert(isinstance(entry.value, str))
				return obj
			except Exception:
				raise ConfigError('Unable to parse %s: %s' % (entry_desc + desc,
					entry.format(print_section = True)))
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
			onChange = unspecified, onValid = unspecified, persistent = False):
		# Make sure option is in a consistent format
		option_list = norm_config_locations(option)
		try:
			if self._log.isEnabledFor(logging.DEBUG2):
				self._log.log(logging.DEBUG2, 'Config query from: %r', self._getCaller())
			default_str = self._getDefaultStr(default_obj, def2obj, obj2str)
			assert(unspecified(default_str) or isinstance(default_str, str))

			self._log.log(logging.DEBUG1, 'Config query for config option %r', str.join(' / ', option_list))
			(old_entry, cur_entry) = self._config_view.get(option_list, default_str, persistent = persistent)
			if onChange == unspecified:
				onChange = self._default_on_change
			if onValid == unspecified:
				onValid = self._default_on_valid
			return self._processEntries(old_entry, cur_entry, desc, obj2str, str2obj, onChange, onValid)
		except Exception:
			if unspecified(default_obj):
				raise ConfigError('Unable to get %r from option %r (no default)' % (desc, str.join(' / ', option_list)))
			raise ConfigError('Unable to get %r from option %r (default: %r)' % (desc, str.join(' / ', option_list), repr(default_obj)))

	def _setInternal(self, desc, obj2str, option, set_obj, opttype, source):
		try:
			if not source:
				source = '<%s by %s>' % (desc, self._getCaller())
			try:
				value = obj2str(set_obj)
			except Exception:
				raise APIError('Unable to get string representation of set value: %s' % repr(set_obj))
			entry = self._config_view.set(norm_config_locations(option), value, opttype, source)
			self._log.log(logging.INFO2, 'Setting %s %s %s ', desc, ConfigEntry.OptTypeDesc[opttype], entry.format(print_section = True))
			return entry
		except Exception:
			raise ConfigError('Unable to set %s %r to %r (source: %r)' % (desc, option, repr(set_obj), source))

	# Handling string config options - whitespace around the value will get discarded
	def get(self, option, default = unspecified, obj2str = str.__str__, str2obj = str, **kwargs):
		return self._getInternal('string', obj2str, str2obj, None, option, default, **kwargs)
	def set(self, option, value, opttype = '=', source = None, obj2str = str.__str__):
		return self._setInternal('string', obj2str, option, value, opttype, source)
