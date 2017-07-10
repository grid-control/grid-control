# | Copyright 2016-2017 Karlsruhe Institute of Technology
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
from grid_control.config.chandlers_base import TriggerAbort
from grid_control.config.config_entry import ConfigEntry, ConfigError, norm_config_locations
from hpfwk import APIError
from python_compat import unspecified, when_unspecified


class ConfigInterface(object):
	# Config interface class accessing typed data using an string interface provided by config_view
	def __init__(self, config_view, default_on_change=unspecified, default_on_valid=unspecified):
		self._config_view = config_view
		self._default_on_change = when_unspecified(default_on_change, TriggerAbort())
		self._default_on_valid = when_unspecified(default_on_valid, None)
		self._log = logging.getLogger('config.%s' % self._config_view.config_name.lower())

	def __repr__(self):
		return '<%s(view = %s)>' % (self.__class__.__name__, self._config_view)

	def change_view(self, interface_cls=None,
			default_on_change=unspecified, default_on_valid=unspecified, **kwargs):
		interface_cls = interface_cls or self.__class__
		return interface_cls(self._config_view.get_view(**kwargs),
			when_unspecified(default_on_change, self._default_on_change),
			when_unspecified(default_on_valid, self._default_on_valid))

	def get(self, option, default=unspecified, obj2str=str.__str__, str2obj=str, **kwargs):
		# Getting string config options - whitespace around the value will get discarded
		return self._get_internal('string', obj2str, str2obj, None, option, default, **kwargs)

	def get_config_name(self):
		return self._config_view.config_name

	def get_option_list(self):
		# Get all selected options
		result = []
		for entry in self._config_view.iter_entries():
			if entry.option not in result:
				result.append(entry.option)
		return result

	def get_work_path(self, *fn_list):
		return os.path.join(self._config_view.config_vault['path:work_dn'], *fn_list)

	def set(self, option, value, opttype='=', source=None, obj2str=str.__str__, section=None):
		# Setting string config options - whitespace around the value will get discarded
		return self._set_internal('string', obj2str, option, value, opttype, source, section)

	def write(self, stream, **kwargs):
		# Write settings to file
		return self._config_view.write(stream, **kwargs)

	def _get_caller(self):
		# Find config caller
		for frame in inspect.stack():
			if ('/packages/grid_control/config/' not in frame[1]) or frame[0].f_locals.get('self'):
				if isinstance(frame[0].f_locals.get('self'), self.__class__):
					continue
				if frame[0].f_locals.get('self'):
					return '%s::%s' % (frame[0].f_locals.get('self').__class__.__name__, frame[3])
				return frame[3]

	def _get_default_str(self, default_obj, def2obj, obj2str):
		# First transform default into string if applicable
		if not unspecified(default_obj):
			try:
				if def2obj:
					default_obj = def2obj(default_obj)
			except Exception:
				raise APIError('Unable to convert default object: %s' % repr(default_obj))
			try:
				result = obj2str(default_obj)
				if not isinstance(result, str):
					raise APIError('Default string representation function returned %r' % result)
				return result
			except Exception:
				raise APIError('Unable to get string representation of default object: %s' % repr(default_obj))
		return unspecified

	def _get_internal(self, desc, obj2str, str2obj, def2obj, option, default_obj,
			on_change=unspecified, on_valid=unspecified, persistent=False, override=None):
		if self._log.isEnabledFor(logging.DEBUG2):
			self._log.log(logging.DEBUG2, 'Config query from: %r', self._get_caller())
		# Make sure option is in a consistent format
		option_list = norm_config_locations(option)
		self._log.log(logging.DEBUG1, 'Config query for config option %r', str.join(' / ', option_list))
		if override:
			return str2obj(override)
		default_str = None
		try:
			default_str = self._get_default_str(default_obj, def2obj, obj2str)
			(old_entry, cur_entry) = self._config_view.get(option_list, default_str, persistent=persistent)
			return self._process_entries(old_entry, cur_entry, desc, obj2str, str2obj,
				when_unspecified(on_change, self._default_on_change),
				when_unspecified(on_valid, self._default_on_valid))
		except Exception:
			if unspecified(default_obj):
				default_str = 'no default'  # pylint:disable=redefined-variable-type
			elif not default_str:
				default_str = repr(default_obj)
			raise ConfigError('Unable to get %r from option %r (%s)' % (desc,
				str.join(' / ', option_list), default_str))

	def _process_entries(self, old_entry, cur_entry, desc, obj2str, str2obj, on_change, on_valid):
		# Wrap parsing of object
		def _parse_entry(entry, entry_desc=''):
			try:
				obj = str2obj(entry.value)
				entry.value = obj2str(obj)  # Update value of entry with formatted data
				if not isinstance(entry.value, str):
					raise APIError('String representation function returned %r' % entry.value)
				return obj
			except Exception:
				raise ConfigError('Unable to parse %s: %s' % (entry_desc + desc,
					entry.format(print_section=True)))
		cur_obj = _parse_entry(cur_entry)

		# Notify about changes
		if on_change and old_entry:
			old_obj = _parse_entry(old_entry, 'stored ')
			if (old_obj == cur_obj) is False:
				# Passing self as first argument allows to limit reinits to current config view
				cur_obj = on_change(self, old_obj, cur_obj, cur_entry, obj2str)
				cur_entry.value = obj2str(cur_obj)
		if on_valid:
			return on_valid(cur_entry.format_opt(), cur_obj)
		return cur_obj

	def _set_internal(self, desc, obj2str, option, set_obj, opttype, source, section=None):
		try:
			value = obj2str(set_obj)
		except Exception:
			raise APIError('Unable to set %s %r - invalid object %s' % (desc, option, repr(set_obj)))
		try:
			if section is not None:
				scoped_iterface = self.change_view(view_class='SimpleConfigView', set_sections=[section])
				return scoped_iterface.set(option, value, opttype, source, str.__str__)
			if not source:
				source = '<%s by %s>' % (desc, self._get_caller())
			entry = self._config_view.set(norm_config_locations(option), value, opttype, source)
			self._log.log(logging.INFO2, 'Setting %s %s %s ', desc,
				ConfigEntry.map_opt_type2desc[opttype], entry.format(print_section=True))
			return entry
		except Exception:
			raise ConfigError('Unable to set %s %r to %r (source: %r)' % (desc, option, value, source))
