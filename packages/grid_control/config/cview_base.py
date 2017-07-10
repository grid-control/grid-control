# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

import logging
from grid_control.config.config_entry import ConfigEntry, ConfigError, norm_config_locations
from hpfwk import AbstractError, Plugin
from python_compat import ichain, imap, lfilter, sorted, unspecified


class ConfigView(Plugin):
	def __init__(self, name, parent=None):
		self.config_vault = {}
		# used shared config vault from parent if present
		self.config_vault = (parent or self).config_vault
		self.set_config_name(name)

	def get(self, option_list, default_str, persistent):
		# Return old and current merged config entries
		raise AbstractError

	def get_view(self, view_class=None, **kwargs):
		raise AbstractError

	def iter_entries(self):
		raise AbstractError

	def set(self, option, value, opttype, source):
		raise AbstractError

	def set_config_name(self, name):
		self.config_name = name
		self._log = logging.getLogger('config.%s' % name.lower())

	def write(self, stream, print_minimal=False, print_source=False, print_oneline=False, **kwarg):
		config = self._prepare_write(**kwarg)
		for section in sorted(config):
			if not print_oneline:
				stream.write('[%s]\n' % section)
			for option in sorted(config[section]):
				entry_list = sorted(config[section][option], key=lambda e: e.order)
				if print_minimal:
					entry_list = ConfigEntry.simplify_entries(entry_list)
				for entry in entry_list:
					source = ''
					if print_source:
						source = entry.source
					stream.write(entry.format(print_section=print_oneline, source=source) + '\n')
			if not print_oneline:
				stream.write('\n')

	def _get_write_entries(self):
		return self.iter_entries()

	def _prepare_write(self, entries=None,
			print_state=False, print_unused=True, print_default=True, print_workdir=False):
		entries = entries or self._get_write_entries()
		result = {}
		for entry in entries:
			if print_workdir and entry.option == 'workdir':
				result.setdefault(entry.section, {}).setdefault(entry.option, []).append(entry)
			elif print_unused or entry.used:
				if print_default or not entry.source.startswith('<default'):
					if print_state or not entry.option.startswith('#'):
						result.setdefault(entry.section, {}).setdefault(entry.option, []).append(entry)
		return result


class HistoricalConfigView(ConfigView):
	# Historical config view
	def __init__(self, name, container_old, container_cur, parent=None):
		ConfigView.__init__(self, name, parent)
		self._container_old = container_old
		self._container_cur = container_cur

	def get(self, option_list, default_str, persistent):
		# Return old and current merged config entries
		entry_old = None
		if self._container_old.enabled:  # If old container is enabled => return stored entry
			entry_old = ConfigEntry.combine_entries(self._match_entries(self._container_old, option_list))
		# Process current entry
		(entry_default, entry_default_fallback) = self._get_default_entries(
			option_list, default_str, persistent, entry_old)
		entry_cur = self.get_entry(option_list, entry_default, entry_default_fallback)
		if entry_cur is None:
			raise ConfigError('"[%s] %s" does not exist!' % (
				self._get_section(specific=False), option_list[-1]))
		description = 'Using user supplied %s'
		if persistent and entry_default.used:
			description = 'Using persistent    %s'
		elif entry_default.used and (entry_cur.value != entry_default.value):
			description = 'Using modified default value %s'
		elif entry_default.used:
			description = 'Using default value %s'
		elif '!' in entry_cur.section:
			description = 'Using dynamic value %s'
		self._log.log(logging.INFO2, description, entry_cur.format(print_section=True))
		return (entry_old, entry_cur)

	def get_entry(self, option_list, entry_default, entry_default_fallback):
		if not unspecified(entry_default.value):
			self._container_cur.set_default_entry(entry_default)
		# Assemble matching config entries and combine them
		entries = self._match_entries(self._container_cur, option_list)
		if not unspecified(entry_default.value):
			entries.append(entry_default_fallback)
		self._log.log(logging.DEBUG1, 'Used config entries:')
		for entry in entries:
			self._log.log(logging.DEBUG1, '  %s (%s | %s)',
				entry.format(print_section=True), entry.source, entry.order)
		entry_cur = ConfigEntry.combine_entries(entries)
		# Ensure that fallback default value is stored in persistent storage
		if entry_default_fallback.used and not unspecified(entry_default.value):
			self._container_cur.set_default_entry(entry_default_fallback)
		return entry_cur

	def get_view(self, view_class=None, **kwargs):
		if not view_class:
			view_class = self.__class__
		elif isinstance(view_class, str):
			view_class = ConfigView.get_class(view_class)
		return view_class(self.config_name, self._container_old, self._container_cur, self, **kwargs)

	def iter_entries(self):
		return self._match_entries(self._container_cur)

	def set(self, option_list, value, opttype, source):
		entry = self._create_entry(option_list, value, opttype, source, specific=True, reverse=True)
		self._container_cur.append(entry)
		self._log.log(logging.INFO3, 'Setting option %s', entry.format(print_section=True))
		return entry

	def _create_entry(self, option_list, value, opttype, source, specific, reverse):
		section = self._get_section(specific)
		if reverse:
			section += '!'
		return ConfigEntry(section, option_list[-1], value, opttype, source)

	def _get_default_entries(self, option_list, default_str, persistent, entry_old):
		if persistent and entry_old:
			default_str = entry_old.value
		entry_default = self._create_entry(option_list, default_str,
			'?=', '<default>', specific=False, reverse=True)
		if persistent and not entry_old:
			entry_default = self._container_cur.get_default_entry(entry_default) or entry_default
		entry_default_fallback = self._create_entry(option_list, entry_default.value,
			'?=', '<default fallback>', specific=True, reverse=False)
		return (entry_default, entry_default_fallback)

	def _get_section(self, specific):
		raise AbstractError

	def _get_section_key(self, section):
		raise AbstractError

	def _match_entries(self, container, option_list=None):
		key_list = container.get_options()
		if option_list is not None:
			key_list = lfilter(key_list.__contains__, option_list)

		def _get_entry_key_ordered(entry):
			return (tuple(imap(_remove_none, _get_section_key_filtered(entry))), entry.order)

		def _get_section_key_filtered(entry):
			return self._get_section_key(entry.section.replace('!', '').strip())

		def _remove_none(key):
			if key is None:
				return -1
			return key

		def _select_sections(entry):
			return _get_section_key_filtered(entry) is not None

		result = []
		for key in key_list:
			(entries, entries_reverse) = ([], [])
			for entry in container.iter_config_entries(key, _select_sections):
				if entry.section.endswith('!'):
					entries_reverse.append(entry)
				else:
					entries.append(entry)
			result.extend(sorted(entries_reverse, key=_get_entry_key_ordered, reverse=True))
			result.extend(sorted(entries, key=_get_entry_key_ordered))
		return result


class SimpleConfigView(HistoricalConfigView):
	# Simple ConfigView implementation
	def __init__(self, name, container_old, container_cur, parent=None,
			set_sections=unspecified, add_sections=None):
		HistoricalConfigView.__init__(self, name, container_old, container_cur, parent or self)
		self._section_list = self._init_variable(parent or self, '_section_list', None,
			set_sections, add_sections, norm_config_locations)

	def __repr__(self):
		return '<%s(sections = %r)>' % (self.__class__.__name__, self._section_list)

	def _get_section(self, specific):
		if self._section_list and specific:
			return self._section_list[-1]
		elif self._section_list:
			return self._section_list[0]
		return 'global'

	def _get_section_key(self, section):
		if self._section_list is None:
			return (section,)
		if section in self._section_list:
			return (self._section_list.index(section),)

	def _init_variable(self, parent, member_name, default,
			set_value, add_value, norm_values, parse_value=lambda x: [x]):
		def _collect(value):
			return list(ichain(imap(parse_value, value)))
		# Setting initial value of variable
		result = default
		if hasattr(parent, member_name):  # get from parent if available
			result = getattr(parent, member_name)
		if set_value is None:
			result = set_value
		elif not unspecified(set_value):
			result = norm_values(list(_collect(set_value)))
		# Add to settings
		if add_value and (result is not None):
			result = result + norm_values(list(_collect(add_value)))
		elif add_value:
			result = norm_values(list(_collect(add_value)))
		setattr(self, member_name, result)
		return result
