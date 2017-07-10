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
from grid_control.utils.parsing import str_dict_linear
from hpfwk import APIError, NestedException, clear_current_exception
from python_compat import ichain, ifilter, imap, lchain, lidfilter, lmap, set, sorted, unspecified


class ConfigError(NestedException):
	pass


def join_config_locations(opt_first, *opt_list):
	if isinstance(opt_first, (list, tuple)):  # first option is a list - expand the first parameter
		if not opt_list:  # only first option -> clean and return
			return lmap(str.strip, opt_first)
		return lchain(imap(lambda opt: join_config_locations(opt.strip(), *opt_list), opt_first))
	if not opt_list:  # only first option -> clean and return
		return [opt_first.strip()]
	return lmap(lambda opt: (opt_first + ' ' + opt).strip(), join_config_locations(*opt_list))


def norm_config_locations(value):
	# return canonized section or option string
	if value is not None:
		if not isinstance(value, list):
			value = [value]
		return lmap(_norm_config_location, value)


class ConfigContainer(object):
	def __init__(self, name):
		self.enabled = True
		self._write_mode = True  # True: allowed | None: ignored | False: raise APIError
		self._counter = 0
		self._content = {}
		self._content_default = {}

	def append(self, entry):
		if self._write_mode is False:
			raise APIError('Config container is read-only!')
		elif self._write_mode is True:
			self._counter += 1
			entry.order = self._counter
			option_list = self._content.setdefault(entry.option, [])
			option_list.append(entry)

	def get_default_entry(self, entry):
		return self._content_default.get(entry.option, {}).get(entry.section)

	def get_entry(self, option, filter_fun):
		return ConfigEntry.combine_entries(self.iter_config_entries(option, filter_fun))

	def get_options(self):
		return sorted(set(ichain([self._content.keys(), self._content_default.keys()])))

	def iter_config_entries(self, option, filter_fun):
		source_list = [self._content.get(option, []), self._content_default.get(option, {}).values()]
		return ifilter(filter_fun, ichain(source_list))

	def protect(self, raise_on_change=True):
		if (self._write_mode is True) and raise_on_change:
			self._write_mode = False
		elif self._write_mode is True:
			self._write_mode = None

	def resolve(self):
		so_value_dict = self._get_value_dict()
		for option in self._content:
			for entry in self._content[option]:
				subst_dict = dict(so_value_dict.get('default', {}))
				subst_dict.update(so_value_dict.get('global', {}))
				subst_dict.update(so_value_dict.get(entry.section, {}))
				try:  # Protection for non-interpolation "%" in value
					value = entry.value.replace('%', '\x01')  # protect %
					value = value.replace('\x01(', '%(') % subst_dict  # perform substitution
					value = value.replace('\x01', '%')  # unprotect %
				except Exception:
					raise ConfigError('Unable to interpolate value %r with %r' % (entry.value, subst_dict))
				if entry.value != value:
					entry.value = value
					entry.source = entry.source + ' [interpolated]'

	def set_default_entry(self, entry):
		entry_cur = self.get_default_entry(entry)
		if entry_cur and (entry_cur.value != entry.value):
			raise APIError('Inconsistent default values! (%r != %r)' % (entry_cur.value, entry.value))
		elif (self._write_mode is False) and not entry_cur:
			raise APIError('Config container is read-only!')
		elif self._write_mode is True:
			entry.order = 0
			self._content_default.setdefault(entry.option, {}).setdefault(entry.section, entry)

	def _get_value_dict(self):
		so_entries_dict = {}
		for option in self._content:
			for entry in self._content[option]:
				so_entries_dict.setdefault(entry.section, {}).setdefault(entry.option, []).append(entry)
		so_value_dict = {}
		for section in so_entries_dict:
			for option in so_entries_dict[section]:
				result = ''
				try:
					(entry, _) = ConfigEntry.process_entries(so_entries_dict[section][option])
					if entry:
						result = entry.value
				except ConfigError:  # eg. by '-=' without value
					clear_current_exception()
				so_value_dict.setdefault(section, {})[option] = result
		return so_value_dict


class ConfigEntry(object):
	# Holder of config information
	map_opt_type2desc = {'?=': 'default', '+=': 'append', '^=': 'prepend', '-=': 'replace',
		'=': 'override', '*=': 'finalize', '!=': 'modified'}

	def __init__(self, section, option, value, opttype, source,
			order=None, accessed=False, used=False):
		(self.section, self.option) = (_norm_config_location(section), _norm_config_location(option))
		(self.source, self.order) = (source.lower(), order)
		(self.value, self.opttype, self.accessed, self.used) = (value, opttype, accessed, used)

	def __repr__(self):
		return '%s(%s)' % (self.__class__.__name__, str_dict_linear(self.__dict__))

	def combine_entries(cls, entry_iter):
		(result, entry_list_used) = cls._process_and_mark_entries(entry_iter)
		for entry in entry_list_used:
			entry.used = True
		return result
	combine_entries = classmethod(combine_entries)

	def format(self, print_section=False, print_default=False,
			default=unspecified, source='', wraplen=33):
		if unspecified(self.value) or (not print_default and (self.value == default)):
			return ''
		if print_section:
			prefix = '[%s] %s' % (self.section, self.option)
		else:
			prefix = self.option
		prefix += ' %s' % self.opttype

		line_list = lidfilter(imap(str.strip, self.value.strip().splitlines()))
		if len(line_list) == 1:
			line_list = [prefix + ' ' + line_list[0]]  # everything on one line
		else:
			line_list.insert(0, prefix)  # prefix on first line - rest on other lines

		result = ''
		for line in line_list:
			if not result:  # first line
				if source and (len(line) >= wraplen):
					result += '; source: ' + source + '\n'
				elif source:
					result = line.ljust(wraplen) + '  ; ' + source + '\n'
					continue
			else:
				result += '\t'
			result += line + '\n'
		return result.rstrip()

	def format_opt(self):
		if '!' in self.section:
			return '<%s> %s' % (self.section.replace('!', ''), self.option)
		return '[%s] %s' % (self.section, self.option)

	def process_entries(cls, entry_iter, apply_modifiers=True):
		entry_list = list(entry_iter)
		try:
			return cls._process_entries(entry_list, apply_modifiers)
		except Exception:
			raise ConfigError('Error while processing:\n\t' + str.join('\n\t',
				imap(lambda entry: entry.format(print_section=True), entry_list)))
	process_entries = classmethod(process_entries)

	def simplify_entries(cls, entry_iter):
		# called to simplify entries for a specific option *and* section - sorted by order
		(result_base, entry_list_used) = cls._process_and_mark_entries(entry_iter, apply_modifiers=False)

		# Merge subsequent += and ^= entries
		def _merge_subsequent_entries(entry_iter):
			prev_entry = None
			for entry in entry_iter:
				entry = ConfigEntry(entry.section, entry.option, entry.value, entry.opttype,
					'<processed>', entry.order, entry.accessed, entry.used)  # copy entry for modifications
				if prev_entry and (entry.opttype == prev_entry.opttype):
					if entry.opttype == '+=':
						entry.value = prev_entry.value + '\n' + entry.value
					elif entry.opttype == '^=':
						entry.value = entry.value + '\n' + prev_entry.value
					else:
						yield prev_entry
				elif prev_entry:
					yield prev_entry
				prev_entry = entry
			if prev_entry:
				yield prev_entry

		if entry_list_used[0].opttype in ['*=', '!=', '?=', '=']:
			result = [cls.combine_entries(entry_list_used)]
		else:
			result = list(_merge_subsequent_entries(entry_list_used))

		try:
			(result_base, entry_list_used) = cls._process_and_mark_entries(entry_iter, apply_modifiers=True)
			(result_simplified, used_simplified) = cls._process_and_mark_entries(result)
		except ConfigError:  # unable to simplify pure modification list
			clear_current_exception()
			return result
		assert len(used_simplified) == len(result)
		if result_simplified and result_base:
			assert result_simplified.value.strip() == result_base.value.strip()
		else:
			assert result_simplified == result_base
		return result
	simplify_entries = classmethod(simplify_entries)

	def _apply_modifiers(cls, entry, modifier_list):
		def _create(base, value):
			return cls(base.section, base.option, value, '=', '<processed>')
		for modifier in modifier_list:
			if modifier.opttype == '+=':
				if entry:
					entry = _create(entry, entry.value + '\n' + modifier.value)
				else:
					entry = _create(modifier, modifier.value)
			elif modifier.opttype == '^=':
				if entry:
					entry = _create(entry, modifier.value + '\n' + entry.value)
				else:
					entry = _create(modifier, modifier.value)
			elif modifier.opttype == '-=':
				if modifier.value.strip() == '':  # without arguments: remove all entries up to this entry
					entry = None
				elif entry is not None:  # with arguments: remove string from current value
					entry = _create(entry, entry.value.replace(modifier.value.strip(), ''))
				else:
					raise ConfigError('Unable to substract "%s" from non-existing value!' % modifier.format_opt())
		return entry
	_apply_modifiers = classmethod(_apply_modifiers)

	def _process_and_mark_entries(cls, entry_iter, apply_modifiers=True):
		entry_list = list(entry_iter)
		for entry in entry_list:
			entry.accessed = True
		return cls.process_entries(entry_list, apply_modifiers)
	_process_and_mark_entries = classmethod(_process_and_mark_entries)

	def _process_entries(cls, entry_iter, apply_modifiers=True):
		entry_iter = iter(entry_iter)
		result = None
		entry_list_used = []
		modifier_list = []
		for entry in entry_iter:
			if entry.opttype == '-=':  # context sensitive option
				if entry.value.strip() == '':  # set-like option
					entry_list_used = [entry]
					result = None
					modifier_list = []
				else:
					modifier_list.append(entry)
			elif entry.opttype in ['+=', '^=']:  # modifier options
				modifier_list.append(entry)
			elif entry.opttype in ['*=', '!=', '?=', '=']:  # set options
				if entry.opttype == '*=':  # this option can not be changed by other config entries
					return _discard_following(entry, entry_iter)
				elif entry.opttype == '=':  # set but don't apply collected modifiers
					# subsequent modifiers apply!
					entry_list_used = [entry]
					result = entry
					modifier_list = []
				elif entry.opttype == '?=':  # Conditional set with applied modifiers
					if not result:
						entry_list_used = [entry] + modifier_list
						result = cls._apply_modifiers(entry, modifier_list)
						modifier_list = []
				elif entry.opttype == '!=':  # set and apply collected modifiers
					entry_list_used = [entry] + modifier_list
					result = cls._apply_modifiers(entry, modifier_list)
					modifier_list = []
		entry_list_used.extend(modifier_list)
		if apply_modifiers and modifier_list:  # apply remaining modifiers - result can be None
			result = cls._apply_modifiers(result, modifier_list)
		return (result, entry_list_used)
	_process_entries = classmethod(_process_entries)


def _discard_following(entry, entry_iter):
	log = logging.getLogger('config')
	log.debug('The following matching entries following %s are discarded:%s',
		entry.format(print_section=True),
		str.join('', imap(lambda e: '\n\t' + e.format(print_section=True), entry_iter)))
	return (entry, [entry])


def _norm_config_location(value):
	value = value.lower().strip().replace('\t', ' ')
	while '  ' in value:
		value = value.replace('  ', ' ')
	return value
