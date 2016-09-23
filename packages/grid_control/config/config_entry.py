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

from grid_control.utils.parsing import str_dict
from hpfwk import APIError, NestedException, clear_current_exception
from python_compat import ichain, ifilter, imap, lfilter, lmap, set, sorted, unspecified


class ConfigError(NestedException):
	pass


def norm_config_locations(value):
	# return canonized section or option string
	if value is not None:
		if not isinstance(value, list):
			value = [value]
		return lmap(_norm_config_location, value)


def add_config_suffix(option, suffix):
	if isinstance(option, (list, tuple)):
		return lmap(lambda x: add_config_suffix(x, suffix), option)
	return option.rstrip() + ' ' + suffix


def _norm_config_location(value):
	value = value.lower().strip().replace('\t', ' ')
	while '  ' in value:
		value = value.replace('  ', ' ')
	return value


class ConfigEntry(object):
	# Holder of config information
	OptTypeDesc = {'?=': 'default', '+=': 'append', '^=': 'prepend', '-=': 'replace',
		'=': 'override', '*=': 'finalize', '!=': 'modified'}

	def __init__(self, section, option, value, opttype, source,
			order=None, accessed=False, used=False):
		(self.section, self.option) = (_norm_config_location(section), _norm_config_location(option))
		(self.source, self.order) = (source.lower(), order)
		(self.value, self.opttype, self.accessed, self.used) = (value, opttype, accessed, used)

	def __repr__(self):
		return '%s(%s)' % (self.__class__.__name__, str_dict(self.__dict__))

	def format_opt(self):
		if '!' in self.section:
			return '<%s> %s' % (self.section.replace('!', ''), self.option)
		return '[%s] %s' % (self.section, self.option)

	def format(self, print_section=False, print_default=False,
			default=unspecified, source='', wraplen=33):
		if unspecified(self.value) or (not print_default and (self.value == default)):
			return ''
		if print_section:
			prefix = '[%s] %s' % (self.section, self.option)
		else:
			prefix = self.option
		prefix += ' %s' % self.opttype

		line_list = lfilter(lambda x: x != '', imap(str.strip, self.value.strip().splitlines()))
		if not line_list:
			line_list = [prefix]  # just prefix - without trailing whitespace
		elif len(line_list) > 1:
			line_list = [prefix] + line_list  # prefix on first line - rest on other lines
		else:
			line_list = [prefix + ' ' + line_list[0]]  # everything on one line

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

	def process_entries(cls, entry_iter):
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
			if entry.opttype in ['+=', '^=']:  # modifier options
				modifier_list.append(entry)
			elif entry.opttype in ['*=', '!=', '?=', '=']:  # set options
				if entry.opttype == '*=':  # this option can not be changed by other config entries
					# TODO: notify that subsequent config options will be ignored
					return (entry, [entry])
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
		if modifier_list:  # apply remaining modifiers - result can be None
			entry_list_used.extend(modifier_list)
			result = cls._apply_modifiers(result, modifier_list)
		return (result, entry_list_used)
	process_entries = classmethod(process_entries)

	def simplify_entries(cls, entry_iter):
		# called to simplify entries for a specific option *and* section - sorted by order
		(result_base, entry_list_used) = cls._process_and_mark_entries(entry_iter)

		# Merge subsequent += and ^= entries
		def merge_subsequent_entries(entry_iter):
			prev_entry = None
			for entry in entry_iter:
				if prev_entry and (entry.opttype == prev_entry.opttype):
					if entry.opttype == '+=':
						entry.value = prev_entry.value + '\n' + entry.value
						entry.source = '<processed>'
					elif entry.opttype == '^=':
						entry.value = entry.value + '\n' + prev_entry.value
						entry.source = '<processed>'
					else:
						yield prev_entry
				prev_entry = entry
			if prev_entry:
				yield prev_entry

		if entry_list_used[0].opttype in ['*=', '!=', '?=', '=']:
			result = [cls.combine_entries(entry_list_used)]
		else:
			result = list(merge_subsequent_entries(entry_list_used))

		(result_simplified, used_simplified) = cls._process_and_mark_entries(result)
		assert len(used_simplified) == len(result)
		if result_simplified and result_base:
			assert result_simplified.value.strip() == result_base.value.strip()
		else:
			assert result_simplified == result_base
		return result
	simplify_entries = classmethod(simplify_entries)

	def _process_and_mark_entries(cls, entry_iter):
		entry_list = list(entry_iter)
		for entry in entry_list:
			entry.accessed = True
		return cls.process_entries(entry_list)
	_process_and_mark_entries = classmethod(_process_and_mark_entries)

	def combine_entries(cls, entry_iter):
		(result, entry_list_used) = cls._process_and_mark_entries(entry_iter)
		for entry in entry_list_used:
			entry.used = True
		return result
	combine_entries = classmethod(combine_entries)


class ConfigContainer(object):
	def __init__(self, name):
		self.enabled = True
		self._read_only = False
		self._counter = 0
		self._content = {}
		self._content_default = {}

	def resolve(self):
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

	def set_read_only(self):
		self._read_only = True

	def getDefault(self, entry):
		return self._content_default.get(entry.option, {}).get(entry.section)

	def setDefault(self, entry):
		curEntry = self.getDefault(entry)
		if self._read_only and not curEntry:
			raise APIError('Config container is read-only!')
		elif curEntry and (curEntry.value != entry.value):
			raise APIError('Inconsistent default values! (%r != %r)' % (curEntry.value, entry.value))
		entry.order = 0
		self._content_default.setdefault(entry.option, {}).setdefault(entry.section, entry)

	def append(self, entry):
		if self._read_only:
			raise APIError('Config container is read-only!')
		self._counter += 1
		entry.order = self._counter
		self._content.setdefault(entry.option, []).append(entry)

	def getEntry(self, option, filterExpr):
		return ConfigEntry.combine_entries(self.iter_config_entries(option, filterExpr))

	def iter_config_entries(self, option, filterExpr):
		source_list = [self._content.get(option, []), self._content_default.get(option, {}).values()]
		return ifilter(filterExpr, ichain(source_list))

	def get_options(self):
		return sorted(set(ichain([self._content.keys(), self._content_default.keys()])))
