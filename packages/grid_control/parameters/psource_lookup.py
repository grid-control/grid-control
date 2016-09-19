# | Copyright 2012-2016 Karlsruhe Institute of Technology
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

from grid_control.config import ConfigError, Matcher
from grid_control.parameters.psource_base import ParameterError, ParameterInfo, ParameterSource
from grid_control.parameters.psource_basic import KeyParameterSource, SingleParameterSource
from python_compat import imap, irange, izip, lmap, md5_hex

def parse_lookup_create_args(pconfig, output_user, lookup_user_list):
	# Transform output and lookup input: eg. key('A', 'B') -> ['A', 'B']
	def keys_to_vn_list(src):
		result = []
		src.fill_parameter_metadata(result)
		return lmap(lambda meta: meta.value, result)
	if isinstance(output_user, str):
		output_vn = output_user
	else:
		output_vn = keys_to_vn_list(output_user)[0]
	if isinstance(lookup_user_list, str):
		lookup_vn_list = lookup_user_list.split()
	elif lookup_user_list is not None:
		lookup_vn_list = keys_to_vn_list(lookup_user_list)
	else: # no lookup information given - query config for default lookup variable
		lookup_vn_list = [pconfig.get('default lookup')]
	if not lookup_vn_list or lookup_vn_list == ['']:
		raise ConfigError('Lookup parameter not defined!')

	# configure lookup matcher
	name_matcher_default = pconfig.get('', 'default matcher', 'equal')
	name_matcher_raw = pconfig.get(output_vn, 'matcher', name_matcher_default)
	name_matcher_list = name_matcher_raw.lower().splitlines()
	if len(name_matcher_list) == 1: # single matcher given - extend to same length as lookup_list
		name_matcher_list = name_matcher_list * len(lookup_vn_list)
	elif len(name_matcher_list) != len(lookup_vn_list):
		raise ConfigError('Match-functions (length %d) and match-keys (length %d) do not match!' %
			(len(name_matcher_list), len(lookup_vn_list)))
	lookup_matcher_list = []
	for name_matcher in name_matcher_list:
		lookup_matcher_list.append(Matcher.create_instance(name_matcher, pconfig, output_vn))

	# configure lookup dictionary
	(lookup_dict, lookup_order) = pconfig.get_parameter(output_vn)
	if not pconfig.getBool(output_vn, 'empty set', False):
		for lookup_key in lookup_dict:
			if len(lookup_dict[lookup_key]) == 0:
				lookup_dict[lookup_key].append('')
	return (output_vn, lookup_vn_list, lookup_matcher_list, lookup_dict, lookup_order)


def parse_lookup_factory_args(pconfig, output_vn_list, lookup_vn_list):
	# Return list of (is_nested, PSourceClass, arguments) entries
	if len(output_vn_list) != 1: # multi-lookup handling
		result = []
		for output_vn in output_vn_list:
			assert(isinstance(output_vn, str))
			result.extend(parse_lookup_factory_args(pconfig, [output_vn], lookup_vn_list))
		return result
	output_vn = output_vn_list[0]
	assert(isinstance(output_vn, str))

	parameter_value = pconfig.get_parameter(output_vn.lstrip('!'))
	if isinstance(parameter_value, list): # simple parameter source
		if len(parameter_value) == 1:
			return [(False, ParameterSource.get_class('ConstParameterSource'), [output_vn, parameter_value[0]])]
		else:
			return [(False, ParameterSource.get_class('SimpleParameterSource'), [output_vn, parameter_value])]
	elif isinstance(parameter_value, tuple) and parameter_value[0] == 'format':
		return [(False, ParameterSource.get_class('FormatterParameterSource'), parameter_value[1:])]

	lookup_vn = None
	if lookup_vn_list: # default lookup key
		lookup_vn = KeyParameterSource(*lookup_vn_list)

	# Determine kind of lookup, [3] == lookup_dict
	tmp = parse_lookup_create_args(pconfig, KeyParameterSource(output_vn), lookup_vn)
	lookup_dict = tmp[3]
	lookup_len = lmap(len, lookup_dict.values())

	if (min(lookup_len) == 1) and (max(lookup_len) == 1): # simple lookup sufficient for this setup
		return [(False, SimpleLookupParameterSource, list(tmp))]
	# switch needs elevation beyond local scope
	return [(True, SwitchingLookupParameterSource, list(tmp))]


class LookupHelper(object):
	def __init__(self, lookup_vn_list, lookup_matcher_list, lookup_dict, lookup_order):
		(self._lookup_vn_list, self._lookup_matcher_list) = (lookup_vn_list, lookup_matcher_list)
		(self._lookup_dict, self._lookup_order) = (lookup_dict, lookup_order)

	def __repr__(self):
		if len(self._lookup_vn_list) == 1:
			return repr(self._lookup_vn_list[0])
		return 'key(%s)' % str.join(', ', imap(lambda x: "'%s'" % x, self._lookup_vn_list))

	def get_psrc_hash(self):
		return md5_hex(str(lmap(lambda x: self._lookup_dict, self._lookup_order)))

	def lookup(self, psp):
		lookup_value_list = lmap(psp.get, self._lookup_vn_list)
		lookup_dict_key = self._match_lookup_dict_key(lookup_value_list)
		return self._lookup_dict.get(lookup_dict_key)

	def _match_lookup_dict_key(self, lookup_value_list):
		for lookup_dict_key in self._lookup_order:
			match = True
			for (lookup_value, lookup_expr, lookup_matcher) in izip(lookup_value_list, lookup_dict_key, self._lookup_matcher_list):
				if lookup_value is not None:
					match = match and (lookup_matcher.matcher(lookup_value, lookup_expr) > 0)
			if match:
				return lookup_dict_key


class SimpleLookupParameterSource(SingleParameterSource):
	alias = ['lookup']

	def __init__(self, output_vn, lookup_vn_list, lookup_matcher_list, lookup_dict, lookup_order):
		self._lookup_vn_list = lookup_vn_list
		self._helper = LookupHelper(lookup_vn_list, lookup_matcher_list, lookup_dict, lookup_order)
		SingleParameterSource.__init__(self, output_vn, [output_vn, self._helper.get_psrc_hash()])

	def __repr__(self):
		return "lookup('%s', %s)" % (self._key, repr(self._helper))

	def create_psrc(cls, pconfig, repository, key, lookup = None): # pylint:disable=arguments-differ
		return SimpleLookupParameterSource(*parse_lookup_create_args(pconfig, key, lookup))
	create_psrc = classmethod(create_psrc)

	def fill_parameter_content(self, pnum, result):
		output_tuple = self._helper.lookup(result)
		if output_tuple is None:
			return
		elif len(output_tuple) != 1:
			raise ConfigError("%s can't handle multiple lookup parameter sets!" % self.__class__.__name__)
		elif output_tuple[0] is not None:
			result[self._key] = output_tuple[0]

	def get_parameter_deps(self):
		return self._lookup_vn_list

	def show_psrc(self):
		return ['%s: var = %s, lookup = %s' % (self.__class__.__name__, self._key, repr(self._helper))]


class SwitchingLookupParameterSource(SingleParameterSource):
	alias = ['switch']

	def __init__(self, psrc, output_vn, lookup_vn_list, lookup_matcher_list, lookup_dict, lookup_order):
		SingleParameterSource.__init__(self, output_vn, [])
		self._helper = LookupHelper(lookup_vn_list, lookup_matcher_list, lookup_dict, lookup_order)
		self._psrc = psrc
		self._psp_field = self._init_psp_field()

	def __repr__(self):
		return "switch(%r, '%s', %s)" % (self._psrc, self._key, repr(self._helper))

	def create_psrc(cls, pconfig, repository, psrc, key, lookup = None): # pylint:disable=arguments-differ
		return SwitchingLookupParameterSource(psrc, *parse_lookup_create_args(pconfig, key, lookup))
	create_psrc = classmethod(create_psrc)

	def fill_parameter_content(self, pnum, result):
		if len(self._psp_field) == 0:
			self._psrc.fill_parameter_content(pnum, result)
			return
		psrc_pnum, output_idx = self._psp_field[pnum]
		self._psrc.fill_parameter_content(psrc_pnum, result)
		result[self._key] = self._helper.lookup(result)[output_idx]

	def fill_parameter_metadata(self, result):
		result.append(self._meta)
		self._psrc.fill_parameter_metadata(result)

	def get_parameter_len(self):
		return len(self._psp_field)

	def get_psrc_hash(self):
		return md5_hex(self._key + self._helper.get_psrc_hash() + self._psrc.get_psrc_hash())

	def get_used_psrc_list(self):
		return [self] + self._psrc.get_used_psrc_list()

	def resync_psrc(self):
		(result_redo, result_disable, _) = ParameterSource.EmptyResyncResult()
		(psrc_redo, psrc_disable, psrc_size_change) = self._psrc.resync_psrc()
		self._psp_field = self._init_psp_field()
		for pnum, psp_info in enumerate(self._psp_field):
			psrc_pnum, _ = psp_info # ignore output_idx
			if psrc_pnum in psrc_redo:
				result_redo.add(pnum)
			if psrc_pnum in psrc_disable:
				result_disable.add(pnum)
		return (result_redo, result_disable, psrc_size_change)

	def show_psrc(self):
		result = ['%s: var = %s, lookup = %s' % (self.__class__.__name__, self._key, repr(self._helper))]
		return result + lmap(lambda x: '\t' + x, self._psrc.show_psrc())

	def _init_psp_field(self):
		result = []
		def add_psp_entry(pnum):
			tmp = {ParameterInfo.ACTIVE: True, ParameterInfo.REQS: [], 'GC_JOB_ID': pnum, 'GC_PARAM': pnum}
			self._psrc.fill_parameter_content(pnum, tmp)
			output_tuple = self._helper.lookup(tmp)
			if output_tuple:
				for (lookup_idx, tmp) in enumerate(output_tuple):
					result.append((pnum, lookup_idx))

		if self._psrc.get_parameter_len() is None:
			raise ParameterError('Unable to use %r with an infinite parameter space!' % self.__class__.__name__)
		else:
			for pnum in irange(self._psrc.get_parameter_len()):
				add_psp_entry(pnum)
		if len(result) == 0:
			self._log.critical('Lookup parameter "%s" has no matching entries!', self._key)
		return result
