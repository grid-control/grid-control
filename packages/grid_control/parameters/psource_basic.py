# | Copyright 2012-2017 Karlsruhe Institute of Technology
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

import re, random
from grid_control.backends import WMS
from grid_control.config import ConfigError
from grid_control.parameters.psource_base import ParameterInfo, ParameterMetadata, ParameterSource
from grid_control.utils.parsing import parse_time, parse_type, str_dict_linear
from hpfwk import ignore_exception
from python_compat import imap, lmap, md5_hex


class ImmutableParameterSource(ParameterSource):
	def __init__(self, hash_src_list):
		ParameterSource.__init__(self)
		self._hash = md5_hex(repr(hash_src_list))

	def get_psrc_hash(self):
		return self._hash


class KeyParameterSource(ParameterSource):
	alias_list = ['key']

	def __init__(self, *keys):
		ParameterSource.__init__(self)
		self._keys = lmap(lambda key: key.lstrip('!'), keys)
		self._meta = lmap(lambda key: ParameterMetadata(key.lstrip('!'), untracked='!' in key), keys)

	def __repr__(self):
		return 'key(%s)' % str.join(', ', self._keys)

	def fill_parameter_metadata(self, result):
		result.extend(self._meta)


class RequirementParameterSource(ParameterSource):
	alias_list = ['req']

	def __repr__(self):
		return 'req()'

	def fill_parameter_content(self, pnum, result):
		if 'WALLTIME' in result:
			result[ParameterInfo.REQS].append((WMS.WALLTIME, parse_time(result.pop('WALLTIME'))))
		if 'CPUTIME' in result:
			result[ParameterInfo.REQS].append((WMS.CPUTIME, parse_time(result.pop('CPUTIME'))))
		for (req_key, vn) in [(WMS.MEMORY, 'MEMORY'), (WMS.CPUS, 'CPUS'), (WMS.DISKSPACE, 'DISKSPACE')]:
			if vn in result:
				result[ParameterInfo.REQS].append((req_key, int(result.pop(vn))))

	def fill_parameter_metadata(self, result):
		for output_vn in result:
			if output_vn.value in ['WALLTIME', 'CPUTIME', 'MEMORY', 'CPUS', 'DISKSPACE']:
				result.remove(output_vn)

	def get_psrc_hash(self):
		return ''


class SingleParameterSource(ImmutableParameterSource):
	def __init__(self, output_vn, hash_src_list):
		ImmutableParameterSource.__init__(self, hash_src_list)
		self._output_vn = output_vn.lstrip('!')
		self._meta = ParameterMetadata(self._output_vn, untracked='!' in output_vn)

	def fill_parameter_metadata(self, result):
		result.append(self._meta)


class CollectParameterSource(SingleParameterSource):  # Merge parameter values
	alias_list = ['collect']

	def __init__(self, output_vn, *vn_list):
		SingleParameterSource.__init__(self, output_vn, [output_vn, vn_list])
		self._vn_list_plain = vn_list
		self._vn_list = lmap(lambda regex: re.compile('^%s$' % regex.replace('...', '.*')), list(vn_list))

	def fill_parameter_content(self, pnum, result):
		for src in self._vn_list:
			for output_vn in result:
				if src.search(str(output_vn)):
					result[self._output_vn] = result[output_vn]
					return


class ConstParameterSource(SingleParameterSource):
	alias_list = ['const']

	def __init__(self, output_vn, value):
		SingleParameterSource.__init__(self, output_vn, [output_vn, value])
		self._value = value

	def __repr__(self):
		return 'const(%r, %s)' % (self._meta.get_value(), repr(self._value))

	def create_psrc(cls, pconfig, repository, output_vn, value=None):  # pylint:disable=arguments-differ
		if value is None:
			value = pconfig.get(output_vn)
		return ConstParameterSource(output_vn, value)
	create_psrc = classmethod(create_psrc)

	def fill_parameter_content(self, pnum, result):
		result[self._output_vn] = self._value

	def show_psrc(self):
		return ['%s: const = %s, value = %s' % (self.__class__.__name__, self._output_vn, self._value)]


class CounterParameterSource(SingleParameterSource):
	alias_list = ['counter']

	def __init__(self, output_vn, seed):
		SingleParameterSource.__init__(self, '!%s' % output_vn.lstrip(), [output_vn, seed])
		self._seed = seed

	def __repr__(self):
		return 'counter(%r, %s)' % (self._meta.get_value(), self._seed)

	def fill_parameter_content(self, pnum, result):
		result[self._output_vn] = self._seed + result['GC_JOB_ID']

	def show_psrc(self):
		return ['%s: var = %s, start = %s' % (self.__class__.__name__, self._output_vn, self._seed)]


class FormatterParameterSource(SingleParameterSource):
	alias_list = ['format']

	def __init__(self, output_vn, fmt, source, default=''):
		SingleParameterSource.__init__(self, '!%s' % output_vn, [output_vn, fmt, source, default])
		(self._fmt, self._source, self._default) = (fmt, source, default)

	def __repr__(self):
		return 'format(%r, %r, %r, %r)' % (self._output_vn, self._fmt, self._source, self._default)

	def fill_parameter_content(self, pnum, result):
		src = parse_type(str(result.get(self._source, self._default)))
		result[self._output_vn] = self._fmt % src

	def show_psrc(self):
		return ['%s: var = %s, fmt = %r, source = %s, default = %r' %
			(self.__class__.__name__, self._output_vn, self._fmt, self._source, self._default)]


class RNGParameterSource(SingleParameterSource):
	alias_list = ['rng']

	def __init__(self, output_vn='JOB_RANDOM', low=1e6, high=1e7 - 1):
		SingleParameterSource.__init__(self, '!%s' % output_vn.lstrip('!'), [output_vn, low, high])
		(self._low, self._high) = (int(low), int(high))

	def __repr__(self):
		return 'rng(%r)' % self._meta.get_value()

	def fill_parameter_content(self, pnum, result):
		result[self._output_vn] = random.randint(self._low, self._high)

	def show_psrc(self):
		return ['%s: var = %s, range = (%s, %s)' % (self.__class__.__name__,
			self._output_vn, self._low, self._high)]


class RegexTransformParameterSource(SingleParameterSource):
	alias_list = ['regex_transform']

	def __init__(self, output_vn, source_vn, regex_dict, regex_order, default=None):
		SingleParameterSource.__init__(self, '!%s' % output_vn,
			[output_vn, source_vn, regex_order, str_dict_linear(regex_dict)])
		(self._source_vn, self._default) = (source_vn, default)
		(self._regex_order, self._regex_dict) = (regex_order, regex_dict)
		self._regex_comp = {}  # precompile regex
		for regex_pattern in self._regex_order:
			self._regex_comp[regex_pattern] = re.compile(regex_pattern)

	def __repr__(self):
		return 'regex_transform(%r, %r, %r)' % (self._output_vn, self._source_vn,
			str_dict_linear(self._regex_dict))

	def fill_parameter_content(self, pnum, result):
		for regex_pattern in self._regex_order:
			regex_obj = self._regex_comp[regex_pattern]
			source_str = result.get(self._source_vn, '')
			if regex_obj.match(source_str):
				result[self._output_vn] = regex_obj.sub(self._regex_dict[regex_pattern], source_str)
				return
		result[self._output_vn] = self._regex_dict.get(None, self._default)

	def get_parameter_deps(self):
		return [self._source_vn]

	def show_psrc(self):
		return ['%s: var = %s, source_vn = %r, regex_dict = %r' %
			(self.__class__.__name__, self._output_vn, self._source_vn, str_dict_linear(self._regex_dict))]


class SimpleParameterSource(SingleParameterSource):
	alias_list = ['var']

	def __init__(self, output_vn, value_list):
		SingleParameterSource.__init__(self, output_vn, [output_vn, value_list])
		if value_list is None:
			raise ConfigError('Missing values for %s' % output_vn)
		self._value_list = value_list

	def __repr__(self):
		return 'var(%r)' % self._meta.get_value()

	def create_psrc(cls, pconfig, repository, output_vn):  # pylint:disable=arguments-differ
		return SimpleParameterSource(output_vn, pconfig.get_parameter(output_vn.lstrip('!')))
	create_psrc = classmethod(create_psrc)

	def fill_parameter_content(self, pnum, result):
		result[self._output_vn] = self._value_list[pnum]

	def get_parameter_len(self):
		return len(self._value_list)

	def show_psrc(self):
		return ['%s: var = %s, len = %d' % (self.__class__.__name__,
			self._output_vn, len(self._value_list))]


class TransformParameterSource(SingleParameterSource):
	alias_list = ['transform']

	def __init__(self, output_vn, fmt, default=''):
		SingleParameterSource.__init__(self, '!%s' % output_vn, [output_vn, fmt, default])
		(self._fmt, self._default) = (fmt, default)

	def __repr__(self):
		return 'transform(%r, %r, %r)' % (self._output_vn, self._fmt, self._default)

	def fill_parameter_content(self, pnum, result):
		tmp = dict(imap(lambda k_v: (str(k_v[0]), parse_type(str(k_v[1]))), result.items()))
		result[self._output_vn] = ignore_exception(Exception, self._default, eval, self._fmt, tmp)

	def show_psrc(self):
		return ['%s: var = %s, expr = %r, default = %r' %
			(self.__class__.__name__, self._output_vn, self._fmt, self._default)]
