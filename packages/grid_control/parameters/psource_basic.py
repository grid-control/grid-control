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

import re, random
from grid_control.backends import WMS
from grid_control.config import ConfigError
from grid_control.parameters.psource_base import ParameterInfo, ParameterMetadata, ParameterSource
from grid_control.utils.parsing import parse_time, parse_type, str_dict
from python_compat import imap, lmap, md5_hex


class ImmutableParameterSource(ParameterSource):
	def __init__(self, hash_src_list):
		ParameterSource.__init__(self)
		self._hash = md5_hex(repr(hash_src_list))

	def get_psrc_hash(self):
		return self._hash


class InternalParameterSource(ImmutableParameterSource):
	def __init__(self, value_list, meta_list):
		(self._value_list, self._meta_list) = (value_list, meta_list)
		self._keys = lmap(lambda pm: pm.get_value(), meta_list)
		ImmutableParameterSource.__init__(self, (lmap(str_dict, value_list), self._keys))

	def __repr__(self):
		return '<internal:%s=%s>' % (str.join('|', self._keys), self.get_psrc_hash())

	def fill_parameter_content(self, pnum, result):
		result.update(self._value_list[pnum])

	def fill_parameter_metadata(self, result):
		result.extend(self._meta_list)

	def get_parameter_len(self):
		return len(self._value_list)


class SingleParameterSource(ImmutableParameterSource):
	def __init__(self, key, hash_src_list):
		ImmutableParameterSource.__init__(self, hash_src_list)
		self._key = key.lstrip('!')
		self._meta = ParameterMetadata(self._key, untracked='!' in key)

	def fill_parameter_metadata(self, result):
		result.append(self._meta)


class CollectParameterSource(SingleParameterSource):  # Merge parameter values
	alias_list = ['collect']

	def __init__(self, key, *vn_list):
		SingleParameterSource.__init__(self, key, [key, vn_list])
		self._vn_list_plain = vn_list
		self._vn_list = lmap(lambda regex: re.compile('^%s$' % regex.replace('...', '.*')), list(vn_list))

	def fill_parameter_content(self, pnum, result):
		for src in self._vn_list:
			for key in result:
				if src.search(str(key)):
					result[self._key] = result[key]
					return


class ConstParameterSource(SingleParameterSource):
	alias_list = ['const']

	def __init__(self, key, value):
		SingleParameterSource.__init__(self, key, [key, value])
		self._value = value

	def __repr__(self):
		return 'const(%r, %s)' % (self._meta.get_value(), repr(self._value))

	def create_psrc(cls, pconfig, repository, key, value=None):  # pylint:disable=arguments-differ
		if value is None:
			value = pconfig.get(key)
		return ConstParameterSource(key, value)
	create_psrc = classmethod(create_psrc)

	def fill_parameter_content(self, pnum, result):
		result[self._key] = self._value

	def show_psrc(self):
		return ['%s: const = %s, value = %s' % (self.__class__.__name__, self._key, self._value)]


class CounterParameterSource(SingleParameterSource):
	alias_list = ['counter']

	def __init__(self, key, seed):
		SingleParameterSource.__init__(self, '!%s' % key.lstrip(), [key, seed])
		self._seed = seed

	def __repr__(self):
		return 'counter(%r, %s)' % (self._meta.get_value(), self._seed)

	def fill_parameter_content(self, pnum, result):
		result[self._key] = self._seed + result['GC_JOB_ID']

	def show_psrc(self):
		return ['%s: var = %s, start = %s' % (self.__class__.__name__, self._key, self._seed)]


class FormatterParameterSource(SingleParameterSource):
	alias_list = ['format']

	def __init__(self, key, fmt, source, default=''):
		SingleParameterSource.__init__(self, '!%s' % key, [key, fmt, source, default])
		(self._fmt, self._source, self._default) = (fmt, source, default)

	def __repr__(self):
		return 'format(%r, %r, %r, %r)' % (self._key, self._fmt, self._source, self._default)

	def fill_parameter_content(self, pnum, result):
		src = parse_type(str(result.get(self._source, self._default)))
		result[self._key] = self._fmt % src

	def show_psrc(self):
		return ['%s: var = %s, fmt = %r, source = %s, default = %r' %
			(self.__class__.__name__, self._key, self._fmt, self._source, self._default)]


class RNGParameterSource(SingleParameterSource):
	alias_list = ['rng']

	def __init__(self, key='JOB_RANDOM', low=1e6, high=1e7 - 1):
		SingleParameterSource.__init__(self, '!%s' % key.lstrip('!'), [key, low, high])
		(self._low, self._high) = (int(low), int(high))

	def __repr__(self):
		return 'rng(%r)' % self._meta.get_value()

	def fill_parameter_content(self, pnum, result):
		result[self._key] = random.randint(self._low, self._high)

	def show_psrc(self):
		return ['%s: var = %s, range = (%s, %s)' % (self.__class__.__name__,
			self._key, self._low, self._high)]


class SimpleParameterSource(SingleParameterSource):
	alias_list = ['var']

	def __init__(self, key, value_list):
		SingleParameterSource.__init__(self, key, [key, value_list])
		if value_list is None:
			raise ConfigError('Missing values for %s' % key)
		self._value_list = value_list

	def __repr__(self):
		return 'var(%r)' % self._meta.get_value()

	def create_psrc(cls, pconfig, repository, key):  # pylint:disable=arguments-differ
		return SimpleParameterSource(key, pconfig.get_parameter(key.lstrip('!')))
	create_psrc = classmethod(create_psrc)

	def fill_parameter_content(self, pnum, result):
		result[self._key] = self._value_list[pnum]

	def get_parameter_len(self):
		return len(self._value_list)

	def show_psrc(self):
		return ['%s: var = %s, len = %d' % (self.__class__.__name__, self._key, len(self._value_list))]


class TransformParameterSource(SingleParameterSource):
	alias_list = ['transform']

	def __init__(self, key, fmt, default=''):
		SingleParameterSource.__init__(self, '!%s' % key, [key, fmt, default])
		(self._fmt, self._default) = (fmt, default)

	def __repr__(self):
		return 'transform(%r, %r, %r)' % (self._key, self._fmt, self._default)

	def fill_parameter_content(self, pnum, result):
		tmp = dict(imap(lambda k_v: (str(k_v[0]), parse_type(str(k_v[1]))), result.items()))
		try:
			result[self._key] = eval(self._fmt, tmp)  # pylint:disable=eval-used
		except Exception:
			result[self._key] = self._default

	def show_psrc(self):
		return ['%s: var = %s, expr = %r, default = %r' %
			(self.__class__.__name__, self._key, self._fmt, self._default)]


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
		if 'MEMORY' in result:
			result[ParameterInfo.REQS].append((WMS.MEMORY, int(result.pop('MEMORY'))))

	def fill_parameter_metadata(self, result):
		for key in ['WALLTIME', 'CPUTIME', 'MEMORY']:
			if key in result:
				result.remove(key)

	def get_psrc_hash(self):
		return ''
