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
from grid_control.parameters.psource_base import ImmutableParameterSource, ParameterInfo, ParameterMetadata, ParameterSource
from grid_control.utils.parsing import parseTime, parseType
from python_compat import imap, lmap

class InternalParameterSource(ImmutableParameterSource):
	def __init__(self, values, keys):
		ImmutableParameterSource.__init__(self, (values, keys))
		(self._values, self._keys) = (values, keys)

	def getMaxParameters(self):
		return len(self._values)

	def fillParameterInfo(self, pNum, result):
		result.update(self._values[pNum])

	def fillParameterKeys(self, result):
		result.extend(imap(ParameterMetadata, self._keys))


class RequirementParameterSource(ParameterSource):
	alias = ['req']

	def fillParameterKeys(self, result):
		for key in ['WALLTIME', 'CPUTIME', 'MEMORY']:
			if key in result:
				result.remove(key)

	def __repr__(self):
		return 'req()'

	def getHash(self):
		return ''

	def fillParameterInfo(self, pNum, result):
		if 'WALLTIME' in result:
			result[ParameterInfo.REQS].append((WMS.WALLTIME, parseTime(result.pop('WALLTIME'))))
		if 'CPUTIME' in result:
			result[ParameterInfo.REQS].append((WMS.CPUTIME, parseTime(result.pop('CPUTIME'))))
		if 'MEMORY' in result:
			result[ParameterInfo.REQS].append((WMS.MEMORY, int(result.pop('MEMORY'))))


class SingleParameterSource(ImmutableParameterSource):
	def __init__(self, key, hash_src_list):
		ImmutableParameterSource.__init__(self, hash_src_list)
		self._key = key.lstrip('!')
		self._meta = ParameterMetadata(self._key, untracked = '!' in key)

	def fillParameterKeys(self, result):
		result.append(self._meta)


class KeyParameterSource(ParameterSource):
	alias = ['key']

	def __init__(self, *keys):
		ParameterSource.__init__(self)
		self._keys = lmap(lambda key: key.lstrip('!'), keys)
		self._meta = lmap(lambda key: ParameterMetadata(key.lstrip('!'), untracked = '!' in key), keys)

	def fillParameterKeys(self, result):
		result.extend(self._meta)

	def __repr__(self):
		return 'key(%s)' % str.join(', ', self._keys)


class SimpleParameterSource(SingleParameterSource):
	alias = ['var']

	def __init__(self, key, values):
		SingleParameterSource.__init__(self, key, [key, values])
		if values is None:
			raise ConfigError('Missing values for %s' % key)
		self._values = values

	def show(self):
		return ['%s: var = %s, len = %d' % (self.__class__.__name__, self._key, len(self._values))]

	def getMaxParameters(self):
		return len(self._values)

	def fillParameterInfo(self, pNum, result):
		result[self._key] = self._values[pNum]

	def __repr__(self):
		return 'var(%s)' % repr(self._meta)

	def create(cls, pconfig, repository, key): # pylint:disable=arguments-differ
		return SimpleParameterSource(key, pconfig.getParameter(key.lstrip('!')))
	create = classmethod(create)


class ConstParameterSource(SingleParameterSource):
	alias = ['const']

	def __init__(self, key, value):
		SingleParameterSource.__init__(self, key, [key, value])
		self._value = value

	def show(self):
		return ['%s: const = %s, value = %s' % (self.__class__.__name__, self._key, self._value)]

	def fillParameterInfo(self, pNum, result):
		result[self._key] = self._value

	def __repr__(self):
		return 'const(%s, %s)' % (repr(self._key), repr(self._value))

	def create(cls, pconfig, repository, key, value = None): # pylint:disable=arguments-differ
		if value is None:
			value = pconfig.get(key)
		return ConstParameterSource(key, value)
	create = classmethod(create)


class RNGParameterSource(SingleParameterSource):
	alias = ['rng']

	def __init__(self, key = 'JOB_RANDOM', low = 1e6, high = 1e7-1):
		SingleParameterSource.__init__(self, '!%s' % key, [key, low, high])
		(self._low, self._high) = (int(low), int(high))

	def show(self):
		return ['%s: var = %s, range = (%s, %s)' % (self.__class__.__name__, self._key, self._low, self._high)]

	def fillParameterInfo(self, pNum, result):
		result[self._key] = random.randint(self._low, self._high)

	def __repr__(self):
		return 'rng(%s)' % repr(self._meta).replace('!', '')


class CounterParameterSource(SingleParameterSource):
	alias = ['counter']

	def __init__(self, key, seed):
		SingleParameterSource.__init__(self, '!%s' % key, [key, seed])
		self._seed = seed

	def show(self):
		return ['%s: var = %s, start = %s' % (self.__class__.__name__, self._key, self._seed)]

	def fillParameterInfo(self, pNum, result):
		result[self._key] = self._seed + result['GC_JOB_ID']

	def __repr__(self):
		return 'counter(%r, %s)' % (self._meta, self._seed)


class FormatterParameterSource(SingleParameterSource):
	alias = ['format']

	def __init__(self, key, fmt, source, default = ''):
		SingleParameterSource.__init__(self, '!%s' % key, [key, fmt, source, default])
		(self._fmt, self._source, self._default) = (fmt, source, default)

	def show(self):
		return ['%s: var = %s, fmt = %r, source = %s, default = %r' %
			(self.__class__.__name__, self._key, self._fmt, self._source, self._default)]

	def fillParameterInfo(self, pNum, result):
		src = parseType(str(result.get(self._source, self._default)))
		result[self._key] = self._fmt % src

	def __repr__(self):
		return 'format(%r, %r, %r, %r)' % (self._key, self._fmt, self._source, self._default)


class TransformParameterSource(SingleParameterSource):
	alias = ['transform']

	def __init__(self, key, fmt, default = ''):
		SingleParameterSource.__init__(self, '!%s' % key, [key, fmt, default])
		(self._fmt, self._default) = (fmt, default)

	def show(self):
		return ['%s: var = %s, expr = %r, default = %r' %
			(self.__class__.__name__, self._key, self._fmt, self._default)]

	def fillParameterInfo(self, pNum, result):
		tmp = dict(imap(lambda k_v: (str(k_v[0]), parseType(str(k_v[1]))), result.items()))
		try:
			result[self._key] = eval(self._fmt, tmp) # pylint:disable=eval-used
		except Exception:
			result[self._key] = self._default

	def __repr__(self):
		return 'transform(%r, %r, %r)' % (self._key, self._fmt, self._default)


class CollectParameterSource(SingleParameterSource): # Merge parameter values
	alias = ['collect']

	def __init__(self, key, *sources):
		SingleParameterSource.__init__(self, key, [key, sources])
		self._sources_plain = sources
		self._sources = lmap(lambda regex: re.compile('^%s$' % regex.replace('...', '.*')), list(sources))

	def fillParameterInfo(self, pNum, result):
		for src in self._sources:
			for key in result:
				if src.search(str(key)):
					result[self._key] = result[key]
					return
