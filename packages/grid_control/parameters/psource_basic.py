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
from grid_control import utils
from grid_control.backends import WMS
from grid_control.config import ConfigError
from grid_control.parameters.psource_base import ParameterInfo, ParameterMetadata, ParameterSource
from grid_control.utils.parsing import parseTime
from hpfwk import APIError
from python_compat import imap, lmap, md5_hex

class InternalParameterSource(ParameterSource):
	def __init__(self, values, keys):
		ParameterSource.__init__(self)
		(self._values, self._keys) = (values, keys)

	def getMaxParameters(self):
		return len(self._values)

	def fillParameterInfo(self, pNum, result):
		result.update(self._values[pNum])

	def fillParameterKeys(self, result):
		result.extend(imap(ParameterMetadata, self._keys))

	def getHash(self):
		return md5_hex(str(self._values) + str(self._keys))


class RequirementParameterSource(ParameterSource):
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


class SingleParameterSource(ParameterSource):
	def __init__(self, key):
		ParameterSource.__init__(self)
		self._key = key.lstrip('!')
		self._meta = ParameterMetadata(self._key, untracked = '!' in key)

	def fillParameterKeys(self, result):
		result.append(self._meta)


class KeyParameterSource(ParameterSource):
	def __init__(self, *keys):
		ParameterSource.__init__(self)
		self._keys = lmap(lambda key: key.lstrip('!'), keys)
		self._meta = lmap(lambda key: ParameterMetadata(key.lstrip('!'), untracked = '!' in key), keys)

	def fillParameterKeys(self, result):
		result.extend(self._meta)

	def __repr__(self):
		return 'key(%s)' % str.join(', ', self._keys)
ParameterSource.managerMap['key'] = 'KeyParameterSource'


class SimpleParameterSource(SingleParameterSource):
	def __init__(self, key, values):
		SingleParameterSource.__init__(self, key)
		if values is None:
			raise ConfigError('Missing values for %s' % key)
		self._values = values

	def show(self):
		return ['%s: var = %s, len = %d' % (self.__class__.__name__, self._key, len(self._values))]

	def getMaxParameters(self):
		return len(self._values)

	def fillParameterInfo(self, pNum, result):
		result[self._key] = self._values[pNum]

	def getHash(self):
		return md5_hex(str(self._key) + str(self._values))

	def __repr__(self):
		return 'var(%s)' % repr(self._meta)

	def create(cls, pconfig, key): # pylint:disable=arguments-differ
		return SimpleParameterSource(key, pconfig.getParameter(key.lstrip('!')))
	create = classmethod(create)
ParameterSource.managerMap['var'] = 'SimpleParameterSource'


class SimpleFileParameterSource(SimpleParameterSource):
	def getHash(self):
		raise APIError('Not yet implemented') # return hash of file content

	def __repr__(self):
		return 'files(%s)' % repr(self._meta)
ParameterSource.managerMap['files'] = 'SimpleFileParameterSource'


class ConstParameterSource(SingleParameterSource):
	def __init__(self, key, value):
		SingleParameterSource.__init__(self, key)
		self._value = value

	def show(self):
		return ['%s: const = %s, value = %s' % (self.__class__.__name__, self._key, self._value)]

	def getHash(self):
		return md5_hex(str(self._key) + str(self._value))

	def fillParameterInfo(self, pNum, result):
		result[self._key] = self._value

	def __repr__(self):
		return 'const(%s, %s)' % (repr(self._key), repr(self._value))

	def create(cls, pconfig, key, value = None): # pylint:disable=arguments-differ
		if value is None:
			value = pconfig.get(key)
		return ConstParameterSource(key, value)
	create = classmethod(create)
ParameterSource.managerMap['const'] = 'ConstParameterSource'


class RNGParameterSource(SingleParameterSource):
	def __init__(self, key = 'JOB_RANDOM', low = 1e6, high = 1e7-1):
		SingleParameterSource.__init__(self, '!%s' % key)
		(self.low, self.high) = (int(low), int(high))

	def show(self):
		return ['%s: var = %s, range = (%s, %s)' % (self.__class__.__name__, self._key, self.low, self.high)]

	def fillParameterInfo(self, pNum, result):
		result[self._key] = random.randint(self.low, self.high)

	def getHash(self):
		return md5_hex(str(self._key) + str([self.low, self.high]))

	def __repr__(self):
		return 'rng(%s)' % repr(self._meta).replace('!', '')
ParameterSource.managerMap['rng'] = 'RNGParameterSource'


class CounterParameterSource(SingleParameterSource):
	def __init__(self, key, seed):
		SingleParameterSource.__init__(self, '!%s' % key)
		self._seed = seed

	def show(self):
		return ['%s: var = %s, start = %s' % (self.__class__.__name__, self._key, self._seed)]

	def getHash(self):
		return md5_hex(str(self._key) + str(self._seed))

	def fillParameterInfo(self, pNum, result):
		result[self._key] = self._seed + result['GC_JOB_ID']

	def __repr__(self):
		return 'counter(%r, %s)' % (self._meta, self._seed)
ParameterSource.managerMap['counter'] = 'CounterParameterSource'


class FormatterParameterSource(SingleParameterSource):
	def __init__(self, key, fmt, source, default = ''):
		SingleParameterSource.__init__(self, '!%s' % key)
		(self._fmt, self._source, self._default) = (fmt, source, default)

	def getHash(self):
		return md5_hex(str(self._key) + str([self._fmt, self._source, self._default]))

	def show(self):
		return ['%s: var = %s, fmt = %s, source = %s, default = %s' %
			(self.__class__.__name__, self._key, self._fmt, self._source, self._default)]

	def fillParameterInfo(self, pNum, result):
		src = utils.parseType(str(result.get(self._source, self._default)))
		result[self._key] = self._fmt % src

	def __repr__(self):
		return 'format(%r, %r, %r, %r)' % (self._key, self._fmt, self._source, self._default)
ParameterSource.managerMap['format'] = 'FormatterParameterSource'


class CollectParameterSource(SingleParameterSource): # Merge parameter values
	def __init__(self, target, *sources):
		SingleParameterSource.__init__(self, target)
		self._sources_plain = sources
		self._sources = lmap(lambda regex: re.compile('^%s$' % regex.replace('...', '.*')), list(sources))

	def getHash(self):
		return md5_hex(str(self._key) + str(self._sources_plain))

	def fillParameterInfo(self, pNum, result):
		for src in self._sources:
			for key in result:
				if src.search(str(key)):
					result[self._key] = result[key]
					return
ParameterSource.managerMap['collect'] = 'CollectParameterSource'
