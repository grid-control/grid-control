# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

import time, logging
from grid_control.utils.data_structures import makeEnum
from hpfwk import AbstractError, NestedException, Plugin
from python_compat import md5_hex, set

ParameterInfo = makeEnum(['ACTIVE', 'HASH', 'REQS', 'FILES'])


class ParameterError(NestedException):
	pass


class ParameterMetadata(str):
	def __new__(cls, value, untracked = False):
		obj = str.__new__(cls, value)
		obj.untracked = untracked
		return obj

	def __repr__(self):
		if self.untracked:
			return "'!%s'" % self
		return "'%s'" % self


class ParameterSource(Plugin):
	def EmptyResyncResult(cls):
		return (set(), set(), False)
	EmptyResyncResult = classmethod(EmptyResyncResult)

	def create(cls, pconfig, repository, *args, **kwargs):
		return cls(*args, **kwargs)
	create = classmethod(create)

	def __init__(self):
		self._log = logging.getLogger('parameters.source')
		Plugin.__init__(self)

	def canFinish(self):
		return True

	def depends(self):
		return []

	def getMaxParameters(self):
		return None

	def fillParameterKeys(self, result):
		raise AbstractError

	def fillParameterInfo(self, pNum, result):
		raise AbstractError

	def resync(self): # only needed if the parameters are opaque references (like partition idx)
		return ParameterSource.EmptyResyncResult()

	def show(self):
		return [self.__class__.__name__ + ':']

	def getUsedSources(self):
		return [self]

	def getHash(self):
		raise AbstractError


class ImmutableParameterSource(ParameterSource):
	def __init__(self, hash_src_list):
		ParameterSource.__init__(self)
		self._hash = md5_hex(repr(hash_src_list))

	def getHash(self):
		return self._hash


class LimitedResyncParameterSource(ParameterSource):
	def __init__(self):
		ParameterSource.__init__(self)
		(self._resyncInterval, self._resyncForce, self._resyncLast) = (0, False, time.time())

	def resyncSetup(self, interval = None, force = None):
		if interval is not None:
			self._resyncInterval = interval
		if force is not None:
			self._resyncForce = force

	def _resync_enabled(self):
		return self._resyncForce or (self._resyncInterval >= 0 and (abs(time.time() - self._resyncLast) >= self._resyncInterval))

	def getHash(self):
		if self._resync_enabled():
			return md5_hex(repr(time.time()))
		return self._hash

	def resync(self): # only needed if the parameters are opaque references (like partition index)
		result = None
		if self._resync_enabled():
			result = self._resync()
			self._resyncForce = False
			self._resyncLast = time.time()
		return result or ParameterSource.EmptyResyncResult()

	def _resync(self):
		pass


class NullParameterSource(ParameterSource):
	alias = ['null']

	def create(cls, pconfig, repository):
		return cls()
	create = classmethod(create)

	def fillParameterKeys(self, result):
		pass

	def fillParameterInfo(self, pNum, result):
		pass

	def getHash(self):
		return ''

	def __repr__(self):
		return 'null()'
