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


class ParameterMetadata(object):
	def __init__(self, value, untracked = False):
		(self.value, self.untracked) = (value, untracked)

	def __repr__(self):
		return self.get_value()

	def get_value(self):
		if self.untracked:
			return '!' + self.value
		return self.value


class ParameterSource(Plugin):
	def EmptyResyncResult(cls):
		return (set(), set(), False)
	EmptyResyncResult = classmethod(EmptyResyncResult)

	def create_psrc(cls, pconfig, repository, *args, **kwargs):
		return cls(*args, **kwargs)
	create_psrc = classmethod(create_psrc)

	def __init__(self):
		self._log = logging.getLogger('parameters.source')
		Plugin.__init__(self)

	def can_finish(self):
		return True

	def fill_parameter_content(self, pNum, result):
		raise AbstractError

	def fill_parameter_metadata(self, result):
		raise AbstractError

	def get_parameter_deps(self):
		return []

	def get_parameter_len(self):
		return None

	def get_psrc_hash(self):
		raise AbstractError

	def get_used_psrc_list(self):
		return [self]

	def resync_psrc(self): # only needed if the parameters are opaque references (like partition idx)
		return ParameterSource.EmptyResyncResult()

	def show_psrc(self):
		return [self.__class__.__name__ + ':']


class ImmutableParameterSource(ParameterSource):
	def __init__(self, hash_src_list):
		ParameterSource.__init__(self)
		self._hash = md5_hex(repr(hash_src_list))

	def get_psrc_hash(self):
		return self._hash


class LimitedResyncParameterSource(ParameterSource):
	def __init__(self):
		ParameterSource.__init__(self)
		(self._resyncInterval, self._resyncForce, self._resyncLast) = (0, False, time.time())

	def get_psrc_hash(self):
		if self._resync_enabled():
			return md5_hex(repr(time.time()))
		return self._hash

	def resync_psrc(self): # only needed if the parameters are opaque references (like partition index)
		result = None
		if self._resync_enabled():
			result = self._resync_psrc()
			self._resyncForce = False
			self._resyncLast = time.time()
		return result or ParameterSource.EmptyResyncResult()

	def resyncSetup(self, interval = None, force = None):
		if interval is not None:
			self._resyncInterval = interval
		if force is not None:
			self._resyncForce = force

	def _resync_enabled(self):
		return self._resyncForce or (self._resyncInterval >= 0 and (abs(time.time() - self._resyncLast) >= self._resyncInterval))

	def _resync_psrc(self):
		pass


class NullParameterSource(ParameterSource):
	alias = ['null']

	def create_psrc(cls, pconfig, repository):
		return cls()
	create_psrc = classmethod(create_psrc)

	def __repr__(self):
		return 'null()'

	def fill_parameter_content(self, pNum, result):
		pass

	def fill_parameter_metadata(self, result):
		pass

	def get_psrc_hash(self):
		return ''
