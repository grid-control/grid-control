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

import time
from grid_control import utils
from hpfwk import AbstractError, NestedException, Plugin
from python_compat import set

class ParameterError(NestedException):
	pass


class ParameterInfo:
	reqTypes = ('ACTIVE', 'HASH', 'REQS')
	for idx, reqType in enumerate(reqTypes):
		locals()[reqType] = idx


class ParameterMetadata(str):
	def __new__(cls, value, untracked = False):
		obj = str.__new__(cls, value)
		obj.untracked = untracked
		return obj

	def __repr__(self):
		return "'%s'" % utils.QM(self.untracked, '!%s' % self, self)


class ParameterSource(Plugin):
	def create(cls, pconfig, *args, **kwargs):
		return cls(*args, **kwargs)
	create = classmethod(create)

	def __init__(self):
		Plugin.__init__(self)
		self._resyncInfo = None
		self._resyncTime = -1 # Default - always resync
		self._resyncLast = None

	def depends(self):
		return []

	def getMaxParameters(self):
		return None

	def fillParameterKeys(self, result):
		raise AbstractError

	def fillParameterInfo(self, pNum, result):
		raise AbstractError

	def resyncSetup(self, interval = None, force = None, info = None):
		self._resyncInfo = info # User override for base resync infos
		if interval is not None:
			self._resyncTime = interval # -1 == always, 0 == disabled, >0 == time in sec between resyncs
			self._resyncLast = time.time()
		if force:
			self._resyncLast = None # Force resync on next attempt

	def resyncEnabled(self):
		if (self._resyncLast is None) or (self._resyncTime == -1):
			return True
		if self._resyncTime > 0:
			if time.time() - self._resyncLast > self._resyncTime:
				return True
		return False

	def resyncFinished(self):
		self._resyncLast = time.time()

	def resync(self): # needed when parameter values do not change but if meaning / validity of values do
		if self.resyncEnabled() and self._resyncInfo:
			return self._resyncInfo
		return (set(), set(), False) # returns two sets of parameter ids and boolean (redo, disable, sizeChange)

	def show(self):
		return [self.__class__.__name__ + ':']

	def getHash(self):
		raise AbstractError

ParameterSource.managerMap = {}
