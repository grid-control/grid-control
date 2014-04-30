#-#  Copyright 2012-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, re, shlex
from psource_base import ParameterSource
from psource_meta import ZipLongParameterSource
from psource_file import *
from pfactory_base import BasicParameterFactory
from grid_control import LoadableObject, QM, utils, RethrowError

# Parameter factory which evaluates a parameter module string
class ModularParameterFactory(BasicParameterFactory):
	def __init__(self, config, name):
		BasicParameterFactory.__init__(self, config, name)
		self.pExpr = self.paramConfig.get('parameters', '')


	def _getUserSource(self, pExpr, parent):
		if not pExpr:
			return parent
		# Wrap plugin factory functions
		def createWrapper(cls):
			def wrapper(*args):
				try:
					return cls.create(self.paramConfig, *args)
				except:
					raise RethrowError('Error while creating %s with arguments "%s"' % (cls, args))
			return wrapper
		userFun = dict(map(lambda (key, cls): (key, createWrapper(cls)), ParameterSource.managerMap.items()))
		try:
			source = eval(pExpr, userFun)
		except:
			utils.eprint('Available functions: %s' % userFun.keys())
			raise
		return ZipLongParameterSource(parent, source)


	def _getRawSource(self, parent):
		return BasicParameterFactory._getRawSource(self, self._getUserSource(self.pExpr, parent))
