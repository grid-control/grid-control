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

import logging
from grid_control.parameters.pfactory_base import BasicParameterFactory
from grid_control.parameters.psource_base import ParameterError, ParameterSource
from grid_control.parameters.psource_meta import ZipLongParameterSource

# Parameter factory which evaluates a parameter module string
class ModularParameterFactory(BasicParameterFactory):
	def __init__(self, config, name):
		BasicParameterFactory.__init__(self, config, name)
		self._pExpr = self._paramConfig.get('parameters', '')


	def _getUserSource(self, pExpr, parent):
		if not pExpr:
			return parent
		# Wrap psource factory functions
		def createWrapper(clsName):
			def wrapper(*args):
				try:
					parameterClass = ParameterSource.getClass(clsName)
				except Exception:
					raise ParameterError('Unable to create parameter source "%r"!' % clsName)
				try:
					return parameterClass.create(self._paramConfig, *args)
				except Exception:
					raise ParameterError('Error while creating "%r" with arguments "%r"' % (parameterClass.__name__, args))
			return wrapper
		userFun = {}
		for (key, cls) in ParameterSource.managerMap.items():
			userFun[key] = createWrapper(cls)
		try:
			source = eval(pExpr, userFun) # pylint:disable=eval-used
		except Exception:
			logging.getLogger('user').warning('Available functions: %s', userFun.keys())
			raise
		return ZipLongParameterSource(parent, source)


	def _getRawSource(self, parent):
		return BasicParameterFactory._getRawSource(self, self._getUserSource(self._pExpr, parent))
