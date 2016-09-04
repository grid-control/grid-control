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

from grid_control.parameters.pfactory_base import UserParameterFactory
from grid_control.parameters.psource_base import ParameterError, ParameterSource
from python_compat import ifilter, sorted

# Parameter factory which evaluates a parameter module string
class ModularParameterFactory(UserParameterFactory):
	alias = ['modular']

	def _getUserSource(self, pExpr):
		# Wrap psource factory functions
		def createWrapper(clsName):
			def wrapper(*args):
				parameterClass = ParameterSource.getClass(clsName)
				try:
					return parameterClass.create(self._paramConfig, self._repository, *args)
				except Exception:
					raise ParameterError('Error while creating %r with arguments %r' % (parameterClass.__name__, args))
			return wrapper
		userFun = {}
		for clsInfo in ParameterSource.getClassList():
			for clsName in ifilter(lambda name: name != 'depth', clsInfo.keys()):
				userFun[clsName] = createWrapper(clsName)
		try:
			return eval(pExpr, dict(userFun)) # pylint:disable=eval-used
		except Exception:
			self._log.warning('Available functions: %s', sorted(userFun.keys()))
			raise
