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

from grid_control.parameters.pfactory_base import UserParameterFactory
from grid_control.parameters.psource_base import ParameterSource
from python_compat import ifilter, partial, sorted


class ModularParameterFactory(UserParameterFactory):
	# Parameter factory which evaluates a parameter module string
	alias_list = ['modular']

	def _get_psrc_user(self, pexpr, repository):
		user_functions = {}
		for cls_info in ParameterSource.get_class_info_list():
			for cls_name in ifilter(lambda name: name != 'depth', cls_info.keys()):
				user_functions[cls_name] = partial(ParameterSource.create_psrc_safe,
					cls_name, self._parameter_config, repository)
		try:
			return eval(pexpr, dict(user_functions))  # pylint:disable=eval-used
		except Exception:
			self._log.warning('Available functions: %s', sorted(user_functions.keys()))
			raise
