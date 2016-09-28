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
	alias_list = ['modular']

	def _get_source_user(self, pexpr, repository):
		# Wrap psource factory functions
		def create_wrapper(cls_name):
			def wrapper(*args):
				psrc_type = ParameterSource.get_class(cls_name)
				try:
					return psrc_type.create_psrc(self._parameter_config, repository, *args)
				except Exception:
					err_msg = 'Error while creating %r with arguments %r'
					raise ParameterError(err_msg % (psrc_type.__name__, args))
			return wrapper
		user_functions = {}
		for cls_info in ParameterSource.get_class_info_list():
			for cls_name in ifilter(lambda name: name != 'depth', cls_info.keys()):
				user_functions[cls_name] = create_wrapper(cls_name)
		try:
			return eval(pexpr, dict(user_functions))  # pylint:disable=eval-used
		except Exception:
			self._log.warning('Available functions: %s', sorted(user_functions.keys()))
			raise
