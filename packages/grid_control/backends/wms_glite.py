# | Copyright 2007-2017 Karlsruhe Institute of Technology
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

from grid_control.backends.wms_grid import GridCancelJobs, GridCheckJobs, GridWMS
from grid_control.utils import deprecated, resolve_install_path


class Glite(GridWMS):
	def __init__(self, config, name):
		deprecated('Please use the GliteWMS backend for grid jobs!')
		GridWMS.__init__(self, config, name,
			submit_exec=resolve_install_path('glite-job-submit'),
			output_exec=resolve_install_path('glite-job-output'),
			check_executor=GridCheckJobs(config, 'glite-job-status'),
			cancel_executor=GridCancelJobs(config, 'glite-job-cancel'))
		self._submit_args_dict.update({'-r': self._ce, '--config-vo': self._config_fn})
