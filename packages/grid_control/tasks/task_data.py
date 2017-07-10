# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import signal
from grid_control.config import TriggerResync
from grid_control.parameters import ParameterSource
from grid_control.tasks.task_base import TaskModule
from grid_control.utils.algos import dict_union


class DataTask(TaskModule):
	def get_var_alias_map(self):
		if self._has_dataset:  # create alias NICK for DATASETNICK
			return dict_union(TaskModule.get_var_alias_map(self), {'NICK': 'DATASETNICK'})
		return TaskModule.get_var_alias_map(self)

	def _create_datasource(self, config, datasource_name, psrc_repository, psrc_list):
		data_ps = ParameterSource.create_instance('DataParameterSource',
			config, datasource_name, psrc_repository)
		if not isinstance(data_ps, ParameterSource.get_class('NullParameterSource')):
			config.set('se output pattern', '@NICK@_job_@GC_JOB_ID@_@X@', section='storage')
			config.set('default lookup', 'DATASETNICK', section='parameters')
			psrc_list.append(data_ps)
			return data_ps

	def _setup_repository(self, config, psrc_repository):
		TaskModule._setup_repository(self, config, psrc_repository)

		psrc_list = []
		for datasource_name in config.get_list('datasource names', ['dataset'],
				on_change=TriggerResync(['datasets', 'parameters'])):
			data_config = config.change_view(view_class='TaggedConfigView', add_sections=[datasource_name])
			self._create_datasource(data_config, datasource_name, psrc_repository, psrc_list)
		self._has_dataset = (psrc_list != [])

		# Register signal handler for manual dataset refresh
		def _external_refresh(sig, frame):
			for psrc in psrc_list:
				self._log.info('External signal triggered resync of datasource %r', psrc.get_datasource_name())
				psrc.setup_resync(force=True)
		signal.signal(signal.SIGUSR2, _external_refresh)

		config.set_state(False, 'resync', detail='datasets')
