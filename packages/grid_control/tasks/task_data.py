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

import signal
from grid_control import utils
from grid_control.config import TriggerResync
from grid_control.datasets import DataProvider, DataSplitter, PartitionProcessor
from grid_control.gc_exceptions import UserError
from grid_control.parameters import ParameterSource
from grid_control.tasks.task_base import TaskModule
from grid_control.utils.parsing import str_time_long


class DataTask(TaskModule):
	def get_var_alias_map(self):
		if self._has_dataset:  # create alias NICK for DATASETNICK
			return utils.merge_dict_list([TaskModule.get_var_alias_map(self), {'NICK': 'DATASETNICK'}])
		return TaskModule.get_var_alias_map(self)

	def _create_datasource(self, config, datasource_name, psrc_repository):
		provider_name_default = config.get('%s provider' % datasource_name, 'ListProvider')
		provider = config.get_composited_plugin(datasource_name, '', ':MultiDatasetProvider:',
			cls=DataProvider, require_plugin=False, on_change=TriggerResync(['datasets', 'parameters']),
			bkwargs={'datasource_name': datasource_name, 'provider_name_default': provider_name_default})

		if provider is not None:
			splitter_name = config.get('%s splitter' % datasource_name, 'FileBoundarySplitter')
			splitter_cls = provider.check_splitter(DataSplitter.get_class(splitter_name))
			splitter = splitter_cls(config, datasource_name)

			# Create and register dataset parameter source
			partition_config = config.change_view(default_on_change=None)
			partition_processor = partition_config.get_composited_plugin(
				['partition processor', '%s partition processor' % datasource_name],
				'TFCPartitionProcessor LocationPartitionProcessor ' +
				'MetaPartitionProcessor BasicPartitionProcessor',
				'MultiPartitionProcessor', cls=PartitionProcessor, on_change=TriggerResync(['parameters']),
				pargs=(datasource_name,))

			data_ps = ParameterSource.create_instance('DataParameterSource', config.get_work_path(),
				# needed for backwards compatible file names: datacache/datamap
				datasource_name.replace('dataset', 'data'),
				provider, splitter, partition_processor, psrc_repository)

			# Select dataset refresh rate
			data_refresh = config.get_time('%s refresh' % datasource_name, -1, on_change=None)
			if data_refresh >= 0:
				data_refresh = max(data_refresh, provider.get_query_interval())
				self._log.info('Dataset source will be queried every %s', str_time_long(data_refresh))
			data_ps.setup_resync(interval=data_refresh, force=config.get_state('resync', detail='datasets'))
			if splitter.get_partition_len() == 0:
				if data_refresh < 0:
					raise UserError('Currently used dataset does not provide jobs to process')
				self._log.warning('Currently used dataset does not provide jobs to process')
			return data_ps

	def _setup_repository(self, config, psrc_repository):
		TaskModule._setup_repository(self, config, psrc_repository)

		psrc_list = []
		for datasource_name in config.get_list('datasource names', ['dataset'],
				on_change=TriggerResync(['datasets', 'parameters'])):
			data_config = config.change_view(view_class='TaggedConfigView', add_sections=[datasource_name])
			psrc_data = self._create_datasource(data_config, datasource_name, psrc_repository)
			if psrc_data is not None:
				psrc_list.append(psrc_data)
				self._has_dataset = True
				tmp_config = data_config.change_view(view_class='TaggedConfigView',
					set_classes=None, set_names=None, set_tags=[], add_sections=['storage'])
				tmp_config.set('se output pattern', '@NICK@_job_@GC_JOB_ID@_@X@')
				tmp_config = data_config.change_view(view_class='TaggedConfigView',
					set_classes=None, set_names=None, set_tags=[], add_sections=['parameters'])
				tmp_config.set('default lookup', 'DATASETNICK')

		self._has_dataset = (psrc_list != [])

		# Register signal handler for manual dataset refresh
		def _external_refresh(sig, frame):
			for psrc in psrc_list:
				self._log.info('External signal triggered resync of datasource %r', psrc.get_name())
				psrc.setup_resync(force=True)
		signal.signal(signal.SIGUSR2, _external_refresh)

		config.set_state(False, 'resync', detail='datasets')
