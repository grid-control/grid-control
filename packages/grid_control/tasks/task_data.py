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
	def _setup_repository(self, config, psrc_repository):
		TaskModule._setup_repository(self, config, psrc_repository)

		psrc_list = []
		for datasource_name in config.getList('datasource names', ['dataset'], onChange = TriggerResync(['datasets', 'parameters'])):
			data_config = config.changeView(view_class = 'TaggedConfigView', addSections = [datasource_name])
			psrc_data = self._create_datasource(data_config, datasource_name, psrc_repository)
			if psrc_data is not None:
				psrc_list.append(psrc_data)
				self._has_dataset = True
				tmp_config = data_config.changeView(view_class = 'TaggedConfigView', setClasses = None, setNames = None, setTags = [], addSections = ['storage'])
				tmp_config.set('se output pattern', '@NICK@_job_@GC_JOB_ID@_@X@')
				tmp_config = data_config.changeView(view_class = 'TaggedConfigView', setClasses = None, setNames = None, setTags = [], addSections = ['parameters'])
				tmp_config.set('default lookup', 'DATASETNICK')

		self._has_dataset = (psrc_list != [])

		# Register signal handler for manual dataset refresh
		def externalRefresh(sig, frame):
			for psrc in psrc_list:
				self._log.info('External signal triggered resync of datasource %r', psrc.get_name())
				psrc.resyncSetup(force = True)
		signal.signal(signal.SIGUSR2, externalRefresh)

		config.setState(False, 'resync', detail = 'datasets')


	def _create_datasource(self, config, datasource_name, psrc_repository):
		dataProvider = config.getCompositePlugin(datasource_name, '', ':MultiDatasetProvider:',
			cls = DataProvider, requirePlugin = False, onChange = TriggerResync(['datasets', 'parameters']))

		if dataProvider is not None:
			splitterName = config.get('%s splitter' % datasource_name, 'FileBoundarySplitter')
			splitterClass = dataProvider.checkSplitter(DataSplitter.get_class(splitterName))
			dataSplitter = splitterClass(config, datasource_name)

			# Create and register dataset parameter source
			partProcessor = config.getCompositePlugin(['partition processor', '%s partition processor' % datasource_name],
				'TFCPartitionProcessor LocationPartitionProcessor MetaPartitionProcessor BasicPartitionProcessor',
				'MultiPartitionProcessor', cls = PartitionProcessor, onChange = TriggerResync(['parameters']),
				pargs = (datasource_name,))

			data_ps = ParameterSource.create_instance('DataParameterSource', config.getWorkPath(),
				datasource_name.replace('dataset', 'data'), # needed for backwards compatible file names: datacache/datamap
				dataProvider, dataSplitter, partProcessor, psrc_repository)

			# Select dataset refresh rate
			data_refresh = config.getTime('%s refresh' % datasource_name, -1, onChange = None)
			if data_refresh >= 0:
				data_refresh = max(data_refresh, dataProvider.queryLimit())
				self._log.info('Dataset source will be queried every %s', str_time_long(data_refresh))
			data_ps.resyncSetup(interval = data_refresh, force = config.getState('resync', detail = 'datasets'))
			if dataSplitter.get_partition_len() == 0:
				if data_refresh < 0:
					raise UserError('Currently used dataset does not provide jobs to process')
				self._log.warning('Currently used dataset does not provide jobs to process')
			return data_ps


	def getVarMapping(self):
		if self._has_dataset: # create alias NICK for DATASETNICK
			return utils.merge_dict_list([TaskModule.getVarMapping(self), {'NICK': 'DATASETNICK'}])
		return TaskModule.getVarMapping(self)
