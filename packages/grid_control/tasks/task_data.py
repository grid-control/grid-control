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
from grid_control.config import triggerResync
from grid_control.datasets import DataProvider, DataSplitter, PartitionProcessor
from grid_control.gc_exceptions import UserError
from grid_control.parameters import ParameterSource
from grid_control.tasks.task_base import TaskModule
from grid_control.utils.parsing import strTime

class DataTask(TaskModule):
	def _setupJobParameters(self, config, psrc_repository):
		TaskModule._setupJobParameters(self, config, psrc_repository)
		data_config = config.changeView(viewClass = 'TaggedConfigView', addSections = ['dataset'])
		self._dataSplitter = None
		dataProvider = data_config.getCompositePlugin('dataset', '', ':MultiDatasetProvider:',
			cls = DataProvider, requirePlugin = False, onChange = triggerResync(['datasets', 'parameters']))
		self._forceRefresh = config.getState('resync', detail = 'datasets')
		config.setState(False, 'resync', detail = 'datasets')
		if not dataProvider:
			return

		tmp_config = data_config.changeView(viewClass = 'TaggedConfigView', setClasses = None, setNames = None, setTags = [], addSections = ['storage'])
		tmp_config.set('se output pattern', '@NICK@_job_@GC_JOB_ID@_@X@')
		tmp_config = data_config.changeView(viewClass = 'TaggedConfigView', setClasses = None, setNames = None, setTags = [], addSections = ['parameters'])
		tmp_config.set('default lookup', 'DATASETNICK')

		splitterName = data_config.get('dataset splitter', 'FileBoundarySplitter')
		splitterClass = dataProvider.checkSplitter(DataSplitter.getClass(splitterName))
		self._dataSplitter = splitterClass(data_config)

		# Create and register dataset parameter source
		self._partProcessor = data_config.getCompositePlugin('partition processor',
			'TFCPartitionProcessor LocationPartitionProcessor MetaPartitionProcessor BasicPartitionProcessor',
			'MultiPartitionProcessor', cls = PartitionProcessor, onChange = triggerResync(['parameters']))
		dataPS = ParameterSource.createInstance('DataParameterSource', data_config.getWorkPath(), 'data',
			dataProvider, self._dataSplitter, self._partProcessor, psrc_repository)

		# Select dataset refresh rate
		data_refresh = data_config.getTime('dataset refresh', -1, onChange = None)
		if data_refresh >= 0:
			data_refresh = max(data_refresh, dataProvider.queryLimit())
			self._log.info('Dataset source will be queried every %s', strTime(data_refresh))
		dataPS.resyncSetup(interval = data_refresh, force = self._forceRefresh)
		def externalRefresh(sig, frame):
			self._log.info('External signal triggered resync of dataset source')
			dataPS.resyncSetup(force = True)
		signal.signal(signal.SIGUSR2, externalRefresh)

		if self._dataSplitter.getMaxJobs() == 0:
			if data_refresh < 0:
				raise UserError('Currently used dataset does not provide jobs to process')
			self._log.warning('Currently used dataset does not provide jobs to process')


	def getVarMapping(self):
		if self._dataSplitter:
			return utils.mergeDicts([TaskModule.getVarMapping(self), {'NICK': 'DATASETNICK'}])
		return TaskModule.getVarMapping(self)
