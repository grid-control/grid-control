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
from python_compat import lfilter

class DataTask(TaskModule):
	def setupJobParameters(self, config, pm):
		data_config = config.changeView(viewClass = 'TaggedConfigView', addSections = ['dataset'])
		self.dataSplitter = None
		self.dataRefresh = -1
		def userRefresh(config, old_obj, cur_obj, cur_entry, obj2str):
			if (old_obj == '') and (cur_obj != ''):
				raise UserError('It is currently not possible to attach a dataset to a non-dataset task!')
			self._log.info('Dataset setup was changed - forcing resync...')
			config.setState(True, 'resync', detail = 'dataset')
			config.setState(True, 'init', detail = 'config') # This will trigger a write of the new options
			return cur_obj
		dataProvider = data_config.getCompositePlugin('dataset', '', ':MultiDatasetProvider:',
			cls = DataProvider, requirePlugin = False, onChange = userRefresh)
		self._forceRefresh = config.getState('resync', detail = 'dataset')
		config.setState(False, 'resync', detail = 'dataset')
		if not dataProvider:
			return

		tmp_config = data_config.changeView(viewClass = 'TaggedConfigView', setClasses = None, setNames = None, setTags = [], addSections = ['storage'])
		tmp_config.set('se output pattern', '@NICK@_job_@GC_JOB_ID@_@X@')
		tmp_config = data_config.changeView(viewClass = 'TaggedConfigView', setClasses = None, setNames = None, setTags = [], addSections = ['parameters'])
		tmp_config.set('default lookup', 'DATASETNICK')

		splitterName = data_config.get('dataset splitter', 'FileBoundarySplitter')
		splitterClass = dataProvider.checkSplitter(DataSplitter.getClass(splitterName))
		self.dataSplitter = splitterClass(data_config)

		# Create and register dataset parameter source
		partProcessor = data_config.getCompositePlugin('partition processor',
			'TFCPartitionProcessor LocationPartitionProcessor MetaPartitionProcessor BasicPartitionProcessor',
			'MultiPartitionProcessor', cls = PartitionProcessor, onChange = triggerResync(['parameters']))
		DataParameterSource = ParameterSource.getClass('DataParameterSource')
		self._dataPS = DataParameterSource(data_config.getWorkPath(), 'data',
			dataProvider, self.dataSplitter, partProcessor)
		DataParameterSource.datasetsAvailable['data'] = self._dataPS

		# Select dataset refresh rate
		self.dataRefresh = data_config.getTime('dataset refresh', -1, onChange = None)
		if self.dataRefresh > 0:
			self._dataPS.resyncSetup(interval = max(self.dataRefresh, dataProvider.queryLimit()))
			utils.vprint('Dataset source will be queried every %s' % strTime(self.dataRefresh), -1)
		else:
			self._dataPS.resyncSetup(interval = 0)
		if self._forceRefresh:
			self._dataPS.resyncSetup(force = True)
		def externalRefresh(sig, frame):
			self._dataPS.resyncSetup(force = True)
		signal.signal(signal.SIGUSR2, externalRefresh)

		if self.dataSplitter.getMaxJobs() == 0:
			raise UserError('There are no events to process')


	def getVarMapping(self):
		if self.dataSplitter:
			return utils.mergeDicts([TaskModule.getVarMapping(self), {'NICK': 'DATASETNICK'}])
		return TaskModule.getVarMapping(self)


	# Called on job submission
	def getSubmitInfo(self, jobNum):
		jobInfo = self.source.getJobInfo(jobNum)
		submitInfo = {'nevtJob': jobInfo.get('MAX_EVENTS', 0),
			'datasetFull': jobInfo.get('DATASETPATH', 'none')}
		return utils.mergeDicts([TaskModule.getSubmitInfo(self, jobNum), submitInfo])


	def canFinish(self):
		return self.dataRefresh <= 0


	def report(self, jobNum):
		info = self.source.getJobInfo(jobNum)
		keys = lfilter(lambda k: not k.untracked, self.source.getJobKeys())
		result = utils.filterDict(info, kF = lambda k: k in keys)
		if self.dataSplitter:
			result.pop('DATASETSPLIT')
			result['Dataset'] = info.get('DATASETNICK', info.get('DATASETPATH', None))
		elif not keys:
			result[' '] = 'All jobs'
		return result
