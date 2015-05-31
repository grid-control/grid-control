#-#  Copyright 2009-2015 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, signal
from grid_control import utils
from grid_control.abstract import ClassFactory
from grid_control.config import TaggedConfigView
from grid_control.datasets import DataProvider, DataSplitter
from grid_control.exceptions import UserError
from grid_control.parameters import DataParameterSource, DataSplitProcessor, ParameterSource
from grid_control.tasks.task_base import TaskModule

class DataTask(TaskModule):
	def setupJobParameters(self, config, pm):
		config = config.changeView(viewClass = TaggedConfigView, addSections = ['dataset'], addTags = [self])
		self.dataSplitter = None
		self.dataRefresh = None
		self.dataset = config.get('dataset', '').strip()
		if self.dataset == '':
			return
		config.set('se output pattern', '@NICK@_job_@GC_JOB_ID@_@X@')
		config.set('default lookup', 'DATASETNICK')

		defaultProvider = config.get('dataset provider', 'ListProvider')
		dataProvider = DataProvider.create(config, self.dataset, defaultProvider)
		splitterName = config.get('dataset splitter', 'FileBoundarySplitter')
		splitterClass = dataProvider.checkSplitter(DataSplitter.getClass(splitterName))
		self.dataSplitter = splitterClass(config)

		# Create and register dataset parameter plugin
		paramSplitProcessor = ClassFactory(config,
			('dataset processor', 'BasicDataSplitProcessor SECheckSplitProcessor'),
			('dataset processor manager', 'MultiDataSplitProcessor'),
			cls = DataSplitProcessor).getInstance(config)
		paramSource = DataParameterSource(config.getWorkPath(), 'data',
			dataProvider, self.dataSplitter, paramSplitProcessor)
		DataParameterSource.datasetsAvailable['data'] = paramSource

		# Select dataset refresh rate
		self.dataRefresh = config.getTime('dataset refresh', -1, onChange = None)
		if self.dataRefresh > 0:
			paramSource.resyncSetup(interval = max(self.dataRefresh, dataProvider.queryLimit()))
			utils.vprint('Dataset source will be queried every %s' % utils.strTime(self.dataRefresh), -1)
		else:
			paramSource.resyncSetup(interval = 0)
		def externalRefresh(sig, frame):
			paramSource.resyncSetup(force = True)
		signal.signal(signal.SIGUSR2, externalRefresh)

		if self.dataSplitter.getMaxJobs() == 0:
			raise UserError('There are no events to process')


	def getDatasetOverviewInfo(self, blocks):
		head = [(DataProvider.DatasetID, 'ID'), (DataProvider.Nickname, 'Nickname'), (DataProvider.Dataset, 'Dataset path')]
		blockInfos = []
		for block in blocks:
			shortProvider = DataProvider.providers.get(block[DataProvider.Provider], block[DataProvider.Provider])
			value = {DataProvider.DatasetID: block.get(DataProvider.DatasetID, 0),
				DataProvider.Nickname: block.get(DataProvider.Nickname, ''),
				DataProvider.Dataset: '%s://%s' % (shortProvider, block[DataProvider.Dataset])}
			if value not in blockInfos:
				blockInfos.append(value)
		return (head, blockInfos, {})


	def printDatasetOverview(self, blocks):
		(head, blockInfos, fmt) = self.getDatasetOverviewInfo(blocks)
		utils.vprint('Using the following datasets:', -1)
		utils.vprint(level = -1)
		utils.printTabular(head, blockInfos, 'rcl', fmt = fmt)
		utils.vprint(level = -1)


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
		return not (self.dataRefresh > 0)


	def report(self, jobNum):
		info = self.source.getJobInfo(jobNum)
		keys = filter(lambda k: k.untracked == False, self.source.getJobKeys())
		result = utils.filterDict(info, kF = lambda k: k in keys)
		if self.dataSplitter:
			result.pop('DATASETSPLIT')
			result['Dataset'] = info.get('DATASETNICK', info.get('DATASETPATH', None))
		elif not keys:
			result[' '] = 'All jobs'
		return result
