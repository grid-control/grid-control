import os, os.path, time, signal
from task_base import TaskModule
from grid_control import Config, GCError, ConfigError, UserError, utils, WMS
from grid_control import datasets
from grid_control.datasets import DataProvider, DataSplitter
from grid_control.parameters.psource_data import ParameterSource, DataParameterSource, DataSplitProcessor

class DataTask(TaskModule):
	def setupJobParameters(self, config, pm):
		self.dataSplitter = None
		self.dataRefresh = None
		self.dataset = config.get(self.__class__.__name__, 'dataset', '').strip()
		if self.dataset == '':
			return
		config.set('storage', 'se output pattern', '@NICK@_job_@MY_JOBID@_@X@', override=False)
		config.set('parameters', 'default lookup', 'DATASETNICK', override=False)

		defaultProvider = config.get(self.__class__.__name__, 'dataset provider', 'ListProvider')
		dataProvider = DataProvider.create(config, self.__class__.__name__, self.dataset, defaultProvider)
		splitterName = config.get(self.__class__.__name__, 'dataset splitter', 'FileBoundarySplitter')
		splitterClass = dataProvider.checkSplitter(DataSplitter.getClass(splitterName))
		self.dataSplitter = splitterClass(config, self.__class__.__name__)
		self.checkSE = config.getBool(self.__class__.__name__, 'dataset storage check', True, onChange = None)

		# Create and register dataset parameter plugin
		paramSource = DataParameterSource(config.workDir, 'data',
			dataProvider, self.dataSplitter, self.initDataProcessor())
		DataParameterSource.datasetsAvailable['data'] = paramSource

		# Select dataset refresh rate
		self.dataRefresh = utils.parseTime(config.get(self.__class__.__name__, 'dataset refresh', '', onChange = None))
		if self.dataRefresh > 0:
			paramSource.resyncSetup(interval = max(self.dataRefresh, dataProvider.queryLimit()))
			utils.vprint('Dataset source will be queried every %s' % utils.strTime(self.dataRefresh), -1)
		else:
			paramSource.resyncSetup(interval = 0)
		def externalRefresh(sig, frame):
			paramSource.resyncSetup(force = True)
		signal.signal(signal.SIGUSR2, externalRefresh)

		if config.opts.init:
			if utils.verbosity() > 2:
				dataProvider.printDataset()
			if utils.verbosity() > 2:
				self.dataSplitter.printAllJobInfo()

		if self.dataSplitter.getMaxJobs() == 0:
			raise UserError('There are no events to process')


	def initDataProcessor(self):
		return DataSplitProcessor(self.checkSE)


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
