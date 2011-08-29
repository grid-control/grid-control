import os, os.path, time, signal
from grid_control import Module, Config, GCError, ConfigError, UserError, utils, WMS
from provider_base import DataProvider
from splitter_base import DataSplitter

class DataMod(Module):
	def __init__(self, config, includeMap = False):
		Module.__init__(self, config)
		(self.dataSplitter, self.dataChange, self.includeMap) = (None, None, includeMap)
		self.dataset = config.get(self.__class__.__name__, 'dataset', '').strip()
		self.checkSE = config.getBool(self.__class__.__name__, 'dataset storage check', True, volatile=True)
		self.dataRefresh = None
		if self.dataset == '':
			return

		if os.path.exists(os.path.join(config.workDir, 'datamap.tar')):
			if config.opts.init and not config.opts.resync:
				utils.eprint('Re-Initialization of task will overwrite the current mapping between jobs and dataset content, which can lead to invalid results!')
				if utils.getUserBool('Do you want to perform a syncronization between the current mapping and the new one to avoid this?', True):
					config.opts.resync = True
		elif config.opts.init and config.opts.resync:
			config.opts.resync = False

		taskInfo = utils.PersistentDict(os.path.join(config.workDir, 'task.dat'), ' = ')
		self.defaultProvider = config.get(self.__class__.__name__, 'dataset provider', 'ListProvider')
		if config.opts.resync:
			self.dataChange = self.doResync()
			self.dataSplitter = self.dataChange[0]
		elif config.opts.init:
			# get datasets
			provider = DataProvider.create(config, self.__class__.__name__, self.dataset, self.defaultProvider)
			taskInfo.write({'max refresh rate': provider.queryLimit()})
			provider.saveState(config.workDir)
			if utils.verbosity() > 2:
				provider.printDataset()
			# split datasets
			splitterName = config.get(self.__class__.__name__, 'dataset splitter', 'FileBoundarySplitter')
			splitterClass = provider.checkSplitter(DataSplitter.getClass(splitterName))
			self.dataSplitter = splitterClass(config, self.__class__.__name__)
			blocks = provider.getBlocks()
			self.dataSplitter.splitDataset(os.path.join(config.workDir, 'datamap.tar'), blocks)
			self.printDatasetOverview(blocks)
			if utils.verbosity() > 2:
				self.dataSplitter.printAllJobInfo()
		else:
			# Load map between jobnum and dataset files
			self.dataSplitter = DataSplitter.loadState(os.path.join(config.workDir, 'datamap.tar'))
			if utils.verbosity() > 0:
				oldBlocks = DataProvider.loadState(self.config, self.config.workDir).getBlocks()
				self.printDatasetOverview(oldBlocks)

		# Select dataset refresh rate
		self.dataRefresh = utils.parseTime(config.get(self.__class__.__name__, 'dataset refresh', '', volatile=True))
		if self.dataRefresh > 0:
			self.dataRefresh = max(self.dataRefresh, taskInfo.get('max refresh rate', 0))
			utils.vprint('Dataset source will be queried every %s' % utils.strTime(self.dataRefresh), -1)
		self.lastRefresh = time.time()
		def externalRefresh(sig, frame):
			self.lastRefresh = 0
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


	# This function is here to allow ParaMod to transform jobNums
	def getTranslatedSplitInfo(self, jobNum):
		if self.dataSplitter == None:
			return {}
		return self.dataSplitter.getSplitInfo(jobNum)


	# Called on job submission
	def onJobSubmit(self, jobObj, jobNum, dbmessage = [{}]):
		splitInfo = self.getTranslatedSplitInfo(jobNum)
		Module.onJobSubmit(self, jobObj, jobNum, [{
			'nevtJob': splitInfo.get(DataSplitter.NEvents, 0),
			'datasetFull': splitInfo.get(DataSplitter.Dataset, '')}] + dbmessage)


	# Returns list of variables needed in config file
	def neededVars(self):
		if self.dataSplitter == None:
			return []
		varMap = {
			DataSplitter.NEvents: 'MAX_EVENTS',
			DataSplitter.Skipped: 'SKIP_EVENTS',
			DataSplitter.FileList: 'FILE_NAMES'
		}
		return map(lambda x: varMap[x], self.dataSplitter.neededVars())


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		data = Module.getJobConfig(self, jobNum)
		if self.dataSplitter == None:
			return data

		splitInfo = self.dataSplitter.getSplitInfo(jobNum)
		if utils.verbosity() > 0:
			utils.vprint('Job number: %d' % jobNum)
			DataSplitter.printInfoForJob(splitInfo)

		if not self.includeMap:
			data['FILE_NAMES'] = self.formatFileList(splitInfo[DataSplitter.FileList])
		data['MAX_EVENTS'] = splitInfo[DataSplitter.NEvents]
		data['SKIP_EVENTS'] = splitInfo.get(DataSplitter.Skipped, 0)
		data['DATASETID'] = splitInfo.get(DataSplitter.DatasetID, None)
		data['DATASETPATH'] = splitInfo.get(DataSplitter.Dataset, None)
		data['DATASETNICK'] = splitInfo.get(DataSplitter.Nickname, None)
		return data


	def formatFileList(self, filelist):
		return str.join(' ', filelist)


	# Get files for input sandbox
	def getInFiles(self):
		files = Module.getInFiles(self)
		if (self.dataSplitter != None) and self.includeMap:
			files.append(os.path.join(self.config.workDir, 'datamap.tar'))
		return files


	def getVarMapping(self):
		return utils.mergeDicts([Module.getVarMapping(self), {'NICK': 'DATASETNICK'}])


	# Get job requirements
	def getRequirements(self, jobNum):
		reqs = Module.getRequirements(self, jobNum)
		if self.dataSplitter != None:
			reqs.append((WMS.STORAGE, self.dataSplitter.getSplitInfo(jobNum).get(DataSplitter.SEList)))
		return reqs


	def canSubmit(self, jobNum):
		if self.checkSE and (self.dataSplitter != None):
			return self.dataSplitter.getSplitInfo(jobNum).get(DataSplitter.SEList) != []
		return True


	def getMaxJobs(self):
		if self.dataSplitter == None:
			return Module.getMaxJobs(self)
		return self.dataSplitter.getMaxJobs()


	def getDependencies(self):
		if (self.dataSplitter != None) and self.includeMap:
			return Module.getDependencies(self) + ['data']
		return Module.getDependencies(self)


	def report(self, jobNum):
		if self.dataSplitter == None:
			return Module.report(self, jobNum)
		info = self.dataSplitter.getSplitInfo(jobNum)
		return { 'Dataset': info.get(DataSplitter.Nickname, info.get(DataSplitter.Dataset, None)) }


	# Called on job submission
	def getSubmitInfo(self, jobNum):
		splitInfo = {}
		if self.dataSplitter:
			splitInfo = self.dataSplitter.getSplitInfo(jobNum)
		info = {'nevtJob': splitInfo.get(DataSplitter.NEvents, 0),
			'datasetFull': splitInfo.get(DataSplitter.Dataset, '')}
		return utils.mergeDicts([Module.getSubmitInfo(self, jobNum), info])


	def doResync(self):
		# Get old and new dataset information
		old = DataProvider.loadState(self.config, self.config.workDir).getBlocks()
		newProvider = DataProvider.create(self.config, self.__class__.__name__, self.dataset, self.defaultProvider)
		newProvider.saveState(self.config.workDir, 'datacache-new.dat')
		new = newProvider.getBlocks()
		self.printDatasetOverview(new)

		# Use old splitting information to synchronize with new dataset infos
		oldDataSplitter = DataSplitter.loadState(os.path.join(self.config.workDir, 'datamap.tar'))
		newSplitName = os.path.join(self.config.workDir, 'datamap-new.tar')
		jobChanges = oldDataSplitter.resyncMapping(newSplitName, old, new, self.config)

		# Move current splitting to backup and use the new splitting from now on
		def backupRename(old, cur, new):
			os.rename(os.path.join(self.config.workDir, cur), os.path.join(self.config.workDir, old))
			os.rename(os.path.join(self.config.workDir, new), os.path.join(self.config.workDir, cur))
		backupRename('datamap-old-%d.tar' % time.time(),   'datamap.tar',   'datamap-new.tar')
		backupRename('datacache-old-%d.dat' % time.time(), 'datacache.dat', 'datacache-new.dat')
		newDataSplitter = DataSplitter.loadState(os.path.join(self.config.workDir, 'datamap.tar'))
		return (oldDataSplitter, newDataSplitter, jobChanges)


	# Intervene in job management
	def getIntervention(self):
		jobChanges = None
		if self.dataSplitter:
			# Perform automatic resync
			if not self.dataChange and self.dataRefresh > 0:
				if time.time() - self.lastRefresh > self.dataRefresh:
					self.lastRefresh = time.time()
					self.dataChange = self.doResync()
			# Deal with job changes
			if self.dataChange:
				(oldDataSplitter, newDataSplitter, jobChanges) = self.dataChange
				self.dataSplitter = newDataSplitter
				self.dataChange = None
		return jobChanges


	def onTaskFinish(self):
		return not (self.dataRefresh > 0)
