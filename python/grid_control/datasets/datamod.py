import os.path, random, time
from grid_control import Module, utils, WMS
from provider_base import DataProvider
from splitter_base import DataSplitter

class DataMod(Module):
	def __init__(self, config):
		Module.__init__(self, config)

		self.dataSplitter = None
		self.dataset = config.get(self.__class__.__name__, 'dataset', '').strip()
		if self.dataset == '':
			return

		defaultProvider = config.get(self.__class__.__name__, 'dbsapi', self.getDefaultProvider())
		if config.opts.init:
			# find datasets
			self.dataprovider = DataProvider.create(config, self.dataset, defaultProvider)
			self.dataprovider.saveState(config.workDir)
			if utils.verbosity() > 2:
				self.dataprovider.printDataset()

			# split datasets
			splitter = config.get(self.__class__.__name__, 'dataset splitter', 'DefaultSplitter')
			eventsPerJob = config.getInt(self.__class__.__name__, 'events per job')
			self.dataSplitter = DataSplitter.open(splitter, { "eventsPerJob": eventsPerJob })
			self.dataSplitter.splitDataset(self.dataprovider.getBlocks())
			self.dataSplitter.saveState(config.workDir)
			if utils.verbosity() > 2:
				self.dataSplitter.printAllJobInfo()
		else:
			# load map between jobnum and dataset files
			try:
				self.dataSplitter = DataSplitter.loadState(config.workDir)
			except:
				raise ConfigError("Current dataset splitting not found in '%s'." % config.workDir)
			if config.opts.resync:
				old = DataProvider.loadState(config, config.workDir)
				new = DataProvider.create(config, self.dataset, defaultProvider)
				self.dataSplitter.resyncMapping(config.workDir, old.getBlocks(), new.getBlocks())
				#TODO: new.saveState(config.workDir)


	# Get default dataset provider
	def getDefaultProvider(self):
		return 'ListProvider'


	# This function is here to allow ParaMod to transform jobNums
	def getTranslatedSplitInfo(self, jobNum):
		if self.dataSplitter == None:
			return {}
		return self.dataSplitter.getSplitInfo(jobNum)


	# Called on job submission
	def onJobSubmit(self, jobObj, jobNum, dbmessage = [{}]):
		splitInfo = self.getTranslatedSplitInfo(jobNum)
		Module.onJobSubmit(self, jobObj, jobNum, [{
			"nevtJob": splitInfo.get(DataSplitter.NEvents, 0),
			"datasetFull": splitInfo.get(DataSplitter.Dataset, '')}] + dbmessage)


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		data = Module.getJobConfig(self, jobNum)
		if self.dataSplitter == None:
			return data

		splitInfo = self.dataSplitter.getSplitInfo(jobNum)
		data['DATASETID'] = splitInfo.get(DataSplitter.DatasetID, None)
		data['DATASETPATH'] = splitInfo.get(DataSplitter.Dataset, None)
		data['DATASETNICK'] = splitInfo.get(DataSplitter.Nickname, None)
		return data


	def getVarMapping(self):
		tmp = ['MAX_EVENTS', 'SKIP_EVENTS', 'FILE_NAMES']
		return dict(Module.getVarMapping(self).items() + zip(tmp, tmp) + [('NICK', 'DATASETNICK')])


	# Get job requirements
	def getRequirements(self, jobNum):
		reqs = Module.getRequirements(self, jobNum)
		if self.dataSplitter != None:
			splitInfo = self.dataSplitter.getSplitInfo(jobNum)
			reqs.append((WMS.STORAGE, splitInfo.get(DataSplitter.SEList, [])))
		return reqs


	def getJobArguments(self, jobNum):
		if self.dataSplitter == None:
			return Module.getJobArguments(self, jobNum)

		splitInfo = self.dataSplitter.getSplitInfo(jobNum)
		if utils.verbosity() > 0:
			print "Job number: %d" % jobNum
			DataSplitter.printInfoForJob(splitInfo)
		return "%d %d %s" % (
			splitInfo[DataSplitter.NEvents],
			splitInfo[DataSplitter.Skipped],
			str.join(' ', splitInfo[DataSplitter.FileList])
		)


	def getMaxJobs(self):
		if self.dataSplitter == None:
			return Module.getMaxJobs(self)
		return self.dataSplitter.getNumberOfJobs()


	def getDependencies(self):
		if self.dataSplitter == None:
			return Module.getDependencies(self)
		return Module.getDependencies(self) + [] # TODO: Dataset env script


	def report(self, jobNum):
		if self.dataSplitter == None:
			return Module.report(self, jobNum)

		info = self.dataSplitter.getSplitInfo(jobNum)
		name = info.get(DataSplitter.Nickname, info.get(DataSplitter.Dataset, None))
		return { "Dataset": name }


	# Called on job submission
	def getSubmitInfo(self, jobNum):
		splitInfo = {}
		if self.dataSplitter:
			splitInfo = self.dataSplitter.getSplitInfo(jobNum)
		try:
			nEvents = int(splitInfo.get(DataSplitter.NEvents))
		except:
			nEvents = 0
		result = Module.getSubmitInfo(self, jobNum)
		result.update({ "nevtJob": nEvents, "datasetFull": splitInfo.get(DataSplitter.Dataset, '') })
		return result
