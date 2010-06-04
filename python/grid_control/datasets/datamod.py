import os.path, random, time
from grid_control import Module, Config, ConfigError, utils, WMS
from provider_base import DataProvider
from splitter_base import DataSplitter

class DataMod(Module):
	def __init__(self, config, includeMap = False):
		Module.__init__(self, config)
		self.includeMap = includeMap

		self.dataSplitter = None
		self.dataset = config.get(self.__class__.__name__, 'dataset', '').strip()
		if self.dataset == '':
			return

		(defaultProvider, defaultSplitter) = self.getDatasetDefaults(config)
		defaultProvider = config.get(self.__class__.__name__, 'dataset provider', defaultProvider)
		if config.opts.init:
			# find datasets
			self.dataprovider = DataProvider.create(config, self.__class__.__name__, self.dataset, defaultProvider)
			self.dataprovider.saveState(config.workDir)
			if utils.verbosity() > 2:
				self.dataprovider.printDataset()

			# split datasets
			splitter = config.get(self.__class__.__name__, 'dataset splitter', defaultSplitter)
			splitter = self.dataprovider.checkSplitter(splitter)
			self.dataSplitter = DataSplitter.open(splitter, config, self.__class__.__name__, {})
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
				new = DataProvider.create(config, self.__class__.__name__, self.dataset, defaultProvider)
				self.dataSplitter.resyncMapping(config.workDir, old.getBlocks(), new.getBlocks())
				#TODO: new.saveState(config.workDir)


	# Get default dataset modules
	def getDatasetDefaults(self, config):
		return ('ListProvider', 'FileBoundarySplitter')


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
			print "Job number: %d" % jobNum
			DataSplitter.printInfoForJob(splitInfo)

		data['MAX_EVENTS'] = splitInfo[DataSplitter.NEvents]
		data['SKIP_EVENTS'] = splitInfo[DataSplitter.Skipped]
		if not self.includeMap:
			data['FILE_NAMES'] = self.formatFileList(splitInfo[DataSplitter.FileList])
		data['DATASETID'] = splitInfo.get(DataSplitter.DatasetID, None)
		data['DATASETPATH'] = splitInfo.get(DataSplitter.Dataset, None)
		data['DATASETNICK'] = splitInfo.get(DataSplitter.Nickname, None)
		return data


	def formatFileList(self, filelist):
		return str.join(" ", filelist)


	# Get files for input sandbox
	def getInFiles(self):
		files = Module.getInFiles(self)
		if (self.dataSplitter != None) and self.includeMap:
			files.append(os.path.join(self.config.workDir, 'datamap.tar'))
		return files


	def getVarMapping(self):
		return dict(Module.getVarMapping(self).items() + [('NICK', 'DATASETNICK')])


	# Get job requirements
	def getRequirements(self, jobNum):
		reqs = Module.getRequirements(self, jobNum)
		if self.dataSplitter != None:
			selist = self.dataSplitter.getSplitInfo(jobNum).get(DataSplitter.SEList, False)
			if selist != False:
				reqs.append((WMS.STORAGE, selist))
		return reqs


	def getMaxJobs(self):
		if self.dataSplitter == None:
			return Module.getMaxJobs(self)
		return self.dataSplitter.getNumberOfJobs()


	def getDependencies(self):
		if (self.dataSplitter != None) and self.includeMap:
			return Module.getDependencies(self) + ['data']
		return Module.getDependencies(self)


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
