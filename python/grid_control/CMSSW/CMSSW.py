import os, string, re, sys, tarfile
from grid_control import ConfigError, Module, WMS, utils
from provider_base import DataProvider
from splitter_base import DataSplitter
from DashboardAPI import DashboardAPI
from time import time, localtime, strftime

class CMSSW(Module):
	def __init__(self, config, opts, proxy):
		Module.__init__(self, config, opts, proxy)

		# SCRAM info
		scramProject = config.get('CMSSW', 'scram project', '').split()
		if len(scramProject):
			self.projectArea = config.getPath('CMSSW', 'project area', '')
			if len(self.projectArea):
				raise ConfigError('Cannot specify both SCRAM project and project area')
			if len(scramProject) != 2:
				raise ConfigError('SCRAM project needs exactly 2 arguments: PROJECT VERSION')
		else:
			self.projectArea = config.getPath('CMSSW', 'project area')

		self.configFile = config.getPath('CMSSW', 'config file')
		self.configFiles = [ self.configFile ]

		self.dataset = config.get('CMSSW', 'dataset', '').strip()
		if self.dataset == '':
			self.dataset = None
			self.eventsPerJob = config.getInt('CMSSW', 'events per job', 0)
		else:
			self.eventsPerJob = config.getInt('CMSSW', 'events per job')
			for tag in [ "__FILE_NAMES__", "__MAX_EVENTS__", "__SKIP_EVENTS__" ]:
				for cfgName in self.configFiles:
					if open(cfgName, 'r').read().find(tag) == -1:
						print open(utils.atRoot('share', 'fail.txt'), 'r').read()
						raise ConfigError("Config file must use __FILE_NAMES__, __MAX_EVENTS__" \
							" and __SKIP_EVENTS__ to work properly with datasets!")

		self.gzipOut = config.getBool('CMSSW', 'gzip output', True)
		self.useReqs = config.getBool('CMSSW', 'use requirements', True)
		self.seRuntime = config.getBool('CMSSW', 'se runtime', False)

		if self.seRuntime and len(self.projectArea):
			self.seInputFiles.append(self.taskID + ".tar.gz"),

		if len(self.projectArea):
			self.pattern = config.get('CMSSW', 'area files', '-.* -config lib module */data *.xml *.sql *.cf[if] *.py').split()

			if os.path.exists(self.projectArea):
				print "Project area found in: %s" % self.projectArea
			else:
				raise ConfigError("Specified config area '%s' does not exist!" % self.projectArea)

			scramPath = os.path.join(self.projectArea, '.SCRAM')
			# try to open it
			try:
				fp = open(os.path.join(scramPath, 'Environment'), 'r')
				self.scramEnv = utils.DictFormat().parse(fp, lowerCaseKey = False)
			except:
				raise ConfigError("Project area file .SCRAM/Environment cannot be parsed!")

			for key in ['SCRAM_PROJECTNAME', 'SCRAM_PROJECTVERSION']:
				if not self.scramEnv.has_key(key):
					raise ConfigError("Installed program in project area can't be recognized.")

			archs = filter(lambda x: os.path.isdir(os.path.join(scramPath, x)), os.listdir(scramPath))
			self.scramArch = config.get('CMSSW', 'scram arch', archs[0])
			try:
				fp = open(os.path.join(scramPath, self.scramArch, 'Environment'), 'r')
				self.scramEnv.update(utils.DictFormat().parse(fp, lowerCaseKey = False))
			except:
				print "Project area file .SCRAM/%s/Environment cannot be parsed!" % self.scramArch
		else:
			self.scramEnv = {
				'SCRAM_PROJECTNAME': scramProject[0],
				'SCRAM_PROJECTVERSION': scramProject[1]
			}
			self.scramArch = config.get('CMSSW', 'scram arch')

		self.scramVersion = config.get('CMSSW', 'scram version', 'scramv1')
		if self.scramEnv['SCRAM_PROJECTNAME'] != 'CMSSW':
			raise ConfigError("Project area not a valid CMSSW project area.")

		for cfgFile in self.configFiles:
			if not os.path.exists(cfgFile):
				raise ConfigError("Config file '%s' not found." % cfgFile)

		self.dataSplitter = None
		if opts.init:
			self._initTask(opts.workDir, config)
		elif self.dataset != None:
			try:
				self.dataSplitter = DataSplitter.loadState(opts.workDir)
			except:
				raise ConfigError("Not a properly initialized work directory '%s'." % opts.workDir)
			if opts.resync:
				old = DataProvider.loadState(config, opts.workDir)
				new = DataProvider.create(config)
				self.dataSplitter.resyncMapping(opts.workDir, old.getBlocks(), new.getBlocks())
				#TODO: new.saveState(opts.workDir)


	def _initTask(self, workDir, config):
		if len(self.projectArea):
			utils.genTarball(os.path.join(workDir, 'runtime.tar.gz'), self.projectArea, self.pattern)

			if self.seRuntime:
				print 'Copy CMSSW runtime to SE',
				sys.stdout.flush()
				source = 'file:///' + os.path.join(workDir, 'runtime.tar.gz')
				target = os.path.join(self.sePath, self.taskID + '.tar.gz')
				if utils.se_copy(source, target, config.getBool('CMSSW', 'se runtime force', True)):
					print 'finished'
				else:
					print 'failed'
					raise RuntimeError("Unable to copy runtime!")

		# find and split datasets
		if self.dataset != None:
			self.dataprovider = DataProvider.create(config)
			self.dataprovider.saveState(workDir)
			if utils.verbosity() > 2:
				self.dataprovider.printDataset()

			splitter = config.get('CMSSW', 'dataset splitter', 'DefaultSplitter')
			self.dataSplitter = DataSplitter.open(splitter, { "eventsPerJob": self.eventsPerJob })
			self.dataSplitter.splitDataset(self.dataprovider.getBlocks())
			self.dataSplitter.saveState(workDir)
			if utils.verbosity() > 2:
				self.dataSplitter.printAllJobInfo()


	# Called on job submission
	def onJobSubmit(self, job, id, dbmessage = [{}]):
		splitInfo = {}
		if self.dataSplitter:
			splitInfo = self.dataSplitter.getSplitInfo(id)
		Module.onJobSubmit(self, job, id, [{
			"application": self.scramEnv['SCRAM_PROJECTVERSION'], "exe": "cmsRun",
			"nevtJob": splitInfo.get(DataSplitter.NEvents, self.eventsPerJob),
			"datasetFull": splitInfo.get(DataSplitter.Dataset, '') }])


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		data = Module.getTaskConfig(self)
		data['CMSSW_CONFIG'] = str.join(' ', map(os.path.basename, self.configFiles))
		data['CMSSW_OLD_RELEASETOP'] = self.scramEnv.get('RELEASETOP', None)
		data['DB_EXEC'] = 'cmsRun'
		data['SCRAM_VERSION'] = self.scramVersion
		data['SCRAM_ARCH'] = self.scramArch
		data['SCRAM_PROJECTVERSION'] = self.scramEnv['SCRAM_PROJECTVERSION']
		data['GZIP_OUT'] = ('no', 'yes')[self.gzipOut]
		data['SE_RUNTIME'] = ('no', 'yes')[self.seRuntime]
		data['HAS_RUNTIME'] = ('no', 'yes')[len(self.projectArea) != 0]
		return data


	# Get job dependent environment variables
	def getJobConfig(self, job):
		data = Module.getJobConfig(self, job)
		if not self.dataSplitter:
			return data

		splitInfo = self.dataSplitter.getSplitInfo(job)
		data['DATASETID'] = splitInfo.get(DataSplitter.DatasetID, None)
		data['DATASETPATH'] = splitInfo.get(DataSplitter.Dataset, None)
		data['DATASETNICK'] = splitInfo.get(DataSplitter.Nickname, None)
		return data


	def getVarMapping(self):
		tmp = ['MAX_EVENTS', 'SKIP_EVENTS', 'FILE_NAMES']
		return dict(Module.getVarMapping(self).items() + zip(tmp, tmp) + [('NICK', 'DATASETNICK')])


	# Get job requirements
	def getRequirements(self, job):
		reqs = Module.getRequirements(self, job)
		if self.useReqs:
			reqs.append((WMS.MEMBER, 'VO-cms-%s' % self.scramEnv['SCRAM_PROJECTVERSION']))
			reqs.append((WMS.MEMBER, 'VO-cms-%s' % self.scramArch))
		if self.dataSplitter != None:
			reqs.append((WMS.STORAGE, self.dataSplitter.getSitesForJob(job)))
		return reqs


	# Get files for input sandbox
	def getInFiles(self):
		files = Module.getInFiles(self)
		if len(self.projectArea) and not self.seRuntime:
			files.append('runtime.tar.gz')
		files.append(utils.atRoot('share', 'run.cmssw.sh')),
		files.extend(self.configFiles)
		return files


	# Get files for output sandbox
	def getOutFiles(self):
		files = Module.getOutFiles(self)[:]
		# Add framework report file
		renameExt = lambda name: str.join('.', name.split('.')[:-1]) + '.xml.gz'
		files.extend(map(renameExt, map(os.path.basename, self.configFiles)))
		if self.gzipOut:
			files.append('cmssw_out.txt.gz')
		return files


	def getCommand(self):
		return './run.cmssw.sh "$@"'


	def getJobArguments(self, job):
		if self.dataSplitter == None:
			return str(self.eventsPerJob)

		splitInfo = self.dataSplitter.getSplitInfo(job)
		if utils.verbosity() > 0:
			print "Job number: %d" % job
			DataSplitter.printInfoForJob(splitInfo)
		return "%d %d %s" % (
			splitInfo[DataSplitter.NEvents],
			splitInfo[DataSplitter.Skipped],
			str.join(' ', splitInfo[DataSplitter.FileList])
		)


	def getMaxJobs(self):
		if self.dataSplitter == None:
			raise ConfigError('Must specifiy number of jobs or dataset!')
		return self.dataSplitter.getNumberOfJobs()
