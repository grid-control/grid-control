import os, sys, shutil
from python_compat import *
from grid_control import QM, ConfigError, WMS, utils, storage, datasets, noDefault
from grid_control.tasks.task_data import DataTask
from grid_control.datasets import DataSplitter
from grid_control.parameters import DataParameterSource, DataSplitProcessor
from lumi_tools import *


class CMSDataSplitProcessor(DataSplitProcessor):
	def formatFileList(self, fl):
		return str.join(', ', map(lambda x: '"%s"' % x, fl))


class ExecutableWrapper:
	def __init__(self, config, section, prefix = '', varPrefix = 'GC', exeDefault = noDefault):
		(self.prefix, self.varPrefix) = (prefix, varPrefix)
		self.sendexec = config.getBool(section, '%s send executable' % prefix, True)
		if self.sendexec:
			self.executable = config.getPath(section, '%s executable' % prefix, exeDefault)
		else:
			self.executable = config.get(section, '%s executable' % prefix, exeDefault, noVar = False)
		self.arguments = config.get(section, '%s arguments' % prefix, '', noVar = False)

	def isActive(self):
		return self.executable

	def getTaskConfig(self):
		return { "%s_ARGS" % self.varPrefix: self.arguments,
			"%s_EXEC" % self.varPrefix: os.path.basename(self.executable) }

	def getSBInFiles(self):
		return QM(self.sendexec and self.executable, [self.executable], [])


class CMSSW(DataTask):
	def __init__(self, config):
		config.set('storage', 'se input timeout', '0:30', override = False)
		config.set(self.__class__.__name__, 'dataset provider', 'DBSApiv2', override = False)
		config.set(self.__class__.__name__, 'dataset splitter', 'EventBoundarySplitter', override = False)
		DataMod.__init__(self, config)
		self.errorDict.update(dict(self.updateErrorDict(utils.pathShare('gc-run.cmssw.sh', pkg = 'grid_control_cms'))))

		# SCRAM info
		scramProject = config.getList(self.__class__.__name__, 'scram project', [])
		if len(scramProject):
			self.projectArea = config.getPath(self.__class__.__name__, 'project area', '')
			if len(self.projectArea):
				raise ConfigError('Cannot specify both SCRAM project and project area')
			if len(scramProject) != 2:
				raise ConfigError('SCRAM project needs exactly 2 arguments: PROJECT VERSION')
		else:
			self.projectArea = config.getPath(self.__class__.__name__, 'project area')

		# This works in tandem with provider_dbsv2.py !
		self.selectedLumis = parseLumiFilter(config.get(self.__class__.__name__, 'lumi filter', ''))

		self.useReqs = config.getBool(self.__class__.__name__, 'software requirements', True, mutable = True)
		self.seRuntime = config.getBool(self.__class__.__name__, 'se runtime', False)

		if len(self.projectArea):
			defaultPattern = '-.* -config bin lib python module */data *.xml *.sql *.cf[if] *.py -*/.git -*/.svn -*/CVS -*/work.*'
			self.pattern = config.getList(self.__class__.__name__, 'area files', defaultPattern.split())

			if os.path.exists(self.projectArea):
				utils.vprint('Project area found in: %s' % self.projectArea, -1)
			else:
				raise ConfigError('Specified config area %r does not exist!' % self.projectArea)

			scramPath = os.path.join(self.projectArea, '.SCRAM')
			# try to open it
			try:
				fp = open(os.path.join(scramPath, 'Environment'), 'r')
				self.scramEnv = utils.DictFormat().parse(fp, keyParser = {None: str})
			except:
				raise ConfigError('Project area file %s/.SCRAM/Environment cannot be parsed!' % self.projectArea)

			for key in ['SCRAM_PROJECTNAME', 'SCRAM_PROJECTVERSION']:
				if key not in self.scramEnv:
					raise ConfigError('Installed program in project area not recognized.')

			archs = filter(lambda x: os.path.isdir(os.path.join(scramPath, x)) and not x.startswith('.'), os.listdir(scramPath))
			self.scramArch = config.get(self.__class__.__name__, 'scram arch', (archs + [None])[0])
			try:
				fp = open(os.path.join(scramPath, self.scramArch, 'Environment'), 'r')
				self.scramEnv.update(utils.DictFormat().parse(fp, keyParser = {None: str}))
			except:
				raise ConfigError('Project area file .SCRAM/%s/Environment cannot be parsed!' % self.scramArch)
		else:
			self.scramEnv = {
				'SCRAM_PROJECTNAME': scramProject[0],
				'SCRAM_PROJECTVERSION': scramProject[1]
			}
			self.scramArch = config.get(self.__class__.__name__, 'scram arch')

		self.scramVersion = config.get(self.__class__.__name__, 'scram version', 'scramv1')
		if self.scramEnv['SCRAM_PROJECTNAME'] != 'CMSSW':
			raise ConfigError('Project area not a valid CMSSW project area.')

		# Information about search order for software environment
		self.searchLoc = []
		if config.opts.init:
			userPath = config.get(self.__class__.__name__, 'cmssw dir', '')
			if userPath != '':
				self.searchLoc.append(('CMSSW_DIR_USER', userPath))
			if self.scramEnv.get('RELEASETOP', None):
				projPath = os.path.normpath('%s/../../../../' % self.scramEnv['RELEASETOP'])
				self.searchLoc.append(('CMSSW_DIR_PRO', projPath))
		if len(self.searchLoc):
			utils.vprint('Local jobs will try to use the CMSSW software located here:', -1)
			for i, loc in enumerate(self.searchLoc):
				key, value = loc
				utils.vprint(' %i) %s' % (i + 1, value), -1)

		# Prolog / Epilog script support - warn about old syntax
		self.prolog = ExecutableWrapper(config, self.__class__.__name__, 'prolog', 'CMSSW_PROLOG', '')
		self.epilog = ExecutableWrapper(config, self.__class__.__name__, 'epilog', 'CMSSW_EPILOG', '')
		if config.getPaths(self.__class__.__name__, 'executable', []) != []:
			raise ConfigError('Prefix executable and argument options with either prolog or epilog!')
		self.arguments = config.get(self.__class__.__name__, 'arguments', '', noVar = False)

		# Get cmssw config files and check their existance
		self.configFiles = []
		cfgDefault = QM(self.prolog.isActive() or self.epilog.isActive(), [], noDefault)
		for cfgFile in config.getPaths(self.__class__.__name__, 'config file', cfgDefault, check = False):
			newPath = os.path.join(config.workDir, os.path.basename(cfgFile))
			if config.opts.init:
				if not os.path.exists(cfgFile):
					raise ConfigError('Config file %r not found.' % cfgFile)
				shutil.copyfile(cfgFile, newPath)
			self.configFiles.append(newPath)

		# Check that for dataset jobs the necessary placeholders are in the config file
		self.prepare = config.getBool(self.__class__.__name__, 'prepare config', False)
		fragment = config.getPath(self.__class__.__name__, 'instrumentation fragment',
			os.path.join('packages', 'grid_control_cms', 'share', 'fragmentForCMSSW.py'))
		if self.dataSplitter != None:
			if config.opts.init:
				if len(self.configFiles) > 0:
					self.instrumentCfgQueue(self.configFiles, fragment, mustPrepare = True)
		else:
			self.eventsPerJob = config.get(self.__class__.__name__, 'events per job', 0, noVar = False)
			if config.opts.init and self.prepare:
				self.instrumentCfgQueue(self.configFiles, fragment)

		if config.opts.init and len(self.projectArea):
			if os.path.exists(os.path.join(config.workDir, 'runtime.tar.gz')):
				if not utils.getUserBool('Runtime already exists! Do you want to regenerate CMSSW tarball?', True):
					return
			# Generate runtime tarball (and move to SE)
			utils.genTarball(os.path.join(config.workDir, 'runtime.tar.gz'), utils.matchFiles(self.projectArea, self.pattern))


	def initDataProcessor(self):
		return CMSDataSplitProcessor(self.checkSE)


	def instrumentCfgQueue(self, cfgFiles, fragment, mustPrepare = False):
		def isInstrumented(cfgName):
			cfg = open(cfgName, 'r').read()
			for tag in self.neededVars():
				if (not '__%s__' % tag in cfg) and (not '@%s@' % tag in cfg):
					return False
			return True
		def doInstrument(cfgName):
			if not isInstrumented(cfgName) or 'customise_for_gc' not in open(cfgName, 'r').read():
				utils.vprint('Instrumenting...', os.path.basename(cfgName), -1)
				open(cfgName, 'a').write(open(fragment, 'r').read())
			else:
				utils.vprint('%s already contains customise_for_gc and all needed variables' % os.path.basename(cfgName), -1)

		cfgStatus = []
		comPath = os.path.dirname(os.path.commonprefix(cfgFiles))
		for cfg in cfgFiles:
			cfgStatus.append({0: cfg.split(comPath, 1)[1].lstrip('/'), 1: str(isInstrumented(cfg)), 2: cfg})
		utils.printTabular([(0, 'Config file'), (1, 'Instrumented')], cfgStatus, 'lc')

		for cfg in cfgFiles:
			if self.prepare or not isInstrumented(cfg):
				if self.prepare or utils.getUserBool('Do you want to prepare %s for running over the dataset?' % cfg, True):
					doInstrument(cfg)
		if mustPrepare and not (True in map(isInstrumented, cfgFiles)):
			raise ConfigError('A config file must use %s to work properly!' %
				str.join(', ', map(lambda x: '@%s@' % x, self.neededVars())))


	# Lumi filter need
	def neededVars(self):
		result = []
		varMap = {
			DataSplitter.NEntries: 'MAX_EVENTS',
			DataSplitter.Skipped: 'SKIP_EVENTS',
			DataSplitter.FileList: 'FILE_NAMES'
		}
		if self.dataSplitter:
			result.extend(map(lambda x: varMap[x], self.dataSplitter.neededVars()))
		if self.selectedLumis:
			result.append('LUMI_RANGE')
		return result


	# Called on job submission
	def getSubmitInfo(self, jobNum):
		result = DataMod.getSubmitInfo(self, jobNum)
		result.update({'application': self.scramEnv['SCRAM_PROJECTVERSION'], 'exe': 'cmsRun'})
		if self.dataSplitter == None:
			result.update({'nevtJob': self.eventsPerJob})
		return result


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		data = DataMod.getTaskConfig(self)
		data.update(dict(self.searchLoc))
		data['CMSSW_OLD_RELEASETOP'] = self.scramEnv.get('RELEASETOP', None)
		data['DB_EXEC'] = 'cmsRun'
		data['SCRAM_ARCH'] = self.scramArch
		data['SCRAM_VERSION'] = self.scramVersion
		data['SCRAM_PROJECTVERSION'] = self.scramEnv['SCRAM_PROJECTVERSION']
		data['GZIP_OUT'] = QM(self.gzipOut, 'yes', 'no')
		data['SE_RUNTIME'] = QM(self.seRuntime, 'yes', 'no')
		data['HAS_RUNTIME'] = QM(len(self.projectArea), 'yes', 'no')
		data['CMSSW_CONFIG'] = str.join(' ', map(os.path.basename, self.configFiles))
		return utils.mergeDicts([data, self.prolog.getTaskConfig(), self.epilog.getTaskConfig()])


	# Get job requirements
	def getRequirements(self, jobNum):
		reqs = DataMod.getRequirements(self, jobNum)
		if self.useReqs:
			reqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self.scramEnv['SCRAM_PROJECTVERSION']))
			reqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self.scramArch))
		return reqs


	# Get files to be transfered via SE (description, source, target)
	def getSEInFiles(self):
		files = DataMod.getSEInFiles(self)
		if len(self.projectArea) and self.seRuntime:
			return files + [('CMSSW runtime', os.path.join(self.config.workDir, 'runtime.tar.gz'), self.taskID + '.tar.gz')]
		return files


	# Get files for input sandbox
	def getSBInFiles(self):
		files = DataMod.getSBInFiles(self) + self.configFiles + self.prolog.getSBInFiles() + self.epilog.getSBInFiles()
		if len(self.projectArea) and not self.seRuntime:
			files.append(os.path.join(self.config.workDir, 'runtime.tar.gz'))
		return files + [utils.pathShare('gc-run.cmssw.sh', pkg = 'grid_control_cms')]


	# Get files for output sandbox
	def getSBOutFiles(self):
		return DataMod.getSBOutFiles(self) + QM(self.gzipOut, ['cmssw.log.gz'], []) + ['cmssw.dbs.tar.gz']


	def getCommand(self):
		return './gc-run.cmssw.sh $@'


	def getJobArguments(self, jobNum):
		return DataMod.getJobArguments(self, jobNum) + ' ' + self.arguments


	def getActiveLumiFilter(self, lumifilter, jobNum = None):
		getLR = lambda x: str.join(',', map(lambda x: '"%s"' % x, formatLumi(x)))
		return getLR(lumifilter) # TODO: Validate subset selection
		try:
			splitInfo = self.dataSplitter.getSplitInfo(jobNum)
			runTag = splitInfo[DataSplitter.MetadataHeader].index("Runs")
			runList = utils.listMapReduce(lambda m: m[runTag], splitInfo[DataSplitter.Metadata])
			return getLR(filterLumiFilter(runList, lumifilter))
		except:
			return getLR(lumifilter)


	def getVarNames(self):
		result = DataMod.getVarNames(self)
		if self.dataSplitter == None:
			result.append('MAX_EVENTS')
		if self.selectedLumis:
			result.append('LUMI_RANGE')
		return result


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		data = DataMod.getJobConfig(self, jobNum)
		if self.dataSplitter == None:
			data['MAX_EVENTS'] = self.eventsPerJob
		if self.selectedLumis:
			data['LUMI_RANGE'] = self.getActiveLumiFilter(self.selectedLumis)
		return data


	def getDescription(self, jobNum): # (task name, job name, type)
		(taskName, jobName, jobType) = DataMod.getDescription(self, jobNum)
		return (taskName, jobName, QM(jobType, jobType, QM(self.dataSplitter, 'analysis', 'production')))


	def getDependencies(self):
		return DataMod.getDependencies(self) + ['cmssw']
