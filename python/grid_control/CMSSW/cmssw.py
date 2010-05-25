import os, sys, shutil
from grid_control import ConfigError, WMS, utils, se_utils, datasets
from grid_control.datasets import DataMod
from lumi_tools import *

class CMSSW(DataMod):
	def __init__(self, config):
		DataMod.__init__(self, config)
		self.updateErrorDict(utils.pathGC('share', 'run.cmssw.sh'))

		# SCRAM info
		scramProject = config.get(self.__class__.__name__, 'scram project', '').split()
		if len(scramProject):
			self.projectArea = config.getPath(self.__class__.__name__, 'project area', '')
			if len(self.projectArea):
				raise ConfigError('Cannot specify both SCRAM project and project area')
			if len(scramProject) != 2:
				raise ConfigError('SCRAM project needs exactly 2 arguments: PROJECT VERSION')
		else:
			self.projectArea = config.getPath(self.__class__.__name__, 'project area')

		# Get cmssw config files and check their existance
		self.configFiles = []
		for cfgFile in config.getPaths(self.__class__.__name__, 'config file'):
			newPath = os.path.join(config.workDir, os.path.basename(cfgFile))
			if config.opts.init:
				if not os.path.exists(cfgFile):
					raise ConfigError("Config file '%s' not found." % cfgFile)
				shutil.copyfile(cfgFile, newPath)
			self.configFiles.append(newPath)

		self.selectedLumis = parseLumiFilter(config.get('CMSSW', 'lumi filter', ''))

		# Prepare (unprepared) cmssw config file for MC production / dataset analysis
		prepare = config.getBool(self.__class__.__name__, 'prepare config', False)
		def doInstrument(cfgName):
			if 'customise_for_gc' not in open(cfgName, 'r').read():
				print "Instrumenting...", os.path.basename(cfgName)
				fragment = utils.pathGC('scripts', 'fragmentForCMSSW.py')
				open(cfgName, 'a').write(open(fragment, 'r').read())

		# Check that for dataset jobs the necessary placeholders are in the config file
		if self.dataSplitter != None:
			def isInstrumented(cfgName):
				cfg = open(cfgName, 'r').read()
				for tag in self.neededVars():
					if (not "__%s__" % tag in cfg) and (not "@%s@" % tag in cfg):
						return False
				return True

			if not (True in map(isInstrumented, self.configFiles)):
				for cfgName in self.configFiles:
					if config.opts.init and not isInstrumented(cfgName):
						if prepare or utils.boolUserInput('Do you want to prepare %s for running over the dataset?' % cfgName, True):
							doInstrument(cfgName)

			if not (True in map(isInstrumented, self.configFiles)):
				raise ConfigError("A config file must use %s to work properly with dataset jobs!" %
					str.join(", ", map(lambda x: "__%s__" % x, self.neededVars())))
		else:
			self.eventsPerJob = config.get(self.__class__.__name__, 'events per job', 0)
			if config.opts.init and prepare:
				map(doInstrument, self.configFiles)

		self.useReqs = config.getBool(self.__class__.__name__, 'use requirements', True, volatile=True)
		self.seRuntime = config.getBool(self.__class__.__name__, 'se runtime', False)

		if self.seRuntime and len(self.projectArea):
			self.seInputFiles.append(self.taskID + ".tar.gz"),

		if len(self.projectArea):
			self.pattern = config.get(self.__class__.__name__, 'area files', '-.* -config lib python module */data *.xml *.sql *.cf[if] *.py').split()

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
				if key not in self.scramEnv:
					raise ConfigError("Installed program in project area can't be recognized.")

			archs = filter(lambda x: os.path.isdir(os.path.join(scramPath, x)), os.listdir(scramPath))
			try:
				self.scramArch = config.get(self.__class__.__name__, 'scram arch', archs[0])
			except:
				raise ConfigError("%s does not contain architecture information!" % scramPath)
			try:
				fp = open(os.path.join(scramPath, self.scramArch, 'Environment'), 'r')
				self.scramEnv.update(utils.DictFormat().parse(fp, lowerCaseKey = False))
			except:
				raise ConfigError("Project area file .SCRAM/%s/Environment cannot be parsed!" % self.scramArch)
		else:
			self.scramEnv = {
				'SCRAM_PROJECTNAME': scramProject[0],
				'SCRAM_PROJECTVERSION': scramProject[1]
			}
			self.scramArch = config.get(self.__class__.__name__, 'scram arch')

		self.scramVersion = config.get(self.__class__.__name__, 'scram version', 'scramv1')
		if self.scramEnv['SCRAM_PROJECTNAME'] != 'CMSSW':
			raise ConfigError("Project area not a valid CMSSW project area.")

		# Information about search order for software environment
		self.searchLoc = []
		if config.opts.init:
			userPath = config.get(self.__class__.__name__, 'cmssw dir', '')
			if userPath != '':
				self.searchLoc.append(('CMSSW_DIR_USER', userPath))
			if self.scramEnv.get('RELEASETOP', None):
				projPath = os.path.normpath("%s/../../../../" % self.scramEnv['RELEASETOP'])
				self.searchLoc.append(('CMSSW_DIR_PRO', projPath))
		if len(self.searchLoc) and config.get('global', 'backend', 'grid') != 'grid':
			print "Jobs will try to use the CMSSW software located here:"
			for i, loc in enumerate(self.searchLoc):
				key, value = loc
				print " %i) %s" % (i + 1, value)

		if config.opts.init and len(self.projectArea):
			# Generate runtime tarball (and move to SE)
			utils.genTarball(os.path.join(config.workDir, 'runtime.tar.gz'), self.projectArea, self.pattern)

			for idx, sePath in enumerate(filter(lambda x: self.seRuntime, self.sePaths)):
				print 'Copy CMSSW runtime to SE', idx,
				sys.stdout.flush()
				source = 'file:///' + os.path.join(config.workDir, 'runtime.tar.gz')
				target = os.path.join(sePath, self.taskID + '.tar.gz')
				proc = se_utils.se_copy(source, target, config.getBool(self.__class__.__name__, 'se runtime force', True))
				if proc.wait() == 0:
					print 'finished'
				else:
					print 'failed'
					print proc.getMessage()
					print "Unable to copy runtime! You can try to copy the CMSSW runtime manually."
					if not utils.boolUserInput('Is runtime available on SE?', False):
						raise RuntimeError("No CMSSW runtime on SE!")


	# Lumi filter need
	def neededVars(self):
		if self.selectedLumis:
			return DataMod.neededVars(self) + ["LUMI_RANGE"]
		return DataMod.neededVars(self)


	# Get default dataset modules
	def getDatasetDefaults(self, config):
		return ('DBSApiv2', 'EventBoundarySplitter')


	# Called on job submission
	def getSubmitInfo(self, jobNum):
		result = DataMod.getSubmitInfo(self, jobNum)
		result.update({"application": self.scramEnv['SCRAM_PROJECTVERSION'], "exe": "cmsRun"})
		if self.dataSplitter == None:
			result.update({"nevtJob": self.eventsPerJob})
		return result


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		data = DataMod.getTaskConfig(self)
		data.update(dict(self.searchLoc))
		data['CMSSW_CONFIG'] = str.join(' ', map(os.path.basename, self.configFiles))
		data['CMSSW_OLD_RELEASETOP'] = self.scramEnv.get('RELEASETOP', None)
		data['DB_EXEC'] = 'cmsRun'
		data['SCRAM_ARCH'] = self.scramArch
		data['SCRAM_VERSION'] = self.scramVersion
		data['SCRAM_PROJECTVERSION'] = self.scramEnv['SCRAM_PROJECTVERSION']
		data['GZIP_OUT'] = ('no', 'yes')[self.gzipOut]
		data['SE_RUNTIME'] = ('no', 'yes')[self.seRuntime]
		data['HAS_RUNTIME'] = ('no', 'yes')[len(self.projectArea) != 0]
		if self.selectedLumis:
			data['LUMI_RANGE'] = str.join(',', map(lambda x: '"%s"' % x, formatLumi(self.selectedLumis)))
		return data


	# Get job requirements
	def getRequirements(self, jobNum):
		reqs = DataMod.getRequirements(self, jobNum)
		if self.useReqs:
			reqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self.scramEnv['SCRAM_PROJECTVERSION']))
			reqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self.scramArch))
		return reqs


	# Get files for input sandbox
	def getInFiles(self):
		files = DataMod.getInFiles(self)
		if len(self.projectArea) and not self.seRuntime:
			files.append(os.path.join(self.config.workDir, 'runtime.tar.gz'))
		files.append(utils.pathGC('share', 'run.cmssw.sh')),
		files.extend(self.configFiles)
		return files


	# Get files for output sandbox
	def getOutFiles(self):
		files = DataMod.getOutFiles(self)
		if self.gzipOut:
			files.append('cmssw.log.gz')
		return files + ['cmssw.dbs.tar.gz']


	def getCommand(self):
		return './run.cmssw.sh $@'


	def formatFileList(self, filelist):
		return str.join(', ', map(lambda x: '"%s"' % x, filelist))


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		data = DataMod.getJobConfig(self, jobNum)
		if self.dataSplitter == None:
			data['MAX_EVENTS'] = self.eventsPerJob
		return data


	def getTaskType(self):
		if self.dataSplitter == None:
			return 'production'
		return 'analysis'


	def getDependencies(self):
		return DataMod.getDependencies(self) + ['cmssw']
