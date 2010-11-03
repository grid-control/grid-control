import itertools, shutil
from cmssw_base import *

class CMSSW(CMSSW_Base):
	def __init__(self, config):
		CMSSW_Base.__init__(self, config)

		# Get cmssw config files and check their existance
		self.configFiles = []
		for cfgFile in config.getPaths(self.__class__.__name__, 'config file'):
			newPath = os.path.join(config.workDir, os.path.basename(cfgFile))
			if config.opts.init:
				if not os.path.exists(cfgFile):
					raise ConfigError('Config file %r not found.' % cfgFile)
				shutil.copyfile(cfgFile, newPath)
			self.configFiles.append(newPath)

		# Check that for dataset jobs the necessary placeholders are in the config file
		self.prepare = config.getBool(self.__class__.__name__, 'prepare config', False)
		if self.dataSplitter != None:
			if config.opts.init:
				self.instrumentCfgQueue(self.configFiles, mustPrepare = True)
		else:
			self.eventsPerJob = config.get(self.__class__.__name__, 'events per job', 0)
			if config.opts.init and self.prepare:
				self.instrumentCfgQueue(self.configFiles)


	def instrumentCfgQueue(self, cfgFiles, mustPrepare = False):
		def isInstrumented(cfgName):
			cfg = open(cfgName, 'r').read()
			for tag in self.neededVars():
				if (not '__%s__' % tag in cfg) and (not '@%s@' % tag in cfg):
					return False
			return True
		def doInstrument(cfgName):
			if not isInstrumented(cfgName) or 'customise_for_gc' not in open(cfgName, 'r').read():
				utils.vprint('Instrumenting...', os.path.basename(cfgName), -1)
				fragment = utils.pathGC('scripts', 'fragmentForCMSSW.py')
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


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		tmp = {'CMSSW_CONFIG': str.join(' ', map(os.path.basename, self.configFiles))}
		return utils.mergeDicts([CMSSW_Base.getTaskConfig(self), tmp])


	# Get files for input sandbox
	def getInFiles(self):
		return CMSSW_Base.getInFiles(self) + self.configFiles


	# Get files for output sandbox
	def getOutFiles(self):
		return CMSSW_Base.getOutFiles(self) + ['cmssw.dbs.tar.gz']
