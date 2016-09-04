# | Copyright 2007-2016 Karlsruhe Institute of Technology
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

import os, logging
from grid_control import utils
from grid_control.backends import WMS
from grid_control.config import ConfigError, noDefault
from grid_control.datasets import DataSplitter, PartitionProcessor
from grid_control.output_processor import DebugJobInfoProcessor
from grid_control.tasks.task_data import DataTask
from grid_control.tasks.task_utils import TaskExecutableWrapper
from python_compat import ifilter, imap, lmap, sorted

class CMSSWDebugJobInfoProcessor(DebugJobInfoProcessor):
	def __init__(self):
		DebugJobInfoProcessor.__init__(self)
		self._display_files.append('cmssw.log.gz')


class LFNPartitionProcessor(PartitionProcessor):
	alias = ['lfnprefix']

	def __init__(self, config):
		PartitionProcessor.__init__(self, config)
		lfnModifier = config.get('partition lfn modifier', '', onChange = None)
		lfnModifierShortcuts = config.getDict('partition lfn modifier dict', {
			'<xrootd>': 'root://cms-xrd-global.cern.ch/',
			'<xrootd:eu>': 'root://xrootd-cms.infn.it/',
			'<xrootd:us>': 'root://cmsxrootd.fnal.gov/',
		}, onChange = None)[0]
		self._prefix = None
		if lfnModifier == '/':
			self._prefix = '/store/'
		elif lfnModifier.lower() in lfnModifierShortcuts:
			self._prefix = lfnModifierShortcuts[lfnModifier.lower()] + '/store/'
		elif lfnModifier:
			self._prefix = lfnModifier + '/store/'

	def enabled(self):
		return self._prefix is not None

	def process(self, pNum, splitInfo, result):
		def prefixLFN(lfn):
			return self._prefix + lfn.split('/store/', 1)[-1]
		if self._prefix:
			splitInfo[DataSplitter.FileList] = lmap(prefixLFN, splitInfo[DataSplitter.FileList])


class CMSSWPartitionProcessor(PartitionProcessor.getClass('BasicPartitionProcessor')):
	alias = ['cmsswpart']

	def _formatFileList(self, fl):
		return str.join(', ', imap(lambda x: '"%s"' % x, fl))


class SCRAMTask(DataTask):
	configSections = DataTask.configSections + ['SCRAMTask']

	def __init__(self, config, name):
		DataTask.__init__(self, config, name)

		# SCRAM settings
		scramArchDefault = noDefault
		scramProject = config.getList('scram project', [])
		if scramProject: # manual scram setup
			if len(scramProject) != 2:
				raise ConfigError('%r needs exactly 2 arguments: <PROJECT> <VERSION>' % 'scram project')
			self._projectArea = None
			self._projectAreaPattern = None
			self._scramProject = scramProject[0]
			self._scramProjectVersion = scramProject[1]
			# ensure project area is not used
			if 'project area' in config.getOptions():
				raise ConfigError('Cannot specify both %r and %r' % ('scram project', 'project area'))

		else: # scram setup used from project area
			self._projectArea = config.getPath('project area')
			self._projectAreaPattern = config.getList('area files', ['-.*', '-config', 'bin', 'lib', 'python', 'module',
				'*/data', '*.xml', '*.sql', '*.db', '*.cf[if]', '*.py', '-*/.git', '-*/.svn', '-*/CVS', '-*/work.*'])
			logging.getLogger('user').info('Project area found in: %s', self._projectArea)

			# try to determine scram settings from environment settings
			scramPath = os.path.join(self._projectArea, '.SCRAM')
			scramEnv = self._parse_scram_file(os.path.join(scramPath, 'Environment'))
			try:
				self._scramProject = scramEnv['SCRAM_PROJECTNAME']
				self._scramProjectVersion = scramEnv['SCRAM_PROJECTVERSION']
			except:
				raise ConfigError('Installed program in project area not recognized.')

			for arch_dir in sorted(ifilter(lambda dn: os.path.isdir(os.path.join(scramPath, dn)), os.listdir(scramPath))):
				scramArchDefault = arch_dir

		self._scramVersion = config.get('scram version', 'scramv1')
		self._scramArch = config.get('scram arch', scramArchDefault)

		self._scramReqs = []
		if config.getBool('scram arch requirements', True, onChange = None):
			self._scramReqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self._scramArch))
		if config.getBool('scram project requirements', False, onChange = None):
			self._scramReqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self._scramProject))
		if config.getBool('scram project version requirements', False, onChange = None):
			self._scramReqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self._scramProjectVersion))


	# Get job requirements
	def getRequirements(self, jobNum):
		return DataTask.getRequirements(self, jobNum) + self._scramReqs


	def getTaskConfig(self):
		data = DataTask.getTaskConfig(self)
		data['SCRAM_VERSION'] = self._scramVersion
		data['SCRAM_ARCH'] = self._scramArch
		data['SCRAM_PROJECTNAME'] = self._scramProject
		data['SCRAM_PROJECTVERSION'] = self._scramProjectVersion
		return data


	def getDependencies(self):
		return DataTask.getDependencies(self) + ['cmssw']


	def _parse_scram_file(self, fn):
		try:
			fp = open(fn, 'r')
			try:
				return utils.DictFormat().parse(fp, keyParser = {None: str})
			finally:
				fp.close()
		except Exception:
			raise ConfigError('Project area file %s cannot be parsed!' % fn)


class CMSSW(SCRAMTask):
	configSections = SCRAMTask.configSections + ['CMSSW']

	def __init__(self, config, name):
		config.set('se input timeout', '0:30')
		config.set('dataset provider', 'DBS3Provider')
		config.set('dataset splitter', 'EventBoundarySplitter')
		config.set('dataset processor', 'LumiDataProcessor', '+=')
		config.set('partition processor', 'TFCPartitionProcessor LocationPartitionProcessor MetaPartitionProcessor ' +
			'LFNPartitionProcessor LumiPartitionProcessor CMSSWPartitionProcessor')
		dash_config = config.changeView(viewClass = 'SimpleConfigView', setSections = ['dashboard'])
		dash_config.set('application', 'cmsRun')
		SCRAMTask.__init__(self, config, name)
		if self._scramProject != 'CMSSW':
			raise ConfigError('Project area contains no CMSSW project')

		self._oldReleaseTop = None
		if self._projectArea:
			self._oldReleaseTop = self._parse_scram_file(os.path.join(self._projectArea, '.SCRAM', self._scramArch, 'Environment')).get('RELEASETOP', None)

		self.updateErrorDict(utils.pathShare('gc-run.cmssw.sh', pkg = 'grid_control_cms'))

		self._projectAreaTarballSE = config.getBool(['se runtime', 'se project area'], True)
		self._projectAreaTarball = config.getWorkPath('cmssw-project-area.tar.gz')

		# Prolog / Epilog script support - warn about old syntax
		self.prolog = TaskExecutableWrapper(config, 'prolog', '')
		self.epilog = TaskExecutableWrapper(config, 'epilog', '')
		if config.getPaths('executable', []) != []:
			raise ConfigError('Prefix executable and argument options with either prolog or epilog!')
		self.arguments = config.get('arguments', '')

		# Get cmssw config files and check their existance
		# Check that for dataset jobs the necessary placeholders are in the config file
		if self._dataSplitter is None:
			self.eventsPerJob = config.get('events per job', '0') # this can be a variable like @USER_EVENTS@!
		fragment = config.getPath('instrumentation fragment', utils.pathShare('fragmentForCMSSW.py', pkg = 'grid_control_cms'))
		self.configFiles = self._processConfigFiles(config, list(self._getConfigFiles(config)), fragment,
			autoPrepare = config.getBool('instrumentation', True),
			mustPrepare = (self._dataSplitter is not None))

		# Create project area tarball
		if self._projectArea and not os.path.exists(self._projectAreaTarball):
			config.setState(True, 'init', detail = 'sandbox')
		# Information about search order for software environment
		self.searchLoc = self._getCMSSWPaths(config)
		if config.getState('init', detail = 'sandbox'):
			if os.path.exists(self._projectAreaTarball):
				if not utils.getUserBool('CMSSW tarball already exists! Do you want to regenerate it?', True):
					return
			# Generate CMSSW tarball
			if self._projectArea:
				utils.genTarball(self._projectAreaTarball, utils.matchFiles(self._projectArea, self._projectAreaPattern))
			if self._projectAreaTarballSE:
				config.setState(True, 'init', detail = 'storage')


	def _getCMSSWPaths(self, config):
		result = []
		userPath = config.get(['cmssw dir', 'vo software dir'], '')
		if userPath:
			userPathLocal = os.path.abspath(utils.cleanPath(userPath))
			if os.path.exists(userPathLocal):
				userPath = userPathLocal
		if userPath:
			result.append(('CMSSW_DIR_USER', userPath))
		if self._oldReleaseTop:
			projPath = os.path.normpath('%s/../../../../' % self._oldReleaseTop)
			result.append(('CMSSW_DIR_PRO', projPath))
		log = logging.getLogger('user')
		log.info('Local jobs will try to use the CMSSW software located here:')
		for i, loc in enumerate(result):
			log.info(' %i) %s', i + 1, loc[1])
		if result:
			log.info('')
		return result


	def _getConfigFiles(self, config):
		cfgDefault = utils.QM(self.prolog.isActive() or self.epilog.isActive(), [], noDefault)
		for cfgFile in config.getPaths('config file', cfgDefault, mustExist = False):
			if not os.path.exists(cfgFile):
				raise ConfigError('Config file %r not found.' % cfgFile)
			yield cfgFile


	def _cfgIsInstrumented(self, fn):
		fp = open(fn, 'r')
		try:
			cfg = fp.read()
		finally:
			fp.close()
		for tag in self.neededVars():
			if (not '__%s__' % tag in cfg) and (not '@%s@' % tag in cfg):
				return False
		return True


	def _cfgStore(self, source, target, fragment_path = None):
		fp = open(source, 'r')
		try:
			content = fp.read()
		finally:
			fp.close()
		fp = open(target, 'w')
		try:
			fp.write(content)
			if fragment_path:
				logging.getLogger('user').info('Instrumenting... %s', os.path.basename(source))
				fragment_fp = open(fragment_path, 'r')
				fp.write(fragment_fp.read())
				fragment_fp.close()
		finally:
			fp.close()


	def _cfgFindUninitialized(self, config, cfgFiles, autoPrepare, mustPrepare):
		comPath = os.path.dirname(os.path.commonprefix(cfgFiles))

		cfgTodo = []
		cfgStatus = []
		for cfg in cfgFiles:
			cfg_new = config.getWorkPath(os.path.basename(cfg))
			cfg_new_exists = os.path.exists(cfg_new)
			if cfg_new_exists:
				isInstrumented = self._cfgIsInstrumented(cfg_new)
				doCopy = False
			else:
				isInstrumented = self._cfgIsInstrumented(cfg)
				doCopy = True
			doPrepare = (mustPrepare or autoPrepare) and not isInstrumented
			doCopy = doCopy or doPrepare
			if doCopy:
				cfgTodo.append((cfg, cfg_new, doPrepare))
			cfgStatus.append({1: cfg.split(comPath, 1)[1].lstrip('/'), 2: cfg_new_exists,
				3: isInstrumented, 4: doPrepare})

		if cfgStatus:
			utils.printTabular([(1, 'Config file'), (2, 'Work dir'), (3, 'Instrumented'), (4, 'Scheduled')], cfgStatus, 'lccc')
		return cfgTodo


	def _processConfigFiles(self, config, cfgFiles, fragment_path, autoPrepare, mustPrepare):
		# process list of uninitialized config files
		for (cfg, cfg_new, doPrepare) in self._cfgFindUninitialized(config, cfgFiles, autoPrepare, mustPrepare):
			if doPrepare and (autoPrepare or utils.getUserBool('Do you want to prepare %s for running over the dataset?' % cfg, True)):
				self._cfgStore(cfg, cfg_new, fragment_path)
			else:
				self._cfgStore(cfg, cfg_new)

		result = []
		for cfg in cfgFiles:
			cfg_new = config.getWorkPath(os.path.basename(cfg))
			if not os.path.exists(cfg_new):
				raise ConfigError('Config file %r was not copied to the work directory!' % cfg)
			isInstrumented = self._cfgIsInstrumented(cfg_new)
			if mustPrepare and not isInstrumented:
				raise ConfigError('Config file %r must use %s to work properly!' %
					(cfg, str.join(', ', imap(lambda x: '@%s@' % x, self.neededVars()))))
			if autoPrepare and not isInstrumented:
				self._log.warning('Config file %r was not instrumented!', cfg)
			result.append(cfg_new)
		return result


	def neededVars(self):
		if self._dataSplitter:
			return self._partProcessor.getNeededKeys(self._dataSplitter) or []
		return ['MAX_EVENTS']


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		data = SCRAMTask.getTaskConfig(self)
		data.update(dict(self.searchLoc))
		data['GZIP_OUT'] = utils.QM(self.gzipOut, 'yes', 'no')
		data['SE_RUNTIME'] = utils.QM(self._projectAreaTarballSE, 'yes', 'no')
		data['HAS_RUNTIME'] = utils.QM(self._projectArea, 'yes', 'no')
		data['CMSSW_EXEC'] = 'cmsRun'
		data['CMSSW_CONFIG'] = str.join(' ', imap(os.path.basename, self.configFiles))
		data['CMSSW_OLD_RELEASETOP'] = self._oldReleaseTop
		if self.prolog.isActive():
			data['CMSSW_PROLOG_EXEC'] = self.prolog.getCommand()
			data['CMSSW_PROLOG_SB_IN_FILES'] = str.join(' ', imap(lambda x: x.pathRel, self.prolog.getSBInFiles()))
			data['CMSSW_PROLOG_ARGS'] = self.prolog.getArguments()
		if self.epilog.isActive():
			data['CMSSW_EPILOG_EXEC'] = self.epilog.getCommand()
			data['CMSSW_EPILOG_SB_IN_FILES'] = str.join(' ', imap(lambda x: x.pathRel, self.epilog.getSBInFiles()))
			data['CMSSW_EPILOG_ARGS'] = self.epilog.getArguments()
		return data


	# Get files to be transfered via SE (description, source, target)
	def getSEInFiles(self):
		files = SCRAMTask.getSEInFiles(self)
		if self._projectArea and self._projectAreaTarballSE:
			return files + [('CMSSW tarball', self._projectAreaTarball, self.taskID + '.tar.gz')]
		return files


	# Get files for input sandbox
	def getSBInFiles(self):
		files = SCRAMTask.getSBInFiles(self) + self.prolog.getSBInFiles() + self.epilog.getSBInFiles()
		for cfgFile in self.configFiles:
			files.append(utils.Result(pathAbs = cfgFile, pathRel = os.path.basename(cfgFile)))
		if self._projectArea and not self._projectAreaTarballSE:
			files.append(utils.Result(pathAbs = self._projectAreaTarball, pathRel = os.path.basename(self._projectAreaTarball)))
		return files + [utils.Result(pathAbs = utils.pathShare('gc-run.cmssw.sh', pkg = 'grid_control_cms'), pathRel = 'gc-run.cmssw.sh')]


	# Get files for output sandbox
	def getSBOutFiles(self):
		if not self.configFiles:
			return SCRAMTask.getSBOutFiles(self)
		return SCRAMTask.getSBOutFiles(self) + utils.QM(self.gzipOut, ['cmssw.log.gz'], []) + ['cmssw.dbs.tar.gz']


	def getCommand(self):
		return './gc-run.cmssw.sh $@'


	def getJobArguments(self, jobNum):
		return SCRAMTask.getJobArguments(self, jobNum) + ' ' + self.arguments


	def getVarNames(self):
		result = SCRAMTask.getVarNames(self)
		if self._dataSplitter is None:
			result.append('MAX_EVENTS')
		return result


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		data = SCRAMTask.getJobConfig(self, jobNum)
		if self._dataSplitter is None:
			data['MAX_EVENTS'] = self.eventsPerJob
		return data


	def getDescription(self, jobNum): # (task name, job name, type)
		result = SCRAMTask.getDescription(self, jobNum)
		if not result.jobType:
			result.jobType = 'analysis'
		return result
