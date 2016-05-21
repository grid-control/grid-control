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
from python_compat import imap, lfilter, lmap

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
	alias = ['cmssw']

	def _formatFileList(self, fl):
		return str.join(', ', imap(lambda x: '"%s"' % x, fl))


class SCRAMTask(DataTask):
	configSections = DataTask.configSections + ['SCRAMTask']

	def __init__(self, config, name):
		DataTask.__init__(self, config, name)


	def getDependencies(self):
		return DataTask.getDependencies(self) + ['cmssw']


class CMSSW(SCRAMTask):
	configSections = DataTask.configSections + ['CMSSW']

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
		self.updateErrorDict(utils.pathShare('gc-run.cmssw.sh', pkg = 'grid_control_cms'))

		# SCRAM settings
		self._configureSCRAMSettings(config)

		self._useReqs = config.getBool('software requirements', True, onChange = None)
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
		if self.dataSplitter is None:
			self.eventsPerJob = config.get('events per job', '0')
		fragment = config.getPath('instrumentation fragment', utils.pathShare('fragmentForCMSSW.py', pkg = 'grid_control_cms'))
		self.configFiles = self._processConfigFiles(config, list(self._getConfigFiles(config)), fragment,
			autoPrepare = config.getBool('instrumentation', True),
			mustPrepare = (self.dataSplitter is not None))

		# Create project area tarball
		if self.projectArea and not os.path.exists(self._projectAreaTarball):
			config.setState(True, 'init', detail = 'sandbox')
		# Information about search order for software environment
		self.searchLoc = self._getCMSSWPaths(config)
		if config.getState('init', detail = 'sandbox'):
			if os.path.exists(self._projectAreaTarball):
				if not utils.getUserBool('CMSSW tarball already exists! Do you want to regenerate it?', True):
					return
			# Generate CMSSW tarball
			if self.projectArea:
				utils.genTarball(self._projectAreaTarball, utils.matchFiles(self.projectArea, self.pattern))
			if self._projectAreaTarballSE:
				config.setState(True, 'init', detail = 'storage')


	def _configureSCRAMSettings(self, config):
		scramProject = config.getList('scram project', [])
		if len(scramProject):
			self.projectArea = config.getPath('project area', '')
			if len(self.projectArea):
				raise ConfigError('Cannot specify both SCRAM project and project area')
			if len(scramProject) != 2:
				raise ConfigError('SCRAM project needs exactly 2 arguments: PROJECT VERSION')
		else:
			self.projectArea = config.getPath('project area')

		if len(self.projectArea):
			self.pattern = config.getList('area files', ['-.*', '-config', 'bin', 'lib', 'python', 'module',
				'*/data', '*.xml', '*.sql', '*.db', '*.cf[if]', '*.py', '-*/.git', '-*/.svn', '-*/CVS', '-*/work.*'])

			if os.path.exists(self.projectArea):
				utils.vprint('Project area found in: %s' % self.projectArea, -1)
			else:
				raise ConfigError('Specified config area %r does not exist!' % self.projectArea)

			scramPath = os.path.join(self.projectArea, '.SCRAM')
			# try to open it
			try:
				fp = open(os.path.join(scramPath, 'Environment'), 'r')
				self.scramEnv = utils.DictFormat().parse(fp, keyParser = {None: str})
			except Exception:
				raise ConfigError('Project area file %s/.SCRAM/Environment cannot be parsed!' % self.projectArea)

			for key in ['SCRAM_PROJECTNAME', 'SCRAM_PROJECTVERSION']:
				if key not in self.scramEnv:
					raise ConfigError('Installed program in project area not recognized.')

			default_archs = lfilter(lambda x: os.path.isdir(os.path.join(scramPath, x)) and not x.startswith('.'), os.listdir(scramPath)) + [noDefault]
			default_arch = default_archs[0]
			self.scramArch = config.get('scram arch', default_arch)
			try:
				fp = open(os.path.join(scramPath, self.scramArch, 'Environment'), 'r')
				self.scramEnv.update(utils.DictFormat().parse(fp, keyParser = {None: str}))
			except Exception:
				raise ConfigError('Project area file .SCRAM/%s/Environment cannot be parsed!' % self.scramArch)
		else:
			self.scramEnv = {
				'SCRAM_PROJECTNAME': scramProject[0],
				'SCRAM_PROJECTVERSION': scramProject[1]
			}
			self.scramArch = config.get('scram arch')

		self.scramVersion = config.get('scram version', 'scramv1')
		if self.scramEnv['SCRAM_PROJECTNAME'] != 'CMSSW':
			raise ConfigError('Project area contains no CMSSW project')


	def _getCMSSWPaths(self, config):
		result = []
		userPath = config.get(['cmssw dir', 'vo software dir'], '')
		if userPath:
			userPathLocal = os.path.abspath(utils.cleanPath(userPath))
			if os.path.exists(userPathLocal):
				userPath = userPathLocal
		if userPath:
			result.append(('CMSSW_DIR_USER', userPath))
		if self.scramEnv.get('RELEASETOP', None):
			projPath = os.path.normpath('%s/../../../../' % self.scramEnv['RELEASETOP'])
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
		if self.dataSplitter:
			return self._dataPS.getNeededDataKeys()
		return ['MAX_EVENTS']


	# Called on job submission
	def getSubmitInfo(self, jobNum):
		result = DataTask.getSubmitInfo(self, jobNum)
		result.update({'application': self.scramEnv['SCRAM_PROJECTVERSION'], 'exe': 'cmsRun'})
		if self.dataSplitter is None:
			result.update({'nevtJob': self.eventsPerJob})
		return result


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		data = DataTask.getTaskConfig(self)
		data.update(dict(self.searchLoc))
		data['CMSSW_OLD_RELEASETOP'] = self.scramEnv.get('RELEASETOP', None)
		data['SCRAM_ARCH'] = self.scramArch
		data['SCRAM_VERSION'] = self.scramVersion
		data['SCRAM_PROJECTVERSION'] = self.scramEnv['SCRAM_PROJECTVERSION']
		data['GZIP_OUT'] = utils.QM(self.gzipOut, 'yes', 'no')
		data['SE_RUNTIME'] = utils.QM(self._projectAreaTarballSE, 'yes', 'no')
		data['HAS_RUNTIME'] = utils.QM(len(self.projectArea), 'yes', 'no')
		data['CMSSW_CONFIG'] = str.join(' ', imap(os.path.basename, self.configFiles))
		if self.prolog.isActive():
			data['CMSSW_PROLOG_EXEC'] = self.prolog.getCommand()
			data['CMSSW_PROLOG_SB_IN_FILES'] = str.join(' ', imap(lambda x: x.pathRel, self.prolog.getSBInFiles()))
			data['CMSSW_PROLOG_ARGS'] = self.prolog.getArguments()
		if self.epilog.isActive():
			data['CMSSW_EPILOG_EXEC'] = self.epilog.getCommand()
			data['CMSSW_EPILOG_SB_IN_FILES'] = str.join(' ', imap(lambda x: x.pathRel, self.epilog.getSBInFiles()))
			data['CMSSW_EPILOG_ARGS'] = self.epilog.getArguments()
		return data


	# Get job requirements
	def getRequirements(self, jobNum):
		reqs = DataTask.getRequirements(self, jobNum)
		if self._useReqs:
			reqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self.scramArch))
		return reqs


	# Get files to be transfered via SE (description, source, target)
	def getSEInFiles(self):
		files = DataTask.getSEInFiles(self)
		if len(self.projectArea) and self._projectAreaTarballSE:
			return files + [('CMSSW tarball', self._projectAreaTarball, self.taskID + '.tar.gz')]
		return files


	# Get files for input sandbox
	def getSBInFiles(self):
		files = DataTask.getSBInFiles(self) + self.prolog.getSBInFiles() + self.epilog.getSBInFiles()
		for cfgFile in self.configFiles:
			files.append(utils.Result(pathAbs = cfgFile, pathRel = os.path.basename(cfgFile)))
		if len(self.projectArea) and not self._projectAreaTarballSE:
			files.append(utils.Result(pathAbs = self._projectAreaTarball, pathRel = os.path.basename(self._projectAreaTarball)))
		return files + [utils.Result(pathAbs = utils.pathShare('gc-run.cmssw.sh', pkg = 'grid_control_cms'), pathRel = 'gc-run.cmssw.sh')]


	# Get files for output sandbox
	def getSBOutFiles(self):
		return DataTask.getSBOutFiles(self) + utils.QM(self.gzipOut, ['cmssw.log.gz'], []) + ['cmssw.dbs.tar.gz']


	def getCommand(self):
		return './gc-run.cmssw.sh $@'


	def getJobArguments(self, jobNum):
		return DataTask.getJobArguments(self, jobNum) + ' ' + self.arguments


	def getVarNames(self):
		result = DataTask.getVarNames(self)
		if self.dataSplitter is None:
			result.append('MAX_EVENTS')
		return result


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		data = DataTask.getJobConfig(self, jobNum)
		if self.dataSplitter is None:
			data['MAX_EVENTS'] = self.eventsPerJob
		return data


	def getDescription(self, jobNum): # (task name, job name, type)
		result = DataTask.getDescription(self, jobNum)
		if not result.jobType:
			result.jobType = 'analysis'
		return result
