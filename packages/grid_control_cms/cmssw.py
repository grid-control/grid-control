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

import os
from grid_control import utils
from grid_control.backends import WMS
from grid_control.config import ConfigError
from grid_control.datasets import DataSplitter, PartitionProcessor
from grid_control.output_processor import DebugJobInfoProcessor
from grid_control.parameters import ParameterMetadata
from grid_control.tasks.task_data import DataTask
from grid_control.tasks.task_utils import TaskExecutableWrapper
from python_compat import ifilter, imap, lmap, set, sorted, unspecified


class CMSSWDebugJobInfoProcessor(DebugJobInfoProcessor):
	def __init__(self):
		DebugJobInfoProcessor.__init__(self)
		self._display_files.append('cmssw.log.gz')


class LFNPartitionProcessor(PartitionProcessor):
	alias_list = ['lfnprefix']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		lfnModifier = config.get(['partition lfn modifier', '%s partition lfn modifier' % datasource_name], '', on_change = None)
		lfnModifierShortcuts = config.get_dict(['partition lfn modifier dict', '%s partition lfn modifier dict' % datasource_name], {
			'<xrootd>': 'root://cms-xrd-global.cern.ch/',
			'<xrootd:eu>': 'root://xrootd-cms.infn.it/',
			'<xrootd:us>': 'root://cmsxrootd.fnal.gov/',
		}, on_change = None)[0]
		self._prefix = None
		if lfnModifier == '/':
			self._prefix = '/store/'
		elif lfnModifier.lower() in lfnModifierShortcuts:
			self._prefix = lfnModifierShortcuts[lfnModifier.lower()] + '/store/'
		elif lfnModifier:
			self._prefix = lfnModifier + '/store/'

	def enabled(self):
		return self._prefix is not None

	def get_partition_metadata(self):
		return lmap(lambda k: ParameterMetadata(k, untracked = True), ['DATASET_SRM_FILES'])

	def process(self, pNum, partition, result):
		def modify_filelist_for_srm(filelist):
			return lmap(lambda f: 'file://' + f.split('/')[-1], filelist)
		def prefix_lfn(lfn):
			return self._prefix + lfn.split('/store/', 1)[-1]
		if self._prefix:
			partition[DataSplitter.FileList] = lmap(prefix_lfn, partition[DataSplitter.FileList])
			if 'srm' in self._prefix:
				result.update({'DATASET_SRM_FILES': str.join(' ', partition[DataSplitter.FileList])})
				partition[DataSplitter.FileList] = modify_filelist_for_srm(partition[DataSplitter.FileList])


class CMSSWPartitionProcessor(PartitionProcessor.get_class('BasicPartitionProcessor')):
	alias_list = ['cmsswpart']

	def _format_file_list(self, fl):
		return str.join(', ', imap(lambda x: '"%s"' % x, fl))


class SCRAMTask(DataTask):
	config_section_list = DataTask.config_section_list + ['SCRAMTask']

	def __init__(self, config, name):
		DataTask.__init__(self, config, name)

		# SCRAM settings
		scramArchDefault = unspecified
		scramProject = config.get_list('scram project', [])
		if scramProject: # manual scram setup
			if len(scramProject) != 2:
				raise ConfigError('%r needs exactly 2 arguments: <PROJECT> <VERSION>' % 'scram project')
			self._projectArea = None
			self._projectAreaPattern = None
			self._scramProject = scramProject[0]
			self._scramProjectVersion = scramProject[1]
			# ensure project area is not used
			if 'project area' in config.get_option_list():
				raise ConfigError('Cannot specify both %r and %r' % ('scram project', 'project area'))

		else: # scram setup used from project area
			self._projectArea = config.get_path('project area')
			self._projectAreaPattern = config.get_list('area files', ['-.*', '-config', 'bin', 'lib', 'python', 'module',
				'*/data', '*.xml', '*.sql', '*.db', '*.cf[if]', '*.py', '-*/.git', '-*/.svn', '-*/CVS', '-*/work.*']) + ['*.pcm'] # FIXME
			self._log.info('Project area found in: %s', self._projectArea)

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
		if config.get_bool('scram arch requirements', True, on_change = None):
			self._scramReqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self._scramArch))
		if config.get_bool('scram project requirements', False, on_change = None):
			self._scramReqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self._scramProject))
		if config.get_bool('scram project version requirements', False, on_change = None):
			self._scramReqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self._scramProjectVersion))


	# Get job requirements
	def getRequirements(self, jobnum):
		return DataTask.getRequirements(self, jobnum) + self._scramReqs


	def get_task_dict(self):
		data = DataTask.get_task_dict(self)
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
				return utils.DictFormat().parse(fp, key_parser = {None: str})
			finally:
				fp.close()
		except Exception:
			raise ConfigError('Project area file %s cannot be parsed!' % fn)


class CMSSW(SCRAMTask):
	config_section_list = SCRAMTask.config_section_list + ['CMSSW']

	def __init__(self, config, name):
		config.set('se input timeout', '0:30')
		config.set('dataset provider', 'DBS3Provider')
		config.set('dataset splitter', 'EventBoundarySplitter')
		config.set('dataset processor', 'LumiDataProcessor', '+=')
		config.set('partition processor', 'TFCPartitionProcessor LocationPartitionProcessor MetaPartitionProcessor ' +
			'LFNPartitionProcessor LumiPartitionProcessor CMSSWPartitionProcessor')
		dash_config = config.change_view(view_class = 'SimpleConfigView', setSections = ['dashboard'])
		dash_config.set('application', 'cmsRun')

		self._neededVars = set()
		SCRAMTask.__init__(self, config, name)

		if self._scramProject != 'CMSSW':
			raise ConfigError('Project area contains no CMSSW project')

		self._oldReleaseTop = None
		if self._projectArea:
			self._oldReleaseTop = self._parse_scram_file(os.path.join(self._projectArea, '.SCRAM', self._scramArch, 'Environment')).get('RELEASETOP', None)

		self.updateErrorDict(utils.get_path_share('gc-run.cmssw.sh', pkg = 'grid_control_cms'))

		self._projectAreaTarballSE = config.get_bool(['se runtime', 'se project area'], True)
		self._projectAreaTarball = config.get_work_path('cmssw-project-area.tar.gz')

		# Prolog / Epilog script support - warn about old syntax
		self.prolog = TaskExecutableWrapper(config, 'prolog', '')
		self.epilog = TaskExecutableWrapper(config, 'epilog', '')
		if config.get_path_list('executable', []) != []:
			raise ConfigError('Prefix executable and argument options with either prolog or epilog!')
		self.arguments = config.get('arguments', '')

		# Get cmssw config files and check their existance
		# Check that for dataset jobs the necessary placeholders are in the config file
		if not self._has_dataset:
			self.eventsPerJob = config.get('events per job', '0') # this can be a variable like @USER_EVENTS@!
			self._neededVars.add('MAX_EVENTS')
		fragment = config.get_path('instrumentation fragment', utils.get_path_share('fragmentForCMSSW.py', pkg = 'grid_control_cms'))
		self._config_file_list = self._processConfigFiles(config, list(self._getConfigFiles(config)), fragment,
			autoPrepare = config.get_bool('instrumentation', True),
			mustPrepare = self._has_dataset)

		# Create project area tarball
		if self._projectArea and not os.path.exists(self._projectAreaTarball):
			config.set_state(True, 'init', detail = 'sandbox')
		# Information about search order for software environment
		self.searchLoc = self._getCMSSWPaths(config)
		if config.get_state('init', detail = 'sandbox'):
			if os.path.exists(self._projectAreaTarball):
				if not utils.get_user_bool('CMSSW tarball already exists! Do you want to regenerate it?', True):
					return
			# Generate CMSSW tarball
			if self._projectArea:
				utils.create_tarball(self._projectAreaTarball, utils.match_files(self._projectArea, self._projectAreaPattern))
			if self._projectAreaTarballSE:
				config.set_state(True, 'init', detail = 'storage')


	def _create_datasource(self, config, name, psrc_repository):
		psrc_data = SCRAMTask._create_datasource(self, config, name, psrc_repository)
		if psrc_data is not None:
			self._neededVars.update(psrc_data.get_needed_dataset_keys())
		return psrc_data


	def _getCMSSWPaths(self, config):
		result = []
		userPath = config.get(['cmssw dir', 'vo software dir'], '')
		if userPath:
			userPathLocal = os.path.abspath(utils.clean_path(userPath))
			if os.path.exists(userPathLocal):
				userPath = userPathLocal
		if userPath:
			result.append(('CMSSW_DIR_USER', userPath))
		if self._oldReleaseTop:
			projPath = os.path.normpath('%s/../../../../' % self._oldReleaseTop)
			result.append(('CMSSW_DIR_PRO', projPath))
		self._log.info('Local jobs will try to use the CMSSW software located here:')
		for i, loc in enumerate(result):
			self._log.info(' %i) %s', i + 1, loc[1])
		if result:
			self._log.info('')
		return result


	def _getConfigFiles(self, config):
		cfgDefault = utils.QM(self.prolog.is_active() or self.epilog.is_active(), [], unspecified)
		for cfgFile in config.get_path_list('config file', cfgDefault, must_exist = False):
			if not os.path.exists(cfgFile):
				raise ConfigError('Config file %r not found.' % cfgFile)
			yield cfgFile


	def _cfgIsInstrumented(self, fn):
		fp = open(fn, 'r')
		try:
			cfg = fp.read()
		finally:
			fp.close()
		for tag in self._neededVars:
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
				self._log.info('Instrumenting... %s', os.path.basename(source))
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
			cfg_new = config.get_work_path(os.path.basename(cfg))
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
			utils.display_table([(1, 'Config file'), (2, 'Work dir'), (3, 'Instrumented'), (4, 'Scheduled')], cfgStatus, 'lccc')
		return cfgTodo


	def _processConfigFiles(self, config, cfgFiles, fragment_path, autoPrepare, mustPrepare):
		# process list of uninitialized config files
		for (cfg, cfg_new, doPrepare) in self._cfgFindUninitialized(config, cfgFiles, autoPrepare, mustPrepare):
			if doPrepare and (autoPrepare or utils.get_user_bool('Do you want to prepare %s for running over the dataset?' % cfg, True)):
				self._cfgStore(cfg, cfg_new, fragment_path)
			else:
				self._cfgStore(cfg, cfg_new)

		result = []
		for cfg in cfgFiles:
			cfg_new = config.get_work_path(os.path.basename(cfg))
			if not os.path.exists(cfg_new):
				raise ConfigError('Config file %r was not copied to the work directory!' % cfg)
			isInstrumented = self._cfgIsInstrumented(cfg_new)
			if mustPrepare and not isInstrumented:
				raise ConfigError('Config file %r must use %s to work properly!' %
					(cfg, str.join(', ', imap(lambda x: '@%s@' % x, sorted(self._neededVars)))))
			if autoPrepare and not isInstrumented:
				self._log.warning('Config file %r was not instrumented!', cfg)
			result.append(cfg_new)
		return result


	# Get environment variables for gc_config.sh
	def get_task_dict(self):
		data = SCRAMTask.get_task_dict(self)
		data.update(dict(self.searchLoc))
		data['GZIP_OUT'] = utils.QM(self.gzipOut, 'yes', 'no')
		data['SE_RUNTIME'] = utils.QM(self._projectAreaTarballSE, 'yes', 'no')
		data['HAS_RUNTIME'] = utils.QM(self._projectArea, 'yes', 'no')
		data['CMSSW_EXEC'] = 'cmsRun'
		data['CMSSW_CONFIG'] = str.join(' ', imap(os.path.basename, self._config_file_list))
		data['CMSSW_OLD_RELEASETOP'] = self._oldReleaseTop
		if self.prolog.is_active():
			data['CMSSW_PROLOG_EXEC'] = self.prolog.get_command()
			data['CMSSW_PROLOG_SB_IN_FILES'] = str.join(' ', imap(lambda x: x.path_rel, self.prolog.getSBInFiles()))
			data['CMSSW_PROLOG_ARGS'] = self.prolog.getArguments()
		if self.epilog.is_active():
			data['CMSSW_EPILOG_EXEC'] = self.epilog.get_command()
			data['CMSSW_EPILOG_SB_IN_FILES'] = str.join(' ', imap(lambda x: x.path_rel, self.epilog.getSBInFiles()))
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
		for cfgFile in self._config_file_list:
			files.append(utils.Result(path_abs = cfgFile, path_rel = os.path.basename(cfgFile)))
		if self._projectArea and not self._projectAreaTarballSE:
			files.append(utils.Result(path_abs = self._projectAreaTarball, path_rel = os.path.basename(self._projectAreaTarball)))
		return files + [utils.Result(path_abs = utils.get_path_share('gc-run.cmssw.sh', pkg = 'grid_control_cms'), path_rel = 'gc-run.cmssw.sh')]


	# Get files for output sandbox
	def getSBOutFiles(self):
		if not self._config_file_list:
			return SCRAMTask.getSBOutFiles(self)
		return SCRAMTask.getSBOutFiles(self) + utils.QM(self.gzipOut, ['cmssw.log.gz'], []) + ['cmssw.dbs.tar.gz']


	def get_command(self):
		return './gc-run.cmssw.sh $@'


	def getJobArguments(self, jobnum):
		return SCRAMTask.getJobArguments(self, jobnum) + ' ' + self.arguments


	def getVarNames(self):
		result = SCRAMTask.getVarNames(self)
		if not self._has_dataset:
			result.append('MAX_EVENTS')
		return result


	# Get job dependent environment variables
	def get_job_dict(self, jobnum):
		data = SCRAMTask.get_job_dict(self, jobnum)
		if not self._has_dataset:
			data['MAX_EVENTS'] = self.eventsPerJob
		return data


	def getDescription(self, jobnum): # (task name, job name, type)
		result = SCRAMTask.getDescription(self, jobnum)
		if not result.jobType:
			result.jobType = 'analysis'
		return result
