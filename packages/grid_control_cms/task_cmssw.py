# | Copyright 2007-2017 Karlsruhe Institute of Technology
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
from grid_control.backends import WMS
from grid_control.config import ConfigError, Matcher, TriggerInit
from grid_control.output_processor import DebugJobInfoProcessor
from grid_control.tasks.task_data import DataTask
from grid_control.tasks.task_utils import TaskExecutableWrapper
from grid_control.utils import Result, clean_path, create_tarball, get_path_share
from grid_control.utils.file_tools import SafeFile
from grid_control.utils.persistency import load_dict
from grid_control.utils.table import ConsoleTable
from grid_control.utils.user_interface import UserInputInterface
from python_compat import ifilter, imap, set, sorted, unspecified


class SCRAMTask(DataTask):
	config_section_list = DataTask.config_section_list + ['SCRAMTask']

	def __init__(self, config, name):
		DataTask.__init__(self, config, name)
		config.set('area files matcher mode', 'ShellStyleMatcher')

		# SCRAM settings
		scram_arch_default = unspecified
		scram_project = config.get_list('scram project', [])
		if scram_project:  # manual scram setup
			if len(scram_project) != 2:
				raise ConfigError('%r needs exactly 2 arguments: <PROJECT> <VERSION>' % 'scram project')
			self._project_area = None
			self._project_area_selector_list = None
			self._scram_project = scram_project[0]
			self._scram_project_version = scram_project[1]
			# ensure project area is not used
			if 'project area' in config.get_option_list():
				raise ConfigError('Cannot specify both %r and %r' % ('scram project', 'project area'))

		else:  # scram setup used from project area
			self._project_area = config.get_dn('project area')
			self._always_matcher = Matcher.create_instance('AlwaysMatcher', config, [''])
			self._project_area_base_fn = config.get_bool('area files basename', True,
				on_change=TriggerInit('sandbox'))
			self._project_area_matcher = config.get_matcher('area files',
				'-.* -config bin lib python module data *.xml *.sql *.db *.cfi *.cff *.py -CVS -work.* *.pcm',
				default_matcher='BlackWhiteMatcher', on_change=TriggerInit('sandbox'))
			self._log.info('Project area found in: %s', self._project_area)

			# try to determine scram settings from environment settings
			scram_path = os.path.join(self._project_area, '.SCRAM')
			scram_env = self._parse_scram_file(os.path.join(scram_path, 'Environment'))
			try:
				self._scram_project = scram_env['SCRAM_PROJECTNAME']
				self._scram_project_version = scram_env['SCRAM_PROJECTVERSION']
			except:
				raise ConfigError('Installed program in project area not recognized.')

			def filter_arch_dir(dn):
				return os.path.isdir(os.path.join(scram_path, dn))
			for arch_dir in sorted(ifilter(filter_arch_dir, os.listdir(scram_path))):
				scram_arch_default = arch_dir

		self._scram_version = config.get('scram version', 'scramv1')
		self._scram_arch = config.get('scram arch', scram_arch_default)

		self._scram_req_list = []
		if config.get_bool('scram arch requirements', True, on_change=None):
			self._scram_req_list.append((WMS.SOFTWARE, 'VO-cms-%s' % self._scram_arch))
		if config.get_bool('scram project requirements', False, on_change=None):
			self._scram_req_list.append((WMS.SOFTWARE, 'VO-cms-%s' % self._scram_project))
		if config.get_bool('scram project version requirements', False, on_change=None):
			self._scram_req_list.append((WMS.SOFTWARE, 'VO-cms-%s' % self._scram_project_version))

	def get_dependency_list(self):
		return DataTask.get_dependency_list(self) + ['cmssw']

	def get_job_dict(self, jobnum):
		job_env_dict = DataTask.get_job_dict(self, jobnum)
		job_env_dict['SCRAM_VERSION'] = self._scram_version
		job_env_dict['SCRAM_ARCH'] = self._scram_arch
		job_env_dict['SCRAM_PROJECTNAME'] = self._scram_project
		job_env_dict['SCRAM_PROJECTVERSION'] = self._scram_project_version
		return job_env_dict

	def get_requirement_list(self, jobnum):
		# Get job requirements
		return DataTask.get_requirement_list(self, jobnum) + self._scram_req_list

	def _parse_scram_file(self, fn):
		try:
			return load_dict(fn, '=')
		except Exception:
			raise ConfigError('Project area file %s cannot be parsed!' % fn)


class CMSSWDebugJobInfoProcessor(DebugJobInfoProcessor):
	alias_list = ['cmssw_debug']

	def __init__(self):
		DebugJobInfoProcessor.__init__(self)
		self._display_files.append('cmssw.log.gz')


class CMSSW(SCRAMTask):
	alias_list = ['']
	config_section_list = SCRAMTask.config_section_list + ['CMSSW']

	def __init__(self, config, name):
		config.set('se input timeout', '0:30')
		config.set('application', 'cmsRun', section='dashboard')
		config.set('dataset provider', 'DBS3Provider')
		config.set('dataset splitter', 'EventBoundarySplitter')
		config.set('dataset processor', 'LumiDataProcessor', '+=')
		config.set('partition processor', 'BasicPartitionProcessor', '-=')
		config.set('partition processor',
			'LFNPartitionProcessor LumiPartitionProcessor CMSSWPartitionProcessor', '+=')

		self._needed_vn_set = set()
		SCRAMTask.__init__(self, config, name)
		self._uii = UserInputInterface()

		# Setup file path informations
		self._cmsrun_output_files = ['cmssw.dbs.tar.gz']
		if self._do_gzip_std_output:
			self._cmsrun_output_files.append('cmssw.log.gz')
		self._script_fpi = Result(path_rel='gc-run.cmssw.sh',
			path_abs=get_path_share('gc-run.cmssw.sh', pkg='grid_control_cms'))

		if self._scram_project != 'CMSSW':
			raise ConfigError('Project area contains no CMSSW project')

		self._old_release_top = None
		if self._project_area:
			scram_arch_env_path = os.path.join(self._project_area, '.SCRAM', self._scram_arch, 'Environment')
			self._old_release_top = self._parse_scram_file(scram_arch_env_path).get('RELEASETOP')

		self._update_map_error_code2msg(
			get_path_share('gc-run.cmssw.sh', pkg='grid_control_cms'))

		self._project_area_tarball_on_se = config.get_bool(['se runtime', 'se project area'], True)
		self._project_area_tarball = config.get_work_path('cmssw-project-area.tar.gz')

		# Prolog / Epilog script support - warn about old syntax
		self.prolog = TaskExecutableWrapper(config, 'prolog', '')
		self.epilog = TaskExecutableWrapper(config, 'epilog', '')
		if config.get_fn_list('executable', []) != []:
			raise ConfigError('Prefix executable and argument options with either prolog or epilog!')
		self.arguments = config.get('arguments', '')

		# Get cmssw config files and check their existance
		# Check that for dataset jobs the necessary placeholders are in the config file
		if not self._has_dataset:
			self._events_per_job = config.get('events per job', '0')
			# this can be a variable like @USER_EVENTS@!
			self._needed_vn_set.add('MAX_EVENTS')
		fragment = config.get_fn('instrumentation fragment',
			get_path_share('fragmentForCMSSW.py', pkg='grid_control_cms'))
		self._config_fn_list = self._process_config_file_list(config,
			config.get_fn_list('config file', self._get_config_file_default()),
			fragment, auto_prepare=config.get_bool('instrumentation', True),
			must_prepare=self._has_dataset)

		# Create project area tarball
		if self._project_area and not os.path.exists(self._project_area_tarball):
			config.set_state(True, 'init', detail='sandbox')
		# Information about search order for software environment
		self._cmssw_search_dict = self._get_cmssw_path_list(config)
		if config.get_state('init', detail='sandbox'):
			msg = 'CMSSW tarball already exists! Do you want to regenerate it?'
			if os.path.exists(self._project_area_tarball) and not self._uii.prompt_bool(msg, True):
				return
			# Generate CMSSW tarball
			if self._project_area:
				create_tarball(_match_files(self._project_area,
					self._project_area_matcher, self._always_matcher.create_matcher(''),
					self._project_area_base_fn), name=self._project_area_tarball)
			if self._project_area_tarball_on_se:
				config.set_state(True, 'init', detail='storage')

	def get_command(self):
		return './gc-run.cmssw.sh $@'

	def get_job_arguments(self, jobnum):
		return SCRAMTask.get_job_arguments(self, jobnum) + ' ' + self.arguments

	def get_job_dict(self, jobnum):
		# Get job dependent environment variables
		job_env_dict = SCRAMTask.get_job_dict(self, jobnum)
		if not self._has_dataset:
			job_env_dict['MAX_EVENTS'] = self._events_per_job
		job_env_dict.update(dict(self._cmssw_search_dict))
		if self._do_gzip_std_output:
			job_env_dict['GZIP_OUT'] = 'yes'
		if self._project_area_tarball_on_se:
			job_env_dict['SE_RUNTIME'] = 'yes'
		if self._project_area:
			job_env_dict['HAS_RUNTIME'] = 'yes'
		job_env_dict['CMSSW_EXEC'] = 'cmsRun'
		job_env_dict['CMSSW_CONFIG'] = str.join(' ', imap(os.path.basename, self._config_fn_list))
		job_env_dict['CMSSW_OLD_RELEASETOP'] = self._old_release_top
		if self.prolog.is_active():
			job_env_dict['CMSSW_PROLOG_EXEC'] = self.prolog.get_command()
			job_env_dict['CMSSW_PROLOG_SB_IN_FILES'] = str.join(' ',
				imap(lambda x: x.path_rel, self.prolog.get_sb_in_fpi_list()))
			job_env_dict['CMSSW_PROLOG_ARGS'] = self.prolog.get_arguments()
		if self.epilog.is_active():
			job_env_dict['CMSSW_EPILOG_EXEC'] = self.epilog.get_command()
			job_env_dict['CMSSW_EPILOG_SB_IN_FILES'] = str.join(' ',
				imap(lambda x: x.path_rel, self.epilog.get_sb_in_fpi_list()))
			job_env_dict['CMSSW_EPILOG_ARGS'] = self.epilog.get_arguments()
		return job_env_dict

	def get_sb_in_fpi_list(self):
		# Get files for input sandbox
		fpi_list = (SCRAMTask.get_sb_in_fpi_list(self) + self.prolog.get_sb_in_fpi_list() +
			self.epilog.get_sb_in_fpi_list())
		for config_file in self._config_fn_list:
			fpi_list.append(Result(path_abs=config_file, path_rel=os.path.basename(config_file)))
		if self._project_area and not self._project_area_tarball_on_se:
			fpi_list.append(Result(path_abs=self._project_area_tarball,
				path_rel=os.path.basename(self._project_area_tarball)))
		return fpi_list + [self._script_fpi]

	def get_sb_out_fn_list(self):
		# Get files for output sandbox
		if not self._config_fn_list:
			return SCRAMTask.get_sb_out_fn_list(self)
		return SCRAMTask.get_sb_out_fn_list(self) + self._cmsrun_output_files

	def get_se_in_fn_list(self):
		# Get files to be transfered via SE (description, source, target)
		files = SCRAMTask.get_se_in_fn_list(self)
		if self._project_area and self._project_area_tarball_on_se:
			return files + [('CMSSW tarball', self._project_area_tarball, self._task_id + '.tar.gz')]
		return files

	def _config_find_uninitialized(self, config, config_file_list, auto_prepare, must_prepare):
		common_path = os.path.dirname(os.path.commonprefix(config_file_list))

		config_file_list_todo = []
		config_file_status_list = []
		for cfg in config_file_list:
			cfg_new = config.get_work_path(os.path.basename(cfg))
			cfg_new_exists = os.path.exists(cfg_new)
			if cfg_new_exists:
				is_instrumented = self._config_is_instrumented(cfg_new)
				do_copy = False
			else:
				is_instrumented = self._config_is_instrumented(cfg)
				do_copy = True
			do_prepare = (must_prepare or auto_prepare) and not is_instrumented
			do_copy = do_copy or do_prepare
			if do_copy:
				config_file_list_todo.append((cfg, cfg_new, do_prepare))
			config_file_status_list.append({1: cfg.split(common_path, 1)[1].lstrip('/'), 2: cfg_new_exists,
				3: is_instrumented, 4: do_prepare})

		if config_file_status_list:
			config_file_status_header = [(1, 'Config file'), (2, 'Work dir'),
				(3, 'Instrumented'), (4, 'Scheduled')]
			ConsoleTable.create(config_file_status_header, config_file_status_list, 'lccc')
		return config_file_list_todo

	def _config_is_instrumented(self, fn):
		cfg = SafeFile(fn).read_close()
		for tag in self._needed_vn_set:
			if (not '__%s__' % tag in cfg) and (not '@%s@' % tag in cfg):
				return False
		return True

	def _config_store_backup(self, source, target, fragment_path=None):
		content = SafeFile(source).read_close()
		if fragment_path:
			self._log.info('Instrumenting... %s', os.path.basename(source))
			content += SafeFile(fragment_path).read_close()
		SafeFile(target, 'w').write_close(content)

	def _create_datasource(self, config, name, psrc_repository, psrc_list):
		psrc_data = SCRAMTask._create_datasource(self, config, name, psrc_repository, psrc_list)
		if psrc_data is not None:
			self._needed_vn_set.update(psrc_data.get_needed_dataset_keys())
		return psrc_data

	def _get_cmssw_path_list(self, config):
		result = []
		path_cmssw_user = config.get(['cmssw dir', 'vo software dir'], '')
		if path_cmssw_user:
			path_cmssw_local = os.path.abspath(clean_path(path_cmssw_user))
			if os.path.exists(path_cmssw_local):
				path_cmssw_user = path_cmssw_local
		if path_cmssw_user:
			result.append(('CMSSW_DIR_USER', path_cmssw_user))
		if self._old_release_top:
			path_scram_project = os.path.normpath('%s/../../../../' % self._old_release_top)
			result.append(('CMSSW_DIR_PRO', path_scram_project))
		self._log.info('Local jobs will try to use the CMSSW software located here:')
		for idx, loc in enumerate(result):
			self._log.info(' %i) %s', idx + 1, loc[1])
		if result:
			self._log.info('')
		return result

	def _get_config_file_default(self):
		if self.prolog.is_active() or self.epilog.is_active():
			return []
		return unspecified

	def _get_var_name_list(self):
		result = SCRAMTask._get_var_name_list(self)
		if not self._has_dataset:
			result.append('MAX_EVENTS')
		return result

	def _process_config_file_list(self, config, config_file_list,
			fragment_path, auto_prepare, must_prepare):
		# process list of uninitialized config files
		iter_uninitialized_config_files = self._config_find_uninitialized(config,
			config_file_list, auto_prepare, must_prepare)
		for (cfg, cfg_new, do_prepare) in iter_uninitialized_config_files:
			ask_user_msg = 'Do you want to prepare %s for running over the dataset?' % cfg
			if do_prepare and (auto_prepare or self._uii.prompt_bool(ask_user_msg, True)):
				self._config_store_backup(cfg, cfg_new, fragment_path)
			else:
				self._config_store_backup(cfg, cfg_new)

		result = []
		for cfg in config_file_list:
			cfg_new = config.get_work_path(os.path.basename(cfg))
			if not os.path.exists(cfg_new):
				raise ConfigError('Config file %r was not copied to the work directory!' % cfg)
			is_instrumented = self._config_is_instrumented(cfg_new)
			if must_prepare and not is_instrumented:
				raise ConfigError('Config file %r must use %s to work properly!' %
					(cfg, str.join(', ', imap(lambda x: '@%s@' % x, sorted(self._needed_vn_set)))))
			if auto_prepare and not is_instrumented:
				self._log.warning('Config file %r was not instrumented!', cfg)
			result.append(cfg_new)
		return result


def _match_files(dn_root, matcher, selected_dir_matcher, base_fn, dn_rel=''):
	# Return (source fn, target fn) - source == None => update activity
	yield (None, dn_rel)
	for fn in os.listdir(os.path.join(dn_root, dn_rel)):
		fn_rel = os.path.join(dn_rel, fn)
		fn_abs = os.path.join(dn_root, fn_rel)
		if base_fn:
			match = matcher.match(fn)
		else:
			match = matcher.match(fn_rel)
		if match < 0:
			continue
		elif os.path.islink(fn_abs):  # Not excluded symlinks
			yield (fn_abs, fn_rel)
		elif os.path.isdir(fn_abs):  # Recurse into directories
			dir_matcher = matcher
			if match > 0:  # directory was manually selected - add everything
				dir_matcher = selected_dir_matcher
			yield_base_dir = True
			for result in _match_files(dn_root, dir_matcher, selected_dir_matcher, base_fn, fn_rel):
				if yield_base_dir:
					yield (fn_abs, fn_rel)
					yield_base_dir = False
				yield result
		elif match > 0:  # Add matches
			yield (fn_abs, fn_rel)
