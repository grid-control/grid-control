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

import os, time, random, logging
from grid_control.backends import WMS
from grid_control.config import ConfigError, NoVarCheck, TriggerInit
from grid_control.gc_plugin import ConfigurablePlugin, NamedPlugin
from grid_control.parameters import ParameterAdapter, ParameterFactory, ParameterInfo
from grid_control.utils import Result, get_path_share, get_version, replace_with_dict
from grid_control.utils.algos import dict_union
from grid_control.utils.file_tools import SafeFile
from grid_control.utils.parsing import str_guid
from hpfwk import AbstractError
from python_compat import ichain, ifilter, imap, izip, lmap, md5_hex


class JobNamePlugin(ConfigurablePlugin):
	def get_name(self, task, task_id, jobnum):
		raise AbstractError


class TaskNamePlugin(ConfigurablePlugin):
	def get_name(self, task):
		raise AbstractError


class TaskModule(NamedPlugin):  # pylint:disable=too-many-instance-attributes
	config_section_list = NamedPlugin.config_section_list + ['task']
	config_tag_name = 'task'

	def __init__(self, config, name):  # Read configuration options and init vars
		NamedPlugin.__init__(self, config, name)
		init_sandbox = TriggerInit('sandbox')
		self._var_checker = NoVarCheck(config)

		# Task requirements
		# Move this into parameter manager?
		jobs_config = config.change_view(view_class='TaggedConfigView',
			add_sections=['jobs'], add_tags=[self])
		self.wall_time = jobs_config.get_time('wall time', on_change=None)
		self._cpu_time = jobs_config.get_time('cpu time', self.wall_time, on_change=None)
		self._cores = jobs_config.get_int(['cores', 'cpus'], 1, on_change=None)
		self._memory = jobs_config.get_int('memory', -1, on_change=None)
		self._job_timeout = jobs_config.get_time('node timeout', -1, on_change=init_sandbox)

		# Compute / get task ID
		self._task_id = config.get('task id', 'GC' + md5_hex(str(time.time()))[:12], persistent=True)
		self._task_date = config.get('task date', time.strftime('%Y-%m-%d'),
			persistent=True, on_change=init_sandbox)
		self._task_time = config.get('task time', time.strftime('%H%M%S'),
			persistent=True, on_change=init_sandbox)
		task_name_generator = config.get_plugin('task name generator',
			'DefaultTaskName', cls=TaskNamePlugin)
		self._task_name = task_name_generator.get_name(self)
		self._task_config_name = config.get_config_name()
		self._job_name_generator = config.get_plugin('job name generator',
			'DefaultJobName', cls=JobNamePlugin)

		# Storage setup
		storage_config = config.change_view(view_class='TaggedConfigView',
			set_classes=None, set_names=None, add_sections=['storage'], add_tags=[self])
		scratch_space_used = storage_config.get_int('scratch space used', 5000, on_change=init_sandbox)
		lz_space_used = storage_config.get_int('landing zone space used', 100, on_change=init_sandbox)
		self._task_var_dict = {
			# Space limits
			'SCRATCH_UL': scratch_space_used,
			'SCRATCH_LL': storage_config.get_int('scratch space left', 1, on_change=init_sandbox),
			'LANDINGZONE_UL': lz_space_used,
			'LANDINGZONE_LL': storage_config.get_int('landing zone space left', 1, on_change=init_sandbox),
		}
		storage_config.set('se output pattern', 'job_@GC_JOB_ID@_@X@')
		self._se_min_size = storage_config.get_int('se min size', -1, on_change=init_sandbox)
		self._disk_min = max(scratch_space_used, lz_space_used)

		self._sb_in_fn_list = config.get_path_list('input files', [], on_change=init_sandbox)
		self._sb_out_fn_list = config.get_list('output files', [], on_change=init_sandbox)
		self._do_gzip_std_output = config.get_bool('gzip output', True, on_change=init_sandbox)

		self._subst_files = config.get_list('subst files', [], on_change=init_sandbox)
		self._dependencies = lmap(str.lower, config.get_list('depends', [], on_change=init_sandbox))

		# Get error messages from gc-run.lib comments
		self.map_error_code2msg = {}
		self._update_map_error_code2msg(get_path_share('gc-run.lib'))

		# Init parameter source manager
		psrc_repository = {}
		self._setup_repository(config, psrc_repository)
		pfactory = config.get_plugin('internal parameter factory', 'BasicParameterFactory',
			cls=ParameterFactory, bind_kwargs={'tags': [self], 'inherit': True})
		self._source = config.get_plugin('parameter adapter', 'TrackedParameterAdapter',
			cls=ParameterAdapter, pargs=(pfactory.get_psrc(psrc_repository),))
		self._log.log(logging.DEBUG3, 'Using parameter adapter %s', repr(self._source))

		self._log.info('Current task ID: %s', self._task_id)
		self._log.info('Task started on: %s', self._task_date)

	def can_finish(self):
		return self._source.can_finish()

	def can_submit(self, jobnum):
		return self._source.can_submit(jobnum)

	def get_command(self):
		raise AbstractError

	def get_dependency_list(self):
		return list(self._dependencies)

	def get_description(self, jobnum=None):  # (task name, job name)
		job_name = None
		if jobnum is not None:
			job_name = self._job_name_generator.get_name(task=self, task_id=self._task_id, jobnum=jobnum)
		return Result(task_id=self._task_id, task_name=self._task_name,
			job_id=jobnum, job_name=job_name)

	def get_intervention(self):
		# Intervene in job management - return (redoJobs, disableJobs, size_change)
		return self._source.resync()

	def get_job_arguments(self, jobnum):
		return ''

	def get_job_dict(self, jobnum):  # Get job dependent environment variables
		job_psp = self._source.get_job_content(jobnum)
		job_env_dict = dict(imap(lambda key: (key.value, job_psp.get(key.value, '')),
			self._source.get_job_metadata()))
		job_env_dict['GC_ARGS'] = self.get_job_arguments(jobnum)
		return dict_union(job_env_dict, self._get_const_job_env())

	def get_job_len(self):
		return self._source.get_job_len()

	def get_requirement_list(self, jobnum):  # Get job requirements
		return [
			(WMS.WALLTIME, self.wall_time),
			(WMS.CPUTIME, self._cpu_time),
			(WMS.MEMORY, self._memory),
			(WMS.CPUS, self._cores),
			(WMS.DISKSPACE, self._disk_min),
		] + self._source.get_job_content(jobnum)[ParameterInfo.REQS]

	def get_sb_in_fpi_list(self):  # Get file path infos for input sandbox
		def _create_fpi(fn):
			return Result(path_abs=fn, path_rel=os.path.basename(fn))
		return lmap(_create_fpi, self._sb_in_fn_list)

	def get_sb_out_fn_list(self):  # Get files for output sandbox
		return list(self._sb_out_fn_list)

	def get_se_in_fn_list(self):
		return []

	def get_task_dict(self):  # OLD API: Get environment variables for gc_config.sh
		task_base_dict = {
			# Storage element
			'SE_MINFILESIZE': self._se_min_size,
			# Sandbox
			'SB_OUTPUT_FILES': str.join(' ', self.get_sb_out_fn_list()),
			'SB_INPUT_FILES': str.join(' ', imap(lambda x: x.path_rel, self.get_sb_in_fpi_list())),
			# Runtime
			'GC_JOBTIMEOUT': self._job_timeout,
			'GC_RUNTIME': self.get_command(),
			# Seeds and substitutions
			'SUBST_FILES': str.join(' ', imap(os.path.basename, self._get_subst_fn_list())),
			'GC_SUBST_OLD_STYLE': str('__' in self._var_checker.markers).lower(),
			# Task infos
			'GC_TASK_CONF': self._task_config_name,
			'GC_TASK_DATE': self._task_date,
			'GC_TASK_TIME': self._task_time,
			'GC_TASK_ID': self._task_id,
			'GC_VERSION': get_version(),
		}
		return dict_union(task_base_dict, self._task_var_dict)

	def get_transient_variables(self):  # variables that are defined during the job itself
		def _create_guid():
			return str_guid(str.join('', imap(lambda x: "%02x" % x, imap(random.randrange, [256] * 16))))
		return {'GC_DATE': time.strftime("%F"), 'GC_TIMESTAMP': time.strftime("%s"),
			'GC_GUID': _create_guid(), 'RANDOM': str(random.randrange(0, 900000000))}

	def get_var_alias_map(self):
		# Transient variables
		transients = ['GC_DATE', 'GC_TIMESTAMP', 'GC_GUID']  # these variables are determined on the WN
		# Alias vars: Eg. __MY_JOB__ will access $GC_JOB_ID - used mostly for compatibility
		var_alias_map = {'DATE': 'GC_DATE', 'TIMESTAMP': 'GC_TIMESTAMP', 'GUID': 'GC_GUID',
			'MY_JOBID': 'GC_JOB_ID', 'MY_JOB': 'GC_JOB_ID', 'JOBID': 'GC_JOB_ID', 'GC_JOBID': 'GC_JOB_ID',
			'CONF': 'GC_CONF', 'TASK_ID': 'GC_TASK_ID'}
		var_name_list = self._get_var_name_list() + transients
		var_alias_map.update(dict(izip(var_name_list, var_name_list)))  # include reflexive mappings
		return var_alias_map

	def substitute_variables(self, name, inp, jobnum=None, additional_var_dict=None, check=True):
		additional_var_dict = additional_var_dict or {}
		merged_var_dict = dict_union(additional_var_dict, self._get_const_job_env())
		if jobnum is not None:
			merged_var_dict.update(self.get_job_dict(jobnum))

		def _do_subst(value):
			return replace_with_dict(value, merged_var_dict,
				ichain([self.get_var_alias_map().items(), izip(additional_var_dict, additional_var_dict)]))
		result = _do_subst(_do_subst(str(inp)))
		if check and self._var_checker.check(result):
			raise ConfigError('%s references unknown variables: %s' % (name, result))
		return result

	def validate_variables(self):
		example_vars = dict.fromkeys(self._get_var_name_list(), '')
		example_vars.update(dict.fromkeys(['X', 'XBASE', 'XBASENAME', 'XEXT',
			'GC_DATE', 'GC_TASK_DATE', 'GC_TIMESTAMP', 'GC_GUID', 'RANDOM'], ''))
		for (name, value) in example_vars.items():
			self.substitute_variables(name, value, None, example_vars)

	def _get_const_job_env(self):
		return {
			# Runtime
			'GC_JOBTIMEOUT': self._job_timeout,
			'GC_RUNTIME': self.get_command(),
			# Seeds and substitutions
			'SUBST_FILES': str.join(' ', imap(os.path.basename, self._get_subst_fn_list())),
			'GC_SUBST_OLD_STYLE': str('__' in self._var_checker.markers).lower(),
			# Task infos
			'GC_TASK_CONF': self._task_config_name,
			'GC_TASK_DATE': self._task_date,
			'GC_TASK_ID': self._task_id,
			'GC_TASK_NAME': self._task_name,
			'GC_TASK_TIME': self._task_time,
			'GC_VERSION': get_version(),
		}

	def _get_subst_fn_list(self):  # Get files whose content will be subject to variable substitution
		return list(self._subst_files)

	def _get_var_name_list(self):
		return list(self._get_const_job_env().keys()) + \
			lmap(lambda key: key.value, self._source.get_job_metadata())

	def _setup_repository(self, config, psrc_repository):
		pass

	def _update_map_error_code2msg(self, fn):
		# Read comments with error codes at the beginning of file: # <code> - description
		for line in ifilter(lambda x: x.startswith('#'), SafeFile(fn).iter_close()):
			tmp = lmap(str.strip, line.lstrip('#').split(' - ', 1))
			if tmp[0].isdigit() and (len(tmp) == 2):
				self.map_error_code2msg[int(tmp[0])] = tmp[1]


class ConfigurableJobName(JobNamePlugin):
	alias_list = ['config']

	def __init__(self, config):
		JobNamePlugin.__init__(self, config)
		self._name = config.get('job name', '@GC_TASK_ID@.@GC_JOB_ID@', on_change=None)

	def get_name(self, task, task_id, jobnum):
		return task.substitute_variables('job name', self._name, jobnum)


class DefaultJobName(JobNamePlugin):
	alias_list = ['default']

	def get_name(self, task, task_id, jobnum):
		return task_id[:10] + '.' + str(jobnum)


class DefaultTaskName(TaskNamePlugin):
	alias_list = ['default']

	def __init__(self, config):
		TaskNamePlugin.__init__(self, config)
		self._name = config.get_config_name()

	def get_name(self, task):
		return self._name
