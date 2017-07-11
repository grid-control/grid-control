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

# Generic base class for workload management systems

import os, glob, shutil, logging
from grid_control.backends.access import AccessToken
from grid_control.backends.aspect_status import CheckInfo
from grid_control.backends.storage import StorageManager
from grid_control.config import TriggerInit
from grid_control.event_base import RemoteEventHandler
from grid_control.gc_plugin import NamedPlugin
from grid_control.output_processor import JobResult
from grid_control.utils import DictFormat, Result, abort, create_tarball, ensure_dir_exists, get_path_pkg, get_path_share, resolve_path, safe_write  # pylint:disable=line-too-long
from grid_control.utils.activity import Activity
from grid_control.utils.algos import dict_union
from grid_control.utils.data_structures import make_enum
from grid_control.utils.file_tools import SafeFile, VirtualFile
from hpfwk import AbstractError, NestedException, clear_current_exception, ignore_exception
from python_compat import ichain, identity, imap, izip, lchain, lmap, set, sorted


class BackendError(NestedException):
	pass


BackendJobState = make_enum([  # pylint:disable=invalid-name
	'ABORTED',  # job was aborted by the WMS
	'CANCELLED',  # job was cancelled
	'DONE',  # job is finished
	'QUEUED',  # job is at WMS and is assigned a place to run
	'RUNNING',  # job is running
	'UNKNOWN',  # job status is unknown
	'WAITING',  # job is at WMS but was not yet assigned some place to run
])


class WMS(NamedPlugin):
	alias_list = ['NullWMS']
	config_section_list = NamedPlugin.config_section_list + ['wms', 'backend']
	config_tag_name = 'wms'

	def __init__(self, config, name):
		name = (name or self.__class__.__name__).upper().replace('.', '_')
		NamedPlugin.__init__(self, config, name)
		self._wait_idle = config.get_int('wait idle', 60, on_change=None)
		self._wait_work = config.get_int('wait work', 10, on_change=None)
		self._job_parser = config.get_plugin('job parser', 'JobInfoProcessor',
			cls='JobInfoProcessor', on_change=None)
		self._remote_event_handler = config.get_composited_plugin(
			['remote monitor', 'remote event handler'], '', 'MultiRemoteEventHandler',
			cls=RemoteEventHandler, bind_kwargs={'tags': [self]},
			require_plugin=False, on_change=TriggerInit('sandbox')) or RemoteEventHandler(config, 'dummy')

	def can_submit(self, needed_time, can_currently_submit):
		raise AbstractError

	def cancel_jobs(self, gc_id_list):
		# Cancel jobs and return list of successfully cancelled gc_id_list
		raise AbstractError

	def check_jobs(self, gc_id_list):
		# Check status and return (gc_id, job_state, job_info) for active jobs
		raise AbstractError

	def deploy_task(self, task, transfer_se, transfer_sb):
		raise AbstractError

	def get_access_token(self, gc_id):
		raise AbstractError  # Return access token instance responsible for this gc_id

	def get_interval_info(self):  # Return (waitIdle, wait)
		return Result(wait_on_idle=self._wait_idle, wait_between_steps=self._wait_work)

	def retrieve_jobs(self, gc_id_jobnum_list):
		raise AbstractError  # Return (jobnum, exit_code, data, outputdir) for retrived jobs

	def submit_jobs(self, jobnum_list, task):  # jobnum_list = [1, 2, ...]
		raise AbstractError  # Return (jobnum, gc_id, data) for successfully submitted jobs

	def _create_gc_id(self, wms_id):
		if not wms_id:
			return None
		return 'WMSID.%s.%s' % (self._name, wms_id)

	def _get_map_wms_id2gc_id(self, gc_id_list):
		result = {}
		for gc_id in gc_id_list:
			wms_id = self._split_gc_id(gc_id)[1]
			if wms_id in result:
				raise BackendError('Multiple gc_id_list map to the same wms_id!')
			result[wms_id] = gc_id
		return result

	def _iter_wms_ids(self, gc_id_jobnum_list):
		for (gc_id, _) in gc_id_jobnum_list:
			yield self._split_gc_id(gc_id)[1]

	def _split_gc_id(self, gc_id):
		return tuple(gc_id.split('.', 2)[1:])

make_enum(['WALLTIME', 'CPUTIME', 'MEMORY', 'CPUS', 'BACKEND',
	'SITES', 'QUEUES', 'SOFTWARE', 'STORAGE', 'DISKSPACE'], WMS)


class BasicWMS(WMS):
	def __init__(self, config, name, check_executor, cancel_executor):
		WMS.__init__(self, config, name)
		for executor in [check_executor, cancel_executor]:
			executor.setup(self._log)
		(self._check_executor, self._cancel_executor) = (check_executor, cancel_executor)

		if self._name != self.__class__.__name__.upper():
			self._log.info('Using batch system: %s (%s)', self.__class__.__name__, self._name)
		else:
			self._log.info('Using batch system: %s', self._name)

		self._runlib = config.get_work_path('gc-run.lib')
		fp = SafeFile(self._runlib, 'w')
		content = SafeFile(get_path_share('gc-run.lib')).read()
		fp.write(content.replace('__GC_VERSION__', __import__('grid_control').__version__))
		fp.close()
		self._path_output = config.get_work_path('output')
		self._path_file_cache = config.get_work_path('files')
		ensure_dir_exists(self._path_output, 'output directory')
		self._path_fail = config.get_work_path('fail')

		# Initialise access token and storage managers

		# UI -> SE -> WN
		self._sm_se_in = config.get_plugin('se input manager', 'SEStorageManager',
			cls=StorageManager, bind_kwargs={'tags': [self]}, pargs=('se', 'se input', 'SE_INPUT'))
		self._sm_sb_in = config.get_plugin('sb input manager', 'LocalSBStorageManager',
			cls=StorageManager, bind_kwargs={'tags': [self]}, pargs=('sandbox', 'sandbox', 'SB_INPUT'))
		# UI <- SE <- WN
		self._sm_se_out = config.get_plugin('se output manager', 'SEStorageManager',
			cls=StorageManager, bind_kwargs={'tags': [self]}, pargs=('se', 'se output', 'SE_OUTPUT'))
		self._sm_sb_out = None

		self._token = config.get_composited_plugin(['proxy', 'access token'], 'TrivialAccessToken',
			'MultiAccessToken', cls=AccessToken, bind_kwargs={'inherit': True, 'tags': [self]})
		self._output_fn_list = None

	def can_submit(self, needed_time, can_currently_submit):
		return self._token.can_submit(needed_time, can_currently_submit)

	def cancel_jobs(self, gc_id_list):
		return self._run_executor('cancelling jobs', self._cancel_executor, identity,
			gc_id_list, self._name)

	def check_jobs(self, gc_id_list):
		# Check status and return (gc_id, job_state, job_info) for active jobs
		def _fmt(value):  # translate CheckInfo enum values in job_info dictionary
			job_info = value[2]  # get mutable job_info dictionary from the immutable tuple
			for key in CheckInfo.enum_value_list:
				if key in job_info:
					job_info[CheckInfo.enum2str(key)] = job_info.pop(key)
			return value
		return self._run_executor('checking job status', self._check_executor, _fmt, gc_id_list)

	def deploy_task(self, task, transfer_se, transfer_sb):
		# HACK
		self._output_fn_list = lmap(lambda d_s_t: d_s_t[2], self._get_out_transfer_info_list(task))
		task.validate_variables()

		# add task SE files to SM
		self._sm_se_in.add_file_list(lmap(lambda d_s_t: d_s_t[2], task.get_se_in_fn_list()))
		# Transfer common SE files
		if transfer_se:
			self._sm_se_in.do_transfer(task.get_se_in_fn_list())

		def _convert(fn_list):
			for fn in fn_list:
				if isinstance(fn, str):
					yield (fn, os.path.basename(fn))
				else:
					yield (fn, os.path.basename(fn.name))

		# Package sandbox tar file
		self._log.log(logging.INFO1, 'Packing sandbox')
		sandbox = self._get_sandbox_name(task)
		ensure_dir_exists(os.path.dirname(sandbox), 'sandbox directory')
		if not os.path.exists(sandbox) or transfer_sb:
			sandbox_file_list = self._get_sandbox_file_list(task, [self._sm_se_in, self._sm_se_out])
			create_tarball(_convert(sandbox_file_list), name=sandbox)

	def get_access_token(self, gc_id):
		return self._token

	def retrieve_jobs(self, gc_id_jobnum_list):  # Process output sandboxes returned by getJobsOutput
		# Function to force moving a directory
		def _force_move(source, target):
			try:
				if os.path.exists(target):
					shutil.rmtree(target)
			except IOError:
				self._log.exception('%r cannot be removed', target)
				clear_current_exception()
				return False
			try:
				shutil.move(source, target)
			except IOError:
				self._log.exception('Error moving job output directory from %r to %r', source, target)
				clear_current_exception()
				return False
			return True

		jobnum_list_retrieved = []

		for jobnum_input, output_dn in self._get_jobs_output(gc_id_jobnum_list):
			# jobnum_input != None, output_dn == None => Job could not be retrieved
			if output_dn is None:
				if jobnum_input not in jobnum_list_retrieved:
					yield (jobnum_input, -1, {}, None)
				continue

			# jobnum_input == None, output_dn != None => Found leftovers of job retrieval
			if jobnum_input is None:
				continue

			# jobnum_input != None, output_dn != None => Job retrieval from WMS was ok
			job_fn = os.path.join(output_dn, 'job.info')
			job_info = ignore_exception(Exception, None, self._job_parser.process, output_dn)
			if job_info is None:
				self._log.exception('Unable to parse job.info')
			if job_info:
				jobnum = job_info[JobResult.JOBNUM]
				if jobnum != jobnum_input:
					raise BackendError('Invalid job id in job file %s' % job_fn)
				if _force_move(output_dn, os.path.join(self._path_output, 'job_%d' % jobnum)):
					jobnum_list_retrieved.append(jobnum_input)
					yield (jobnum, job_info[JobResult.EXITCODE], job_info[JobResult.RAW], output_dn)
				else:
					yield (jobnum, -1, {}, None)
				continue

			# Clean empty output_dns
			for sub_dn in imap(lambda x: x[0], os.walk(output_dn, topdown=False)):
				ignore_exception(Exception, None, os.rmdir, sub_dn)

			if os.path.exists(output_dn):
				# Preserve failed job
				ensure_dir_exists(self._path_fail, 'failed output directory')
				_force_move(output_dn, os.path.join(self._path_fail, os.path.basename(output_dn)))

			yield (jobnum_input, -1, {}, None)

	def submit_jobs(self, jobnum_list, task):
		for jobnum in jobnum_list:
			if abort():
				break
			yield self._submit_job(jobnum, task)

	def _get_in_transfer_info_list(self, task):
		return [
			('GC Runtime', get_path_share('gc-run.sh'), 'gc-run.sh'),
			('GC Runtime library', self._runlib, 'gc-run.lib'),
			('GC Sandbox', self._get_sandbox_name(task), 'gc-sandbox.tar.gz'),
		]

	def _get_jobs_output(self, gc_id_jobnum_list):
		raise AbstractError  # Return (jobnum, sandbox) for finished jobs

	def _get_out_transfer_info_list(self, task):
		return [
			('GC Wrapper - stdout', 'gc.stdout', 'gc.stdout'),
			('GC Wrapper - stderr', 'gc.stderr', 'gc.stderr'),
			('GC Job summary', 'job.info', 'job.info'),
		] + lmap(lambda fn: ('Task output', fn, fn), task.get_sb_out_fn_list())

	def _get_sandbox_file_list(self, task, sm_list):
		# Prepare all input files
		dep_list = set(ichain(imap(lambda x: x.get_dependency_list(), [task] + sm_list)))
		dep_fn_list = lmap(lambda dep: resolve_path('env.%s.sh' % dep,
			lmap(lambda pkg: get_path_share('', pkg=pkg), os.listdir(get_path_pkg()))), dep_list)
		task_config_dict = dict_union(self._remote_event_handler.get_mon_env_dict(),
			*imap(lambda x: x.get_task_dict(), [task] + sm_list))
		task_config_dict.update({'GC_DEPFILES': str.join(' ', dep_list),
			'GC_USERNAME': self._token.get_user_name(), 'GC_WMS_NAME': self._name})
		task_config_str_list = DictFormat(escape_strings=True).format(
			task_config_dict, format='export %s%s%s\n')
		vn_alias_dict = dict(izip(self._remote_event_handler.get_mon_env_dict().keys(),
			self._remote_event_handler.get_mon_env_dict().keys()))
		vn_alias_dict.update(task.get_var_alias_map())
		vn_alias_str_list = DictFormat(delimeter=' ').format(vn_alias_dict, format='%s%s%s\n')

		# Resolve wildcards in task input files
		def _get_task_fn_list():
			for fpi in task.get_sb_in_fpi_list():
				matched = glob.glob(fpi.path_abs)
				if matched != []:
					for match in matched:
						yield match
				else:
					yield fpi.path_abs
		return lchain([self._remote_event_handler.get_file_list(), dep_fn_list, _get_task_fn_list(), [
			VirtualFile('_config.sh', sorted(task_config_str_list)),
			VirtualFile('_varmap.dat', sorted(vn_alias_str_list))]])

	def _get_sandbox_name(self, task):
		return os.path.join(self._path_file_cache,
			task.get_description().task_id, self._name, 'gc-sandbox.tar.gz')

	def _run_executor(self, desc, executor, fmt, gc_id_list, *args):
		# Perform some action with the executor, translate wms_id -> gc_id and format the result
		activity = Activity(desc)
		map_wms_id2gc_id = self._get_map_wms_id2gc_id(gc_id_list)
		wms_id_list = sorted(map_wms_id2gc_id.keys())

		for result in executor.execute(wms_id_list, *args):
			wms_id = result[0]  # result[0] is the wms_id by convention
			gc_id = map_wms_id2gc_id.pop(wms_id, None)
			if gc_id is not None:
				yield fmt((gc_id,) + result[1:])
			else:
				self._log.debug('unable to find gc_id for wms_id %r', wms_id)
		activity.finish()

	def _submit_job(self, jobnum, task):
		raise AbstractError  # Return (jobnum, gc_id, data) for successfully submitted jobs

	def _write_job_config(self, job_config_fn, jobnum, task, extras):
		try:
			job_env_dict = dict_union(task.get_job_dict(jobnum), extras)
			job_env_dict['GC_ARGS'] = task.get_job_arguments(jobnum).strip()
			content = DictFormat(escape_strings=True).format(job_env_dict, format='export %s%s%s\n')
			safe_write(open(job_config_fn, 'w'), content)
		except Exception:
			raise BackendError('Could not write job config data to %s.' % job_config_fn)


class Grid(WMS):  # redirector - used to avoid loading the whole grid module just for the default
	config_section_list = WMS.config_section_list + ['grid']

	def __new__(cls, config, name):
		grid_wms = 'GliteWMS'
		grid_config = config.change_view(view_class='TaggedConfigView',
			set_classes=[WMS.get_class(grid_wms)])
		return WMS.create_instance(grid_wms, grid_config, name)
