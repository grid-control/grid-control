# | Copyright 2017 Karlsruhe Institute of Technology
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

import os, glob, shutil
from grid_control.backends.access import AccessToken
from grid_control.backends.aspect_status import CheckInfo
from grid_control.backends.backend_tools import ChunkedExecutor, ChunkedStatusExecutor
from grid_control.backends.broker_base import Broker, MultiBroker
from grid_control.backends.storage import StorageManager
from grid_control.backends.wms import BackendError, WMS
from grid_control.config import TriggerInit
from grid_control.event_base import RemoteEventHandler
from grid_control.output_processor import JobResult
from grid_control.utils import DictFormat, abort, create_tarball, ensure_dir_exists, get_path_pkg, get_path_share, get_version, resolve_path  # pylint:disable=line-too-long
from grid_control.utils.activity import Activity
from grid_control.utils.file_tools import SafeFile, VirtualFile
from hpfwk import AbstractError, clear_current_exception, ignore_exception
from python_compat import ichain, identity, imap, izip, lmap, set, sorted


class BasicWMS(WMS):
	def __init__(self, config, name, broker_list, check_executor, cancel_executor):
		WMS.__init__(self, config, name)
		do_chunk = config.get_bool('enable chunk', False, on_change=None)
		if do_chunk:
			check_executor = ChunkedStatusExecutor(config, 'check', check_executor)
			cancel_executor = ChunkedExecutor(config, 'cancel', cancel_executor)
		(self._check_executor, self._cancel_executor) = (check_executor, cancel_executor)

		if self._name != self.__class__.__name__.upper():
			self._log.info('Using batch system: %s (%s)', self.__class__.__name__, self._name)
		else:
			self._log.info('Using batch system: %s', self._name)

		self._fn_runlib = config.get_work_path('gc-run.lib')
		self._path_output = config.get_work_path('output')
		self._dn_archive = config.get_work_path('files')
		ensure_dir_exists(self._path_output, 'output directory')
		self._path_fail = config.get_work_path('fail')
		self._env_fmt = DictFormat(escape_strings=True)

		# Initialise access token and storage managers
		self._sm_se_in = config.get_plugin('se input manager', 'SEStorageManager',
			cls=StorageManager, bind_kwargs={'tags': [self]}, pargs=('se', 'se input'))
		self._sm_se_out = config.get_plugin('se output manager', 'SEStorageManager',
			cls=StorageManager, bind_kwargs={'tags': [self]}, pargs=('se', 'se output'))
		self._token = config.get_composited_plugin(['proxy', 'access token'], 'TrivialAccessToken',
			'MultiAccessToken', cls=AccessToken, bind_kwargs={'inherit': True, 'tags': [self]})

		self._remote_event_handler = config.get_composited_plugin(
			['remote monitor', 'remote event handler'], '', 'MultiRemoteEventHandler',
			cls=RemoteEventHandler, bind_kwargs={'tags': [self]},
			require_plugin=False, on_change=TriggerInit('sandbox')) or RemoteEventHandler(config, 'dummy')

		final_broker = Broker.create_instance('FinalBroker', config, name, None)
		self._broker = MultiBroker(config, name, broker_list + [final_broker], 'backend')

	def can_submit(self, needed_time, can_currently_submit):
		return self._token.can_submit(needed_time, can_currently_submit)

	def cancel_jobs(self, gc_id_list):
		return self._run_executor('cancelling jobs', self._cancel_executor,
			identity, gc_id_list, self._name)

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
		task.validate_variables()
		if transfer_se:  # Transfer common SE files
			self._sm_se_in.do_transfer(task.get_se_in_fn_list())

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
			activity = Activity('%s: Submitting job %d' % (self._name, jobnum))
			(gc_id, submit_data) = self._submit_job(*self._get_job_bundle(jobnum, task))
			yield (jobnum, gc_id, submit_data)
			activity.finish()

	def _create_job_archive(self, task, job_desc, jobnum):
		return _create_archive('job archive', 'Creating file archive for job %d' % jobnum,
			os.path.join(self._dn_archive, job_desc.task_id, self._name,
				'gc-job.%s.%s.tar.gz' % (job_desc.task_id, jobnum)),
			self._get_job_input_ft_list, task, jobnum)

	def _create_task_archive(self, task, job_desc):
		return _create_archive('task archive', 'Creating file archive for task %s' % job_desc.task_id,
			os.path.join(self._dn_archive, job_desc.task_id, 'gc-task.%s.tar.gz' % job_desc.task_id),
			self._get_task_input_ft_list, task)

	def _get_job_bundle(self, jobnum, task):
		exec_fn = get_path_share('gc-run-all')
		job_desc = task.get_description(jobnum)
		sb_in_fn_list = [  # TODO: automatic routing
			self._create_task_archive(task, job_desc),
			self._create_job_archive(task, job_desc, jobnum)
		]
		sb_out_fn_list = self._get_sandbox_output_fn_list(jobnum, task)
		req_list = self._broker.process(task.get_requirement_list(jobnum))
		return (job_desc, exec_fn, [], sb_in_fn_list, sb_out_fn_list, req_list)

	def _get_job_env(self, jobnum, task):
		job_env_dict = {}
		job_env_dict.update(task.get_job_dict(jobnum))
		job_env_dict.update(self._remote_event_handler.get_mon_env_dict())
		job_env_dict['SB_OUTPUT_FILES'] = str.join(' ', task.get_sb_out_fn_list()),
		job_env_dict['SB_INPUT_FILES'] = str.join(' ', imap(os.path.basename, task.get_sb_in_fn_list())),
		dep_list = set(ichain(imap(lambda x: x.get_dependency_list(),
			[task, self._sm_se_in, self._sm_se_out])))
		job_env_dict['GC_DEPFILES'] = str.join(' ', dep_list)
		job_env_dict['GC_USERNAME'] = self._token.get_user_name()
		job_env_dict['GC_WMS_NAME'] = self._name
		job_env_dict['GC_ARGS'] = task.get_job_arguments(jobnum).strip()
		return job_env_dict

	def _get_job_input_ft_list(self, task, jobnum):
		dep_list = set(task.get_dependency_list())
		dep_list.update(self._sm_se_in.get_dependency_list())
		dep_list.update(self._sm_se_out.get_dependency_list())
		dep_search_list = lmap(lambda pkg: get_path_share('', pkg=pkg), os.listdir(get_path_pkg()))

		input_ft_list = lmap(lambda dep: resolve_path('env.%s.sh' % dep, dep_search_list), dep_list)
		input_ft_list.extend(self._sm_se_in.get_process(lmap(lambda d_s_t: d_s_t[2],
			task.get_se_in_fn_list()))[1])
		input_ft_list.extend(self._sm_se_out.get_process([])[1])
		input_ft_list.extend(self._remote_event_handler.get_file_list())
		input_ft_list.extend(task.get_schedule(jobnum)[1])
		# Variable setup file
		job_env = self._get_job_env(jobnum, task)
		job_env_line_list = self._env_fmt.format(job_env, format='export %s%s%s\n')
		input_ft_list.append((VirtualFile(None, job_env_line_list), '_gc_job_env.sh'))
		# Variable name map
		vn_alias_dict = dict(task.get_var_alias_map())
		vn_alias_dict.update(dict(izip(job_env, job_env)))
		vn_alias_str_list = self._env_fmt.format(vn_alias_dict, format='%s%s%s\n')
		input_ft_list.append((VirtualFile(None, vn_alias_str_list), '_gc_varmap.dat'))
		# Store sandbox output files for task-independent retrieval
		input_ft_list.append((VirtualFile(None, lmap(lambda output_fn:
			output_fn + '\n', self._get_sandbox_output_fn_list(jobnum, task))), '_gc_sb_out.dat'))
		return input_ft_list

	def _get_jobs_output(self, gc_id_jobnum_list):
		raise AbstractError  # Return (jobnum, sandbox) for finished jobs

	def _get_sandbox_output_fn_list(self, jobnum, task):
		return ['gc.stdout', 'gc.stderr', 'job.info'] + task.get_sb_out_fn_list()

	def _get_task_input_ft_list(self, task):  # TODO: recognize common files
		for fn in task.get_sb_in_fn_list():
			matched_fn_list = glob.glob(fn)  # Resolve wildcards in task input files
			if not matched_fn_list:
				raise Exception('Unable to match input file wildcard %r' % fn)
			for match_fn in matched_fn_list:
				yield (match_fn, os.path.basename(match_fn))
		if not os.path.exists(self._fn_runlib):
			content = SafeFile(get_path_share('gc-run.lib')).read_close()
			content = content.replace('__GC_VERSION__', get_version())
			SafeFile(self._fn_runlib, 'w').write_close(content)
		yield (self._fn_runlib, os.path.basename(self._fn_runlib))
		yield (get_path_share('gc-machine-info'), 'gc-machine-info')
		yield (get_path_share('gc-storage-tool'), 'gc-storage-tool')
		yield (get_path_share('gc-storage-wrapper'), 'gc-storage-wrapper')

	def _submit_job(self, job_desc, exec_fn, arg_list, sb_in_fn_list, sb_out_fn_list, req_list):
		raise AbstractError  # Return (gc_id, submit_data) for successfully submitted jobs


def _create_archive(desc, msg, fn, ft_list_fun, *args):
	ensure_dir_exists(os.path.dirname(fn), '%s directory' % desc)
	activity = Activity(msg)
	try:
		create_tarball(sorted(set(ft_list_fun(*args))), name=fn)
	except Exception:
		raise BackendError('Could not write %s to %s.' % (desc, fn))
	activity.finish()
	return fn
