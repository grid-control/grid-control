# | Copyright 2012-2017 Karlsruhe Institute of Technology
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

# -*- coding: utf-8 -*-

import os, re, time, tempfile
from grid_control.backends.aspect_cancel import CancelAndPurgeJobs
from grid_control.backends.aspect_status import CheckJobsMissingState
from grid_control.backends.broker_base import Broker
from grid_control.backends.condor_wms.processhandler import ProcessHandler
from grid_control.backends.wms import BackendError, BasicWMS, WMS
from grid_control.backends.wms_condor import CondorCancelJobs, CondorCheckJobs
from grid_control.backends.wms_local import LocalPurgeJobs, SandboxHelper
from grid_control.utils import Result, ensure_dir_exists, get_path_share, remove_files, resolve_install_path, safe_write, split_blackwhite_list  # pylint:disable=line-too-long
from grid_control.utils.activity import Activity
from grid_control.utils.data_structures import make_enum
from python_compat import imap, irange, lmap, lzip, md5_hex


# if the ssh stuff proves too hack'y: http://www.lag.net/paramiko/
PoolType = make_enum(['LOCAL', 'SPOOL', 'SSH', 'GSISSH'])  # pylint:disable=invalid-name


class CondorJDLWriter(object):
	def __init__(self, config):
		self._email = config.get(['notifyemail', 'email'], '', on_change=None)
		self._classad_list = config.get_list(['classaddata', 'classad data'], [], on_change=None)
		self._jdl_list = config.get_list(['jdldata', 'jdl data'], [], on_change=None)
		self._pool_query_dict = config.get_dict('poolArgs query', {})[0]

	def get_jdl(self):
		jdl_str_list = []
		if self._email:
			jdl_str_list.append('notify_user = ' + self._email)
		# properly inject any information retrieval keys into ClassAds
		# regular attributes do not need injecting
		for key in self._pool_query_dict.values():
			# is this a match string? '+JOB_GLIDEIN_Entry_Name = "$$(GLIDEIN_Entry_Name:Unknown)"'
			# -> MATCH_GLIDEIN_Entry_Name = "CMS_T2_DE_RWTH_grid-ce2" &&
			#    MATCH_EXP_JOB_GLIDEIN_Entry_Name = "CMS_T2_DE_RWTH_grid-ce2"
			key_match = re.match('(?:MATCH_EXP_JOB_|MATCH_|JOB_)(.*)', key).groups()[0]
			if key_match:
				jdl_str_list.append('+JOB_%s = "$$(%s:Unknown)"' % (key_match, key_match))
		jdl_str_list.extend(imap(lambda classad: '+' + classad, self._classad_list))
		jdl_str_list.extend(self._jdl_list)
		return jdl_str_list


class Condor(BasicWMS):
	config_section_list = BasicWMS.config_section_list + ['condor']

	def __init__(self, config, name):
		self._sandbox_helper = SandboxHelper(config)
		self._error_log_fn = config.get_work_path('error.tar')
		cancel_executor = CancelAndPurgeJobs(config, CondorCancelJobs(config),
				LocalPurgeJobs(config, self._sandbox_helper))
		BasicWMS.__init__(self, config, name,
			check_executor=CheckJobsMissingState(config, CondorCheckJobs(config)),
			cancel_executor=cancel_executor)
		self._task_id = config.get('task id', md5_hex(str(time.time())), persistent=True)  # FIXME
		# finalize config state by reading values or setting to defaults
		# load keys for condor pool ClassAds
		self._jdl_writer = CondorJDLWriter(config)
		self._universe = config.get('universe', 'vanilla', on_change=None)
		self._pool_req_dict = config.get_dict('poolArgs req', {})[0]
		self._pool_work_dn = None
		self._proc_factory = None
		(self._submit_exec, self._transfer_exec) = (None, None)
		# prepare interfaces for local/remote/ssh pool access
		self._remote_type = config.get_enum('remote Type', PoolType, PoolType.LOCAL)
		self._init_pool_interface(config)
		# Sandbox base path where individual job data is stored, staged and returned to
		self._sandbox_dn = config.get_path('sandbox path',
			config.get_work_path('sandbox'), must_exist=False)
		# broker for selecting sites - FIXME: this looks wrong... pool != site
		self._pool_host_list = config.get_list(['poolhostlist', 'pool host list'], [])
		self._broker_site = config.get_plugin('site broker', 'UserBroker', cls=Broker,
			bind_kwargs={'tags': [self]}, pargs=('sites', 'sites', lambda: self._pool_host_list))

	def get_interval_info(self):
		# overwrite for check/submit/fetch intervals
		if self._remote_type in (PoolType.SSH, PoolType.GSISSH):
			return Result(wait_on_idle=30, wait_between_steps=5)
		elif self._remote_type == PoolType.SPOOL:
			return Result(wait_on_idle=60, wait_between_steps=10)
		else:
			return Result(wait_on_idle=20, wait_between_steps=5)

	def submit_jobs(self, jobnum_list, task):
		submit_chunk_size = 25
		for chunk_pos in irange(0, len(jobnum_list), submit_chunk_size):
			for result in self._submit_jobs(jobnum_list[chunk_pos:chunk_pos + submit_chunk_size], task):
				yield result

	def _check_and_log_proc(self, proc):
		if proc.wait() != 0:
			if not self._explain_error(proc, proc.wait()):
				proc.log_error(self._error_log_fn, brief=True)

	def _cleanup_remote_output_dn(self):
		# active remote submission should clean up when no jobs remain
		if self._remote_type in (PoolType.SSH, PoolType.GSISSH):
			activity = Activity('clearing remote work directory')
			# check whether there are any remote working directories remaining
			check_proc = self._proc_factory.logged_execute(
				'find %s -maxdepth 1 -type d | wc -l' % self._get_remote_output_dn())
			try:
				if int(check_proc.get_output()) <= 1:
					cleanup_cmd = 'rm -rf %s' % self._get_remote_output_dn()
					cleanup_proc = self._proc_factory.logged_execute(cleanup_cmd)
					if cleanup_proc.wait() != 0:
						if self._explain_error(cleanup_proc, cleanup_proc.wait()):
							return
						cleanup_proc.log_error(self._error_log_fn)
						raise BackendError('Cleanup process %s returned: %s' % (
							cleanup_proc.cmd, cleanup_proc.get_message()))
			except Exception:
				self._log.warning('There might be some junk data left in: %s @ %s',
					self._get_remote_output_dn(), self._proc_factory.get_domain())
				raise BackendError('Unable to clean up remote working directory')
			activity.finish()

	def _explain_error(self, proc, code):
		if 'Keyboard interrupt raised by user' in proc.get_error():
			return True
		return False

	def _get_dataset_fn_list(self, jobnum, task):
		# TODO: Replace with a dedicated PartitionProcessor to split HDPA file lists.
		# as per ``formatFileList``
		# UserMod filelists are space separated 'File1 File2 File3'
		# CMSSW filelists are individually quoted and comma+space separated '"File1", "File2", "File3"'
		fn_list = task.get_job_dict(jobnum).get('FILE_NAMES', '').strip()
		if '", "' in fn_list:  # CMSSW style
			fn_list = fn_list.strip('"').split('", "')
		else:  # UserMod style
			fn_list = fn_list.split(' ')

		if len(fn_list) > 1 or len(fn_list[0]) > 1:
			data_file = os.path.join(self._get_sandbox_dn(jobnum), 'job_%d_files.txt' % jobnum)
			fp_data_list = open(data_file, 'w')
			try:
				fp_data_list.writelines(lmap(lambda line: line + '\n', fn_list))
			finally:
				fp_data_list.close()
			return ['%s = "%s"' % (self._pool_req_dict['dataFiles'], data_file)]
		return []

	def _get_dest(self, config):
		# read user/sched/collector from config
		user = config.get('remote user', '')
		dest = config.get('remote dest', '@')
		dest_part_list = lmap(str.strip, dest.split('@'))
		if len(dest_part_list) == 1:
			return (user or None, dest_part_list[0], None)
		elif len(dest_part_list) == 2:
			return (user or None, dest_part_list[0], dest_part_list[1])
		else:
			self._log.warning('Could not parse Configuration setting \'remote dest\'!')
			self._log.warning('Expected: [<sched>|<sched>@|<sched>@<collector>]')
			self._log.warning('Found: %s', dest)
			raise BackendError('Could not parse submit destination')

	def _get_jdl_req_str_list(self, jobnum, task):
		# helper for converting GC requirements to Condor requirements
		jdl_req_str_list = []

		def _add_list_classad(classad_name, value):
			if classad_name in self._pool_req_dict:
				classad_str = self._pool_req_dict[classad_name]
				jdl_req_str_list.append('%s = "%s"' % (classad_str, str.join(', ', value)))

		# get requirements from task and broker WMS sites
		req_list = self._broker_site.broker(task.get_requirement_list(jobnum), WMS.SITES)
		for req_type, req_value in req_list:
			if req_type == WMS.SITES:
				(blacklist, whitelist) = split_blackwhite_list(req_value[1])
				_add_list_classad('blacklistSite', blacklist)
				_add_list_classad('whitelistSite', whitelist)
			elif req_type == WMS.WALLTIME:
				if ('walltimeMin' in self._pool_req_dict) and (req_value > 0):
					jdl_req_str_list.append('%s = %d' % (self._pool_req_dict['walltimeMin'], req_value))
			elif (req_type == WMS.STORAGE) and req_value:
				_add_list_classad('requestSEs', req_value)
			elif (req_type == WMS.MEMORY) and (req_value > 0):
				jdl_req_str_list.append('request_memory = %dM' % req_value)
			elif (req_type == WMS.CPUS) and (req_value > 0):
				jdl_req_str_list.append('request_cpus = %d' % req_value)
			# TODO: GLIDEIN_REQUIRE_GLEXEC_USE, WMS.SOFTWARE

		# (HPDA) file location service
		if 'dataFiles' in self._pool_req_dict:
			jdl_req_str_list.extend(self._get_dataset_fn_list(jobnum, task))
		return jdl_req_str_list

	def _get_jdl_str_list(self, jobnum_list, task):
		(script_cmd, sb_in_fn_list) = self._get_script_and_fn_list(task)
		# header for all jobs
		jdl_str_list = [
			'Universe = ' + self._universe,
			'Executable = ' + script_cmd,
		]
		jdl_str_list.extend(self._jdl_writer.get_jdl())
		jdl_str_list.extend([
			'Log = ' + os.path.join(self._get_remote_output_dn(), 'GC_Condor.%s.log') % self._task_id,
			'should_transfer_files = YES',
			'when_to_transfer_output = ON_EXIT',
			'transfer_executable = false',
		])
		# cancel held jobs - ignore spooling ones
		remove_cond = '(JobStatus == 5 && HoldReasonCode != 16)'
		jdl_str_list.append('periodic_remove = (%s)' % remove_cond)

		if self._remote_type == PoolType.SPOOL:
			jdl_str_list.extend([
				# remote submissal requires job data to stay active until retrieved
				'leave_in_queue = (JobStatus == 4) && ' +
				'((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0))',
				# Condor should not attempt to assign to local user
				'+Owner=UNDEFINED'
			])

		for auth_fn in self._token.get_auth_fn_list():
			if self._remote_type not in (PoolType.SSH, PoolType.GSISSH):
				jdl_str_list.append('x509userproxy = %s' % auth_fn)
			else:
				jdl_str_list.append('x509userproxy = %s' % os.path.join(
					self._get_remote_output_dn(), os.path.basename(auth_fn)))

		# job specific data
		for jobnum in jobnum_list:
			jdl_str_list.extend(self._get_jdl_str_list_job(jobnum, task, sb_in_fn_list))

		# combine JDL and add line breaks
		return lmap(lambda line: line + '\n', jdl_str_list)

	def _get_jdl_str_list_job(self, jobnum, task, sb_in_fn_list):
		workdir = self._get_remote_output_dn(jobnum)
		sb_out_fn_list = []
		for (_, src, target) in self._get_out_transfer_info_list(task):
			if src not in ('gc.stdout', 'gc.stderr'):
				sb_out_fn_list.append(target)
		job_sb_in_fn_list = sb_in_fn_list + [os.path.join(workdir, 'job_%d.var' % jobnum)]
		jdl_str_list = [
			# store matching Grid-Control and Condor ID
			'+GridControl_GCtoWMSID = "%s@$(Cluster).$(Process)"' % task.get_description(jobnum).job_name,
			'+GridControl_GCIDtoWMSID = "%s@$(Cluster).$(Process)"' % jobnum,
			# publish the WMS id for Dashboard
			'environment = CONDOR_WMS_DASHID=https://%s:/$(Cluster).$(Process)' % self._name,
			# condor doesn"t execute the job directly. actual job data, files and arguments
			# are accessed by the GC scripts (but need to be copied to the worker)
			'transfer_input_files = ' + str.join(', ', job_sb_in_fn_list),
			# only copy important files - stdout and stderr get remapped but transferred
			# automatically, so don't request them as they would not be found
			'transfer_output_files = ' + str.join(', ', sb_out_fn_list),
			'initialdir = ' + workdir,
			'Output = ' + os.path.join(workdir, "gc.stdout"),
			'Error = ' + os.path.join(workdir, "gc.stderr"),
			'arguments = %s ' % jobnum
		]
		jdl_str_list.extend(self._get_jdl_req_str_list(jobnum, task))
		jdl_str_list.append('Queue\n')
		return jdl_str_list

	def _get_jobs_output(self, gc_id_jobnum_list):
		# retrieve task output files from sandbox directory
		if not len(gc_id_jobnum_list):
			raise StopIteration

		activity = Activity('retrieving job outputs')
		for gc_id, jobnum in gc_id_jobnum_list:
			sandpath = self._get_sandbox_dn(jobnum)
			if sandpath is None:
				yield (jobnum, None)
				continue
			# when working with a remote spool schedd, tell condor to return files
			if self._remote_type == PoolType.SPOOL:
				self._check_and_log_proc(self._proc_factory.logged_execute(
					self._transfer_exec, self._split_gc_id(gc_id)[1]))
			# when working with a remote [gsi]ssh schedd, manually return files
			elif self._remote_type in (PoolType.SSH, PoolType.GSISSH):
				self._check_and_log_proc(self._proc_factory.logged_copy_from_remote(
					self._get_remote_output_dn(jobnum), self._get_sandbox_dn()))
				# clean up remote working directory
				self._check_and_log_proc(self._proc_factory.logged_execute(
					'rm -rf %s' % self._get_remote_output_dn(jobnum)))
			yield (jobnum, sandpath)
		# clean up if necessary
		activity.finish()
		self._cleanup_remote_output_dn()

	def _get_remote_output_dn(self, jobnum=''):
		# return path to condor output dir for a specific job or basepath
		if self._remote_type in (PoolType.LOCAL, PoolType.SPOOL):
			return self._get_sandbox_dn(jobnum)
		else:
			# ssh and gsissh require a remote working directory
			remote_dn = os.path.join(self._pool_work_dn,
				'GCRemote.work.TaskID.' + self._task_id, str(jobnum), '')
			mkdir_proc = self._proc_factory.logged_execute('mkdir -p', remote_dn)
			if mkdir_proc.wait() == 0:
				return remote_dn
			if self._explain_error(mkdir_proc, mkdir_proc.wait()):
				return
			mkdir_proc.log_error(self._error_log_fn)
			raise BackendError('Error accessing or creating remote working directory!\n%s' % remote_dn)

	def _get_sandbox_dn(self, jobnum=''):
		# return path to sandbox for a specific job or basepath
		sandpath = os.path.join(self._sandbox_dn, str(jobnum), '')
		return ensure_dir_exists(sandpath, 'sandbox directory', BackendError)

	def _get_script_and_fn_list(self, task):
		# resolve file paths for different pool types
		# handle gc executable separately
		(script_cmd, sb_in_fn_list) = ('', [])
		if self._remote_type in (PoolType.SSH, PoolType.GSISSH):
			for target in imap(lambda d_s_t: d_s_t[2], self._get_in_transfer_info_list(task)):
				if 'gc-run.sh' in target:
					script_cmd = os.path.join(self._get_remote_output_dn(), target)
				else:
					sb_in_fn_list.append(os.path.join(self._get_remote_output_dn(), target))
		else:
			for source in imap(lambda d_s_t: d_s_t[1], self._get_in_transfer_info_list(task)):
				if 'gc-run.sh' in source:
					script_cmd = source
				else:
					sb_in_fn_list.append(source)
		if self._universe.lower() == 'docker':
			script_cmd = './gc-run.sh'
			sb_in_fn_list.append(get_path_share('gc-run.sh'))
		return (script_cmd, sb_in_fn_list)

	def _init_pool_interface(self, config):
		# prepare commands and interfaces according to selected submit type
		# remote submissal requires different access to Condor tools
		# local : remote == ''          => condor_q job.jdl
		# remote: remote == <pool>      => condor_q -remote <pool> job.jdl
		# ssh   : remote == <user@pool> => ssh <user@pool> 'condor_q job.jdl'
		(user, sched, collector) = self._get_dest(config)
		if self._remote_type in (PoolType.LOCAL, PoolType.SPOOL):
			self._init_pool_interface_local(config, sched, collector)
		else:
			# ssh type instructions are passed to the remote host via regular ssh/gsissh
			if user:
				host = '%s@%s' % (user, sched)
			else:
				host = sched
			self._init_pool_interface_remote(config, sched, collector, host)

	def _init_pool_interface_local(self, config, sched, collector):
		# submission might spool to another schedd and need to fetch output
		self._submit_exec = resolve_install_path('condor_submit')
		self._transfer_exec = resolve_install_path('condor_transfer_data')
		if self._remote_type == PoolType.SPOOL:
			if sched:
				self._submit_exec += ' -remote %s' % sched
				self._transfer_exec += ' -name %s' % sched
			if collector:
				self._submit_exec += ' -pool %s' % collector
				self._transfer_exec += ' -pool %s' % collector
		self._proc_factory = ProcessHandler.create_instance('LocalProcessHandler')

	def _init_pool_interface_remote(self, config, sched, collector, host):
		if self._remote_type == PoolType.SSH:
			self._proc_factory = ProcessHandler.create_instance('SSHProcessHandler',
				remote_host=host, sshLink=config.get_work_path('.ssh', self._name + host))
		else:
			self._proc_factory = ProcessHandler.create_instance('GSISSHProcessHandler',
				remote_host=host, sshLink=config.get_work_path('.gsissh', self._name + host))
		# ssh type instructions rely on commands being available on remote pool
		self._submit_exec = 'condor_submit'
		self._transfer_exec = 'false'  # disabled for this type
		# test availability of commands
		version_proc = self._proc_factory.logged_execute('condor_version')
		if version_proc.wait() != 0:
			version_proc.log_error(self._error_log_fn)
			raise BackendError('Failed to access remote Condor tools! ' +
				'The pool you are submitting to is very likely not configured properly.')
		# get initial workdir on remote pool
		remote_work_dn = config.get('remote workdir', '')
		if remote_work_dn:
			remote_user_name = self._proc_factory.logged_execute('whoami').get_output().strip()
			self._pool_work_dn = os.path.join(remote_work_dn, remote_user_name)
			remote_dn_proc = self._proc_factory.logged_execute('mkdir -p %s' % self._pool_work_dn)
		else:
			remote_dn_proc = self._proc_factory.logged_execute('pwd')
			self._pool_work_dn = remote_dn_proc.get_output().strip()
		if remote_dn_proc.wait() != 0:
			self._log.critical('Code: %d\nOutput Message: %s\nError Message: %s',
				remote_dn_proc.wait(), remote_dn_proc.get_output(), remote_dn_proc.get_error())
			raise BackendError('Failed to determine, create or verify base work directory on remote host')

	def _submit_jobs(self, jobnum_list, task):
		# submit_jobs: Submit a number of jobs and yield (jobnum, WMS ID, other data) sequentially
		# >>jobnum: internal ID of the Job
		# JobNum is linked to the actual *task* here
		(jdl_fn, submit_jdl_fn) = self._submit_jobs_prepare(jobnum_list, task)
		try:
			# submit all jobs simultaneously and temporarily store verbose (ClassAdd) output
			activity = Activity('queuing jobs at scheduler')
			proc = self._proc_factory.logged_execute(self._submit_exec, ' -verbose ' + submit_jdl_fn)

			# extract the Condor ID (WMS ID) of the jobs from output ClassAds
			jobnum_gc_id_list = []
			for line in proc.iter():
				if 'GridControl_GCIDtoWMSID' in line:
					jobnum_wms_id = line.split('=')[1].strip(' "\n').split('@')
					jobnum, wms_id = int(jobnum_wms_id[0]), jobnum_wms_id[1].strip()
					# Condor creates a default job then overwrites settings on any subsequent job
					# i.e. skip every second, but better be sure
					if (not jobnum_gc_id_list) or (jobnum not in lzip(*jobnum_gc_id_list)[0]):
						jobnum_gc_id_list.append((jobnum, self._create_gc_id(wms_id)))

			exit_code = proc.wait()
			activity.finish()
			if (exit_code != 0) or (len(jobnum_gc_id_list) < len(jobnum_list)):
				if not self._explain_error(proc, exit_code):
					self._log.error('Submitted %4d jobs of %4d expected',
						len(jobnum_gc_id_list), len(jobnum_list))
					proc.log_error(self._error_log_fn, jdl=jdl_fn)
		finally:
			remove_files([jdl_fn])

		for (jobnum, gc_id) in jobnum_gc_id_list:
			yield (jobnum, gc_id, {})

	def _submit_jobs_prepare(self, jobnum_list, task):
		activity = Activity('preparing jobs')
		jdl_fn = self._write_jdl(jobnum_list, task)

		# create the _jobconfig.sh file containing the actual data
		for jobnum in jobnum_list:
			try:
				job_var_fn = os.path.join(self._get_sandbox_dn(jobnum), 'job_%d.var' % jobnum)
				self._write_job_config(job_var_fn, jobnum, task, {})
			except Exception:
				raise BackendError('Could not write _jobconfig data for %s.' % jobnum)

		# copy infiles to ssh/gsissh remote pool if required
		submit_jdl_fn = jdl_fn
		if self._remote_type in (PoolType.SSH, PoolType.GSISSH):
			activity_remote = Activity('preparing remote scheduler')
			remote_output_dn = self._get_remote_output_dn()
			# TODO: check whether shared remote files already exist and copy otherwise
			for _, source_fn, target_fn in self._get_in_transfer_info_list(task):
				self._check_and_log_proc(self._proc_factory.logged_copy_to_remote(source_fn,
					os.path.join(remote_output_dn, target_fn)))
			# copy job config files
			for jobnum in jobnum_list:
				self._check_and_log_proc(self._proc_factory.logged_copy_to_remote(
					os.path.join(self._get_sandbox_dn(jobnum), 'job_%d.var' % jobnum),
					os.path.join(self._get_remote_output_dn(jobnum), 'job_%d.var' % jobnum)))
			# copy jdl
			submit_jdl_fn = os.path.join(remote_output_dn, os.path.basename(jdl_fn))
			self._check_and_log_proc(self._proc_factory.logged_copy_to_remote(jdl_fn, submit_jdl_fn))
			# copy proxy
			for auth_fn in self._token.get_auth_fn_list():
				self._check_and_log_proc(self._proc_factory.logged_copy_to_remote(auth_fn,
					os.path.join(self._get_remote_output_dn(), os.path.basename(auth_fn))))
			activity_remote.finish()
		activity.finish()
		return (jdl_fn, submit_jdl_fn)

	def _write_jdl(self, jobnum_list, task):
		# construct a temporary JDL for this batch of jobs
		jdl_fd, jdl_fn = tempfile.mkstemp(suffix='.jdl')
		try:
			data = self._get_jdl_str_list(jobnum_list, task)
			safe_write(os.fdopen(jdl_fd, 'w'), data)
		except Exception:
			remove_files([jdl_fn])
			raise BackendError('Could not write jdl data to %s.' % jdl_fn)
		return jdl_fn
