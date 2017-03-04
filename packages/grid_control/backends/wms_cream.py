# | Copyright 2016-2017 Karlsruhe Institute of Technology
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

import os, re, tempfile
from grid_control import utils
from grid_control.backends.aspect_cancel import CancelAndPurgeJobs, CancelJobsWithProcessBlind
from grid_control.backends.aspect_status import CheckInfo, CheckJobsWithProcess
from grid_control.backends.backend_tools import ChunkedExecutor, ProcessCreatorAppendArguments
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_grid import GridWMS
from grid_control.job_db import Job
from grid_control.utils.activity import Activity
from grid_control.utils.process_base import LocalProcess
from python_compat import imap, irange, md5, tarfile


class CREAMCancelJobs(CancelJobsWithProcessBlind):
	def __init__(self, config):
		CancelJobsWithProcessBlind.__init__(self, config,
			'glite-ce-job-cancel', ['--noint', '--logfile', '/dev/stderr'])


class CREAMPurgeJobs(CancelJobsWithProcessBlind):
	def __init__(self, config):
		CancelJobsWithProcessBlind.__init__(self, config,
			'glite-ce-job-purge', ['--noint', '--logfile', '/dev/stderr'])


class CREAMCheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		proc_factory = ProcessCreatorAppendArguments(config,
			'glite-ce-job-status', ['--level', '0', '--logfile', '/dev/stderr'])
		CheckJobsWithProcess.__init__(self, config, proc_factory, status_map={
			Job.ABORTED: ['ABORTED', 'CANCELLED'],
			Job.DONE: ['DONE-FAILED', 'DONE-OK'],
			Job.QUEUED: ['IDLE', 'REGISTERED'],
			Job.RUNNING: ['REALLY-RUNNING', 'RUNNING'],
			Job.UNKNOWN: ['UNKNOWN'],
			Job.WAITING: ['HELD', 'PENDING'],
		})

	def _parse(self, proc):
		job_info = {}
		for line in proc.stdout.iter(self._timeout):
			line = line.lstrip('*').strip()
			try:
				(key, value) = imap(str.strip, line.split('=', 1))
			except Exception:
				continue
			key = key.lower()
			value = value[1:-1]
			if key == 'jobid':
				yield job_info
				job_info = {CheckInfo.WMSID: value}
			elif key == 'status':
				job_info[CheckInfo.RAW_STATUS] = value
			elif value:
				job_info[key] = value
		yield job_info


class CreamWMS(GridWMS):
	alias_list = ['cream']

	def __init__(self, config, name):
		cancel_executor = CancelAndPurgeJobs(config, CREAMCancelJobs(config), CREAMPurgeJobs(config))
		GridWMS.__init__(self, config, name,
			submit_exec=utils.resolve_install_path('glite-ce-job-submit'),
			output_exec=utils.resolve_install_path('glite-ce-job-output'),
			check_executor=CREAMCheckJobs(config),
			cancel_executor=ChunkedExecutor(config, 'cancel', cancel_executor))

		self._chunk_size = config.get_int('job chunk size', 10, on_change=None)
		self._submit_args_dict.update({'-r': self._ce, '--config-vo': self._config_fn})
		self._output_regex = r'.*For JobID \[(?P<rawId>\S+)\] output will be stored' + \
			' in the dir (?P<output_dn>.*)$'

		self._use_delegate = False
		if self._use_delegate is False:
			self._submit_args_dict['-a'] = ' '

	def _make_jdl(self, jobnum, task):
		return ['[\n'] + GridWMS._make_jdl(self, jobnum, task) + [
			'OutputSandboxBaseDestUri = "gsiftp://localhost";\n]']

	# Get output of jobs and yield output dirs
	def _get_jobs_output(self, gc_id_jobnum_list):
		if len(gc_id_jobnum_list) == 0:
			raise StopIteration

		tmp_dn = os.path.join(self._path_output, 'tmp')
		try:
			if len(gc_id_jobnum_list) == 1:
				# For single jobs create single subdir
				tmp_dn = os.path.join(tmp_dn, md5(gc_id_jobnum_list[0][0]).hexdigest())
			utils.ensure_dir_exists(tmp_dn)
		except Exception:
			raise BackendError('Temporary path "%s" could not be created.' % tmp_dn, BackendError)

		activity = Activity('retrieving %d job outputs' % len(gc_id_jobnum_list))
		chunk_pos_iter = irange(0, len(gc_id_jobnum_list), self._chunk_size)
		for ids in imap(lambda x: gc_id_jobnum_list[x:x + self._chunk_size], chunk_pos_iter):
			map_gc_id2jobnum = dict(ids)
			jobs = ' '.join(self._iter_wms_ids(ids))
			log = tempfile.mktemp('.log')

			proc = LocalProcess(self._output_exec, '--noint', '--logfile', log, '--dir', tmp_dn, jobs)

			# yield output dirs
			todo = map_gc_id2jobnum.values()
			done = []
			current_jobnum = None
			for line in imap(str.strip, proc.stdout.iter(timeout=20)):
				match = re.match(self._output_regex, line)
				if match:
					current_jobnum = map_gc_id2jobnum.get(self._create_gc_id(match.groupdict()['rawId']))
					todo.remove(current_jobnum)
					done.append(match.groupdict()['rawId'])
					output_dn = match.groupdict()['output_dn']
					if os.path.exists(output_dn):
						if 'GC_WC.tar.gz' in os.listdir(output_dn):
							wildcard_tar = os.path.join(output_dn, 'GC_WC.tar.gz')
							try:
								tarfile.TarFile.open(wildcard_tar, 'r:gz').extractall(output_dn)
								os.unlink(wildcard_tar)
							except Exception:
								self._log.error('Can\'t unpack output files contained in %s', wildcard_tar)
					yield (current_jobnum, output_dn)
					current_jobnum = None
			exit_code = proc.status(timeout=10, terminate=True)

			if exit_code != 0:
				if 'Keyboard interrupt raised by user' in proc.stdout.read_log():
					utils.remove_files([log, tmp_dn])
					raise StopIteration
				else:
					self._log.log_process(proc)
				self._log.error('Trying to recover from error ...')
				for dn in os.listdir(tmp_dn):
					yield (None, os.path.join(tmp_dn, dn))
		activity.finish()

		# return unretrievable jobs
		for jobnum in todo:
			yield (jobnum, None)

		purge_log_fn = tempfile.mktemp('.log')
		purge_proc = LocalProcess(utils.resolve_install_path('glite-ce-job-purge'),
			'--noint', '--logfile', purge_log_fn, str.join(' ', done))
		exit_code = purge_proc.status(timeout=60)
		if exit_code != 0:
			if self._explain_error(purge_proc, exit_code):
				pass
			else:
				self._log.log_process(proc)
		utils.remove_files([log, purge_log_fn, tmp_dn])
