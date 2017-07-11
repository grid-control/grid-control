# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import os, sys, calendar, tempfile
from grid_control.backends.aspect_cancel import CancelJobsWithProcess
from grid_control.backends.aspect_status import CheckInfo, CheckJobsWithProcess
from grid_control.backends.backend_tools import ProcessCreatorViaStdin, unpack_wildcard_tar
from grid_control.backends.broker_base import Broker
from grid_control.backends.jdl_writer import JDLWriter
from grid_control.backends.wms import BackendError, BasicWMS, WMS
from grid_control.job_db import Job
from grid_control.utils import ensure_dir_exists, remove_files, resolve_install_path, safe_write
from grid_control.utils.activity import Activity
from grid_control.utils.algos import filter_dict
from grid_control.utils.file_tools import SafeFile
from grid_control.utils.process_base import LocalProcess
from grid_control.utils.user_interface import UserInputInterface
from hpfwk import clear_current_exception
from python_compat import identity, ifilter, imap, lfilter, lmap, md5_hex, parsedate


class GridWMS(BasicWMS):
	config_section_list = BasicWMS.config_section_list + ['grid']
	grid_status_map = {  # FIXME: mismatch between CheckJobs API and GliteWMSDirect usage!
		Job.ABORTED: ['aborted', 'cancelled', 'cleared', 'failed'],
		Job.DONE: ['done'],
		Job.QUEUED: ['scheduled', 'queued'],
		Job.READY: ['ready'],
		Job.RUNNING: ['running'],
		Job.SUBMITTED: ['submitted'],
		Job.WAITING: ['waiting'],
	}

	def __init__(self, config, name, submit_exec, output_exec,
			check_executor, cancel_executor, jdl_writer=None):
		config.set('access token', 'VomsProxy')
		BasicWMS.__init__(self, config, name,
			check_executor=check_executor, cancel_executor=cancel_executor)

		self._broker_site = config.get_plugin('site broker', 'UserBroker',
			cls=Broker, bind_kwargs={'tags': [self]}, pargs=('sites', 'sites', self._get_site_list))
		self._vo = config.get('vo', self._token.get_group())

		self._submit_exec = submit_exec
		self._submit_args_dict = {}
		self._output_exec = output_exec
		self._ce = config.get('ce', '', on_change=None)
		self._config_fn = config.get_path('config', '', on_change=None)
		self._sb_warn_size = config.get_int('warn sb size', 5, on_change=None)
		self._job_dn = config.get_work_path('jobs')
		self._jdl_writer = jdl_writer or JDLWriter()

	def _explain_error(self, proc, code):
		if 'Keyboard interrupt raised by user' in proc.stderr.read_log():
			return True
		return False

	def _get_jobs_output(self, gc_id_jobnum_list):
		# Get output of jobs and yield output dirs
		if len(gc_id_jobnum_list) == 0:
			raise StopIteration

		root_dn = os.path.join(self._path_output, 'tmp')
		try:
			if len(gc_id_jobnum_list) == 1:
				# For single jobs create single subdir
				tmp_dn = os.path.join(root_dn, md5_hex(gc_id_jobnum_list[0][0]))
			else:
				tmp_dn = root_dn
			ensure_dir_exists(tmp_dn)
		except Exception:
			raise BackendError('Temporary path "%s" could not be created.' % tmp_dn, BackendError)

		map_gc_id2jobnum = dict(gc_id_jobnum_list)
		jobs = self._write_wms_id_list(gc_id_jobnum_list)

		activity = Activity('retrieving %d job outputs' % len(gc_id_jobnum_list))
		proc = LocalProcess(self._output_exec, '--noint',
			'--logfile', '/dev/stderr', '-i', jobs, '--dir', tmp_dn)

		# yield output dirs
		todo = map_gc_id2jobnum.values()
		current_jobnum = None
		for line in imap(str.strip, proc.stdout.iter(timeout=60)):
			if line.startswith(tmp_dn):
				todo.remove(current_jobnum)
				output_dn = line.strip()
				unpack_wildcard_tar(self._log, output_dn)
				yield (current_jobnum, output_dn)
				current_jobnum = None
			else:
				current_jobnum = map_gc_id2jobnum.get(self._create_gc_id(line), current_jobnum)
		exit_code = proc.status(timeout=0, terminate=True)
		activity.finish()

		if exit_code != 0:
			if 'Keyboard interrupt raised by user' in proc.stderr.read(timeout=0):
				remove_files([jobs, root_dn])
				raise StopIteration
			else:
				self._log.log_process(proc, files={'jobs': SafeFile(jobs).read()})
			self._log.error('Trying to recover from error ...')
			for dn in os.listdir(root_dn):
				yield (None, os.path.join(root_dn, dn))

		# return unretrievable jobs
		for jobnum in todo:
			yield (jobnum, None)

		remove_files([jobs, tmp_dn])

	def _get_site_list(self):
		return None

	def _make_jdl(self, jobnum, task):
		job_config_fn = os.path.join(self._job_dn, 'job_%d.var' % jobnum)
		sb_in_src_list = lmap(lambda d_s_t: d_s_t[1], self._get_in_transfer_info_list(task))
		sb_out_target_list = lmap(lambda d_s_t: d_s_t[2], self._get_out_transfer_info_list(task))
		wildcard_list = lfilter(lambda x: '*' in x, sb_out_target_list)
		if len(wildcard_list):
			self._write_job_config(job_config_fn, jobnum, task, {'GC_WC': str.join(' ', wildcard_list)})
			sb_out_fn_list = lfilter(lambda x: x not in wildcard_list, sb_out_target_list) + ['GC_WC.tar.gz']
		else:
			self._write_job_config(job_config_fn, jobnum, task, {})
			sb_out_fn_list = sb_out_target_list
		# Warn about too large sandboxes
		sb_in_size_list = lmap(os.path.getsize, sb_in_src_list)
		if sb_in_size_list:
			sb_in_size = sum(sb_in_size_list)
			if (self._sb_warn_size > 0) and (sb_in_size > self._sb_warn_size * 1024 * 1024):
				user_msg = 'Sandbox is very large (%d bytes) and can cause issues with the WMS!' % sb_in_size
				user_msg += ' Do you want to continue?'
				if not UserInputInterface().prompt_bool(user_msg, False):
					sys.exit(os.EX_OK)
				self._sb_warn_size = 0

		reqs = self._broker_site.broker(task.get_requirement_list(jobnum), WMS.SITES)

		def _format_str_list(str_list):
			return '{ %s }' % str.join(', ', imap(lambda x: '"%s"' % x, str_list))

		contents = {
			'Executable': '"gc-run.sh"',
			'Arguments': '"%d"' % jobnum,
			'StdOutput': '"gc.stdout"',
			'StdError': '"gc.stderr"',
			'InputSandbox': _format_str_list(sb_in_src_list + [job_config_fn]),
			'OutputSandbox': _format_str_list(sb_out_fn_list),
			'VirtualOrganisation': '"%s"' % self._vo,
			'Rank': '-other.GlueCEStateEstimatedResponseTime',
			'RetryCount': 2
		}
		return self._jdl_writer.format(reqs, contents)

	def _submit_job(self, jobnum, task):
		# Submit job and yield (jobnum, WMS ID, other data)
		jdl_fd, jdl_fn = tempfile.mkstemp('.jdl')
		try:
			jdl_line_list = self._make_jdl(jobnum, task)
			safe_write(os.fdopen(jdl_fd, 'w'), jdl_line_list)
		except Exception:
			remove_files([jdl_fn])
			raise BackendError('Could not write jdl data to %s.' % jdl_fn)

		try:
			submit_arg_list = []
			for key_value in filter_dict(self._submit_args_dict, value_filter=identity).items():
				submit_arg_list.extend(key_value)
			submit_arg_list.append(jdl_fn)

			activity = Activity('submitting job %d' % jobnum)
			proc = LocalProcess(self._submit_exec, '--nomsg', '--noint',
				'--logfile', '/dev/stderr', *submit_arg_list)

			wms_id = None
			stripped_stdout_iter = imap(str.strip, proc.stdout.iter(timeout=60))
			for line in ifilter(lambda x: x.startswith('http'), stripped_stdout_iter):
				wms_id = line
			exit_code = proc.status(timeout=0, terminate=True)

			activity.finish()

			if (exit_code != 0) or (wms_id is None):
				if self._explain_error(proc, exit_code):
					pass
				else:
					self._log.log_process(proc, files={'jdl': SafeFile(jdl_fn).read()})
		finally:
			remove_files([jdl_fn])
		job_data = {'jdl': str.join('', jdl_line_list)}
		return (jobnum, self._create_gc_id(wms_id), job_data)

	def _write_wms_id_list(self, gc_id_jobnum_list):
		try:
			job_fd, job_fn = tempfile.mkstemp('.jobids')
			safe_write(os.fdopen(job_fd, 'w'), str.join('\n', self._iter_wms_ids(gc_id_jobnum_list)))
		except Exception:
			raise BackendError('Could not write wms ids to %s.' % job_fn)
		return job_fn


class GridCancelJobs(CancelJobsWithProcess):
	def __init__(self, config, cancel_exec):
		proc_factory = GridProcessCreator(config, cancel_exec,
			['--noint', '--logfile', '/dev/stderr', '-i', '/dev/stdin'])
		CancelJobsWithProcess.__init__(self, config, proc_factory)

	def _parse(self, wms_id_list, proc):  # yield list of (wms_id, job_status)
		for line in ifilter(lambda x: x.startswith('- '), proc.stdout.iter(self._timeout)):
			yield (line.strip('- \n'),)


class GridCheckJobs(CheckJobsWithProcess):
	def __init__(self, config, check_exec):
		proc_factory = GridProcessCreator(config, check_exec,
			['--verbosity', 1, '--noint', '--logfile', '/dev/stderr', '-i', '/dev/stdin'])
		CheckJobsWithProcess.__init__(self, config, proc_factory, status_map=GridWMS.grid_status_map)

	def _fill(self, job_info, key, value):
		if key.startswith('current status'):
			if 'failed' in value:
				value = 'failed'
			job_info[CheckInfo.RAW_STATUS] = value.split('(')[0].split()[0].lower()
		elif key.startswith('destination'):
			try:
				dest_info = value.split('/', 1)
				job_info[CheckInfo.SITE] = dest_info[0].strip()
				job_info[CheckInfo.QUEUE] = dest_info[1].strip()
			except Exception:
				return clear_current_exception()
		elif key.startswith('status reason'):
			job_info['reason'] = value
		elif key.startswith('reached') or key.startswith('submitted'):
			try:
				job_info['timestamp'] = int(calendar.timegm(parsedate(value)))
			except Exception:
				return clear_current_exception()
		elif key.startswith('bookkeeping information'):
			return
		elif value:
			job_info[key] = value

	def _handle_error(self, proc):
		self._filter_proc_log(proc, self._errormsg, discard_list=['Keyboard interrupt raised by user'])

	def _parse(self, proc):
		job_info = {}
		discard = False
		for line in proc.stdout.iter(self._timeout):
			if discard or ('log file created' in line.lower()):
				discard = True
				continue
			try:
				(key, value) = imap(str.strip, line.split(':', 1))
			except Exception:
				clear_current_exception()
				continue

			key = key.lower()
			if key.startswith('status info'):
				yield job_info
				job_info = {CheckInfo.WMSID: value}
			else:
				self._fill(job_info, key, value)
		yield job_info


class GridProcessCreator(ProcessCreatorViaStdin):
	def __init__(self, config, cmd, args):
		ProcessCreatorViaStdin.__init__(self, config)
		(self._cmd, self._args) = (resolve_install_path(cmd), args)

	def _arguments(self):
		return [self._cmd] + self._args

	def _stdin_message(self, wms_id_list):
		return str.join('\n', wms_id_list)
