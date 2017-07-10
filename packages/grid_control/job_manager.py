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

import time, bisect, random, logging
from grid_control.config import ConfigError
from grid_control.event_base import LocalEventHandler
from grid_control.gc_plugin import NamedPlugin
from grid_control.job_db import Job, JobClass, JobDB, JobError
from grid_control.job_selector import AndJobSelector, ClassSelector, JobSelector
from grid_control.output_processor import TaskOutputProcessor
from grid_control.report import Report
from grid_control.utils import abort, wait
from grid_control.utils.file_tools import SafeFile, with_file
from grid_control.utils.user_interface import UserInputInterface
from hpfwk import clear_current_exception
from python_compat import ifilter, imap, izip, lfilter, lmap, set, sorted


class JobManager(NamedPlugin):  # pylint:disable=too-many-instance-attributes
	config_section_list = NamedPlugin.config_section_list + ['jobs']
	config_tag_name = 'jobmgr'
	alias_list = ['NullJobManager']

	def __init__(self, config, name, task):
		NamedPlugin.__init__(self, config, name)
		self._local_event_handler = config.get_composited_plugin(
			['local monitor', 'local event handler'], 'logmonitor', 'MultiLocalEventHandler',
			cls=LocalEventHandler, bind_kwargs={'tags': [self, task]}, pargs=(task,),
			require_plugin=False, on_change=None)
		self._local_event_handler = self._local_event_handler or LocalEventHandler(None, '', None)
		self._log = logging.getLogger('jobs.manager')

		self._njobs_limit = config.get_int('jobs', -1, on_change=None)
		self._njobs_inflight = config.get_int('in flight', -1, on_change=None)
		self._njobs_inqueue = config.get_int('in queue', -1, on_change=None)

		self._chunks_enabled = config.get_bool('chunks enabled', True, on_change=None)
		self._chunks_submit = config.get_int('chunks submit', 100, on_change=None)
		self._chunks_check = config.get_int('chunks check', 100, on_change=None)
		self._chunks_retrieve = config.get_int('chunks retrieve', 100, on_change=None)

		self._timeout_unknown = config.get_time('unknown timeout', -1, on_change=None)
		self._timeout_queue = config.get_time('queue timeout', -1, on_change=None)
		self._job_retries = config.get_int('max retry', -1, on_change=None)

		selected = JobSelector.create(config.get('selected', '', on_change=None), task=task)
		self.job_db = config.get_plugin('job database', 'TextFileJobDB',
			cls=JobDB, pargs=(self._get_max_jobs(task), selected), on_change=None)
		self._disabled_jobs_logfile = config.get_work_path('disabled')
		self._output_processor = config.get_plugin('output processor', 'SandboxProcessor',
			cls=TaskOutputProcessor)

		self._uii = UserInputInterface()
		self._interactive_delete = config.is_interactive('delete jobs', True)
		self._interactive_reset = config.is_interactive('reset jobs', True)
		self._do_shuffle = config.get_bool('shuffle', False, on_change=None)
		self._abort_report = config.get_plugin('abort report', 'LocationReport',
			cls=Report, pargs=(self.job_db, task), on_change=None)
		self._show_blocker = True
		self._callback_list = []

	def add_event_handler(self, callback):
		self._callback_list.append(callback)

	def cancel(self, wms, jobnum_list, interactive, show_jobs):
		if len(jobnum_list) == 0:
			return
		if show_jobs:
			self._abort_report.show_report(self.job_db, jobnum_list)
		if interactive and not self._uii.prompt_bool('Do you really want to cancel these jobs?', True):
			return

		def _mark_cancelled(jobnum):
			job_obj = self.job_db.get_job(jobnum)
			if job_obj is not None:
				self._update(job_obj, jobnum, Job.CANCELLED)
				self._local_event_handler.on_job_update(wms, job_obj, jobnum, {'reason': 'cancelled'})

		jobnum_list.reverse()
		map_gc_id2jobnum = self._get_map_gc_id_jobnum(jobnum_list)
		gc_id_list = sorted(map_gc_id2jobnum, key=lambda gc_id: -map_gc_id2jobnum[gc_id])
		for (gc_id,) in wms.cancel_jobs(gc_id_list):
			# Remove deleted job from todo list and mark as cancelled
			_mark_cancelled(map_gc_id2jobnum.pop(gc_id))

		if map_gc_id2jobnum:
			jobnum_list = list(map_gc_id2jobnum.values())
			self._log.warning('There was a problem with cancelling the following jobs:')
			self._abort_report.show_report(self.job_db, jobnum_list)
			if (not interactive) or self._uii.prompt_bool('Do you want to mark them as cancelled?', True):
				lmap(_mark_cancelled, jobnum_list)
		if interactive:
			wait(2)

	def check(self, task, wms):
		jobnum_list = self._sample(self.job_db.get_job_list(ClassSelector(JobClass.PROCESSING)),
			self._get_chunk_size(self._chunks_check))

		# Check jobs in the jobnum_list and return changes, timeouts and successfully reported jobs
		(change, jobnum_list_timeout, reported) = self._check_get_jobnum_list(wms, jobnum_list)
		unreported = len(jobnum_list) - len(reported)
		if unreported > 0:
			self._log.log_time(logging.CRITICAL, '%d job(s) did not report their status!', unreported)
		if change is None:  # neither True or False => abort
			return False

		# Cancel jobs which took too long
		if len(jobnum_list_timeout):
			change = True
			self._log.warning('Timeout for the following jobs:')
			self.cancel(wms, jobnum_list_timeout, interactive=False, show_jobs=True)

		# Process task interventions
		self._process_intervention(task, wms)

		# Quit when all jobs are finished
		if self.job_db.get_job_len(ClassSelector(JobClass.ENDSTATE)) == len(self.job_db):
			self._log_disabled_jobs()
			if task.can_finish():
				self._local_event_handler.on_task_finish(len(self.job_db))
				abort(True)

		return change

	def delete(self, task, wms, select):
		selector = AndJobSelector(ClassSelector(JobClass.PROCESSING),
			JobSelector.create(select, task=task))
		jobs = self.job_db.get_job_list(selector)
		if jobs:
			self._log.warning('Cancelling the following jobs:')
			self.cancel(wms, jobs, interactive=self._interactive_delete, show_jobs=True)

	def finish(self):
		self._local_event_handler.on_workflow_finish()

	def remove_event_handler(self, callback):
		self._callback_list.remove(callback)

	def reset(self, task, wms, select):
		jobnum_list = self.job_db.get_job_list(JobSelector.create(select, task=task))
		if jobnum_list:
			self._log.warning('Resetting the following jobs:')
			self._abort_report.show_report(self.job_db, jobnum_list)
			ask_user_msg = 'Are you sure you want to reset the state of these jobs?'
			if self._interactive_reset or self._uii.prompt_bool(ask_user_msg, False):
				self.cancel(wms, self.job_db.get_job_list(
					ClassSelector(JobClass.PROCESSING), jobnum_list), interactive=False, show_jobs=False)
				for jobnum in jobnum_list:
					self.job_db.commit(jobnum, Job())

	def retrieve(self, task, wms):
		change = False
		jobnum_list = self._sample(self.job_db.get_job_list(ClassSelector(JobClass.DONE)),
			self._get_chunk_size(self._chunks_retrieve))

		job_output_iter = wms.retrieve_jobs(self._get_wms_args(jobnum_list))
		for (jobnum, exit_code, data, outputdir) in job_output_iter:
			job_obj = self.job_db.get_job(jobnum)
			if job_obj is None:
				continue

			if exit_code == 0:
				state = Job.SUCCESS
			elif exit_code == 107:  # set ABORTED instead of FAILED for errorcode 107
				state = Job.ABORTED
			else:
				state = Job.FAILED

			if state == Job.SUCCESS:
				if not self._output_processor.process(outputdir, task):
					exit_code = 108
					state = Job.FAILED

			if state != job_obj.state:
				change = True
				job_obj.set('retcode', exit_code)
				job_obj.set('runtime', data.get('TIME', -1))
				self._update(job_obj, jobnum, state)
				self._local_event_handler.on_job_output(wms, job_obj, jobnum, exit_code)

			if abort():
				return False

		return change

	def submit(self, task, wms):
		jobnum_list = self._submit_get_jobs(task)
		if len(jobnum_list) == 0:
			return False

		submitted = []
		for (jobnum, gc_id, data) in wms.submit_jobs(jobnum_list, task):
			submitted.append(jobnum)
			job_obj = self.job_db.get_job_persistent(jobnum)
			job_obj.clear_old_state()

			if gc_id is None:
				# Could not register at WMS
				self._update(job_obj, jobnum, Job.FAILED)
				continue

			job_obj.assign_id(gc_id)
			for (key, value) in data.items():
				job_obj.set(key, value)

			self._update(job_obj, jobnum, Job.SUBMITTED)
			self._local_event_handler.on_job_submit(wms, job_obj, jobnum)
			if abort():
				return False
		return len(submitted) != 0

	def _check_get_jobnum_list(self, wms, jobnum_list):
		(change, jobnum_list_timeout, reported) = (False, [], [])
		if not jobnum_list:
			return (change, jobnum_list_timeout, reported)
		for (jobnum, job_obj, state, info) in self._check_jobs_raw(wms, jobnum_list):
			if state != Job.UNKNOWN:
				reported.append(jobnum)
			if state != job_obj.state:
				change = True
				for (key, value) in info.items():
					job_obj.set(key, value)
				self._update(job_obj, jobnum, state)
				self._local_event_handler.on_job_update(wms, job_obj, jobnum, info)
			else:
				# If a job stays too long in an inital state, cancel it
				if job_obj.state in (Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED):
					if self._timeout_queue > 0 and time.time() - job_obj.submitted > self._timeout_queue:
						jobnum_list_timeout.append(jobnum)
				if job_obj.state == Job.UNKNOWN:
					if self._timeout_unknown > 0 and time.time() - job_obj.submitted > self._timeout_unknown:
						jobnum_list_timeout.append(jobnum)
			if abort():
				return (None, jobnum_list_timeout, reported)
		return (change, jobnum_list_timeout, reported)

	def _check_jobs_raw(self, wms, jobnum_list):
		# ask wms and yield (jobnum, job_obj, job_status, job_info)
		map_gc_id2jobnum = self._get_map_gc_id_jobnum(jobnum_list)
		for (gc_id, job_state, job_info) in wms.check_jobs(map_gc_id2jobnum.keys()):
			if not abort():
				jobnum = map_gc_id2jobnum.pop(gc_id, None)
				if jobnum is not None:
					yield (jobnum, self.job_db.get_job(jobnum), job_state, job_info)
		for jobnum in map_gc_id2jobnum.values():  # missing jobs are returned with Job.UNKNOWN state
			yield (jobnum, self.job_db.get_job(jobnum), Job.UNKNOWN, {})

	def _get_chunk_size(self, user_size, default=-1):
		if self._chunks_enabled and (user_size > 0):
			return user_size
		return default

	def _get_enabled_jobs(self, task, jobnum_list_ready):
		(n_mod_ok, n_retry_ok, jobnum_list_enabled) = (0, 0, [])
		for jobnum in jobnum_list_ready:
			job_obj = self.job_db.get_job_transient(jobnum)
			can_retry = (self._job_retries < 0) or (job_obj.attempt - 1 < self._job_retries)
			can_submit = task.can_submit(jobnum)
			if can_retry:
				n_retry_ok += 1
			if can_submit:
				n_mod_ok += 1
			if can_submit and can_retry:
				jobnum_list_enabled.append(jobnum)
			if can_submit and (job_obj.state == Job.DISABLED):  # recover jobs
				self._update(job_obj, jobnum, Job.INIT, reason='reenabled by task module')
			elif not can_submit and (job_obj.state != Job.DISABLED):  # disable invalid jobs
				self._update(self.job_db.get_job_persistent(jobnum),
					jobnum, Job.DISABLED, reason='disabled by task module')
		return (n_mod_ok, n_retry_ok, jobnum_list_enabled)

	def _get_map_gc_id_jobnum(self, jobnum_list):
		return dict(imap(lambda jobnum: (self.job_db.get_job(jobnum).gc_id, jobnum), jobnum_list))

	def _get_max_jobs(self, task):
		njobs_user = self._njobs_limit
		njobs_task = task.get_job_len()
		if njobs_task is None:  # Task module doesn't define a maximum number of jobs
			if njobs_user < 0:  # User didn't specify a maximum number of jobs
				raise ConfigError('Task module doesn\'t provide max number of Jobs. ' +
					'User specified number of jobs needed!')
			elif njobs_user >= 0:  # Run user specified number of jobs
				return njobs_user
		if njobs_user < 0:  # No user specified limit => run all jobs
			return njobs_task
		njobs_min = min(njobs_user, njobs_task)
		if njobs_user < njobs_task:
			self._log.warning('Maximum number of jobs in task (%d) was truncated to %d',
				njobs_task, njobs_min)
		return njobs_min

	def _get_wms_args(self, jobnum_list):
		return lmap(lambda jobnum: (self.job_db.get_job(jobnum).gc_id, jobnum), jobnum_list)

	def _log_disabled_jobs(self):
		disabled = self.job_db.get_job_list(ClassSelector(JobClass.DISABLED))
		try:
			with_file(SafeFile(self._disabled_jobs_logfile, 'w'),
				lambda fp: fp.write(str.join('\n', imap(str, disabled))))
		except Exception:
			raise JobError('Could not write disabled jobs to file %s!' % self._disabled_jobs_logfile)
		if disabled:
			self._log.log_time(logging.WARNING, 'There are %d disabled jobs in this task!', len(disabled))
			self._log.log_time(logging.DEBUG,
				'Please refer to %s for a complete list of disabled jobs.', self._disabled_jobs_logfile)

	def _process_intervention(self, task, wms):
		# Process changes of job states requested by task module
		resetable_state_list = [Job.INIT, Job.DISABLED, Job.ABORTED,
			Job.CANCELLED, Job.DONE, Job.FAILED, Job.SUCCESS]

		def _reset_state(jobnum_list, state_new):
			jobnum_listet = set(jobnum_list)
			for jobnum in jobnum_list:
				job_obj = self.job_db.get_job_persistent(jobnum)
				if job_obj.state in resetable_state_list:
					self._update(job_obj, jobnum, state_new)
					jobnum_listet.remove(jobnum)
					job_obj.attempt = 0

			if len(jobnum_listet) > 0:
				raise JobError('For the following jobs it was not possible to reset the state to %s:\n%s' % (
					Job.enum2str(state_new), str.join(', ', imap(str, jobnum_listet))))

		(redo, disable, size_change) = task.get_intervention()
		if (not redo) and (not disable) and (not size_change):
			return
		self._log.log_time(logging.INFO, 'The task module has requested changes to the job database')
		max_job_len_new = self._get_max_jobs(task)
		applied_change = False
		if max_job_len_new != len(self.job_db):
			self._log.log_time(logging.INFO,
				'Number of jobs changed from %d to %d', len(self.job_db), max_job_len_new)
			self.job_db.set_job_limit(max_job_len_new)
			applied_change = True
		if redo:
			self.cancel(wms, self.job_db.get_job_list(
				ClassSelector(JobClass.PROCESSING), redo), interactive=False, show_jobs=True)
			_reset_state(redo, Job.INIT)
			applied_change = True
		if disable:
			self.cancel(wms, self.job_db.get_job_list(
				ClassSelector(JobClass.PROCESSING), disable), interactive=False, show_jobs=True)
			_reset_state(disable, Job.DISABLED)
			applied_change = True
		if applied_change:
			self._log.log_time(logging.INFO, 'All requested changes are applied')

	def _sample(self, jobnum_list, size):
		if size >= 0:
			jobnum_list = random.sample(jobnum_list, min(size, len(jobnum_list)))
		return sorted(jobnum_list)

	def _submit_get_jobs(self, task):
		# Get list of submittable jobs
		jobnum_list_ready = self.job_db.get_job_list(ClassSelector(JobClass.SUBMIT_CANDIDATES))
		(n_mod_ok, n_retry_ok, jobnum_list) = self._get_enabled_jobs(task, jobnum_list_ready)

		if self._show_blocker and jobnum_list_ready and not jobnum_list:  # No submission but ready jobs
			err_str_list = []
			if (n_retry_ok <= 0) or (n_mod_ok != 0):
				err_str_list.append('have hit their maximum number of retries')
			if (n_retry_ok != 0) and (n_mod_ok <= 0):
				err_str_list.append('are vetoed by the task module')
			err_delim = ' and '
			if n_retry_ok or n_mod_ok:
				err_delim = ' or '
			self._log.log_time(logging.WARNING, 'All remaining jobs %s!', str.join(err_delim, err_str_list))
		self._show_blocker = not (len(jobnum_list_ready) > 0 and len(jobnum_list) == 0)

		# Determine number of jobs to submit
		submit = len(jobnum_list)
		if self._njobs_inqueue > 0:
			submit = min(submit, self._njobs_inqueue - self.job_db.get_job_len(
				ClassSelector(JobClass.ATWMS)))
		if self._njobs_inflight > 0:
			submit = min(submit, self._njobs_inflight - self.job_db.get_job_len(
				ClassSelector(JobClass.PROCESSING)))
		if self._chunks_enabled and (self._chunks_submit > 0):
			submit = min(submit, self._chunks_submit)
		submit = max(submit, 0)

		if self._do_shuffle:
			return self._sample(jobnum_list, submit)
		return sorted(jobnum_list)[:submit]

	def _update(self, job_obj, jobnum, new_state, show_wms=False, reason=None):
		old_state = job_obj.state
		if old_state != new_state:
			job_obj.update(new_state)
			self.job_db.commit(jobnum, job_obj)
			self._local_event_handler.on_job_state_change(len(self.job_db), jobnum, job_obj,
				old_state, new_state, reason)
			for callback in self._callback_list:
				callback()


class SimpleJobManager(JobManager):
	alias_list = ['default']

	def __init__(self, config, name, task):
		JobManager.__init__(self, config, name, task)

		# Non-persistent Job defect heuristic to remove jobs, causing errors during status queries
		self._defect_tries = config.get_int(['kick offender', 'defect tries'], 10, on_change=None)
		(self._defect_counter, self._defect_raster) = ({}, 0)

		# job verification heuristic - launch jobs in chunks of increasing size if enough jobs succeed
		self._verify = False
		self._verify_chunk_list = config.get_list('verify chunks', [-1], on_change=None, parse_item=int)
		self._verify_threshold_list = config.get_list(
			['verify reqs', 'verify threshold'], [0.5], on_change=None, parse_item=float)
		if self._verify_chunk_list:
			self._verify = True
			missing_verify_entries = len(self._verify_chunk_list) - len(self._verify_threshold_list)
			self._verify_threshold_list += [self._verify_threshold_list[-1]] * missing_verify_entries
			self._log.log_time(logging.INFO1, 'Verification mode active')
			self._log.log_time(logging.INFO1,
				'Submission is capped unless the success ratio of a chunk of jobs is sufficent.')
			self._log.log_time(logging.DEBUG, 'Enforcing the following (chunksize x ratio) sequence:')
			iter_verify_info = izip(self._verify_chunk_list, self._verify_threshold_list)
			self._log.log_time(logging.DEBUG, str.join(' > ',
				imap(lambda chunk_threshold: '%d x %4.2f' % chunk_threshold, iter_verify_info)))
		self._unreachable_goal_flag = False

	def _check_get_jobnum_list(self, wms, jobnum_list):
		if self._defect_tries:
			num_defect = len(self._defect_counter)  # Waiting list gets larger in case reported == []
			num_wait = num_defect - max(1, int(num_defect / 2**self._defect_raster))
			jobnum_list_wait = self._sample(self._defect_counter, num_wait)
			jobnum_list = lfilter(lambda jobnum: jobnum not in jobnum_list_wait, jobnum_list)

		(change, jobnum_list_timeout, reported) = JobManager._check_get_jobnum_list(
			self, wms, jobnum_list)
		for jobnum in reported:
			self._defect_counter.pop(jobnum, None)

		if self._defect_tries and (change is not None):
			# make 'raster' iteratively smaller
			self._defect_raster += 1
			if reported:
				self._defect_raster = 1
			for jobnum in ifilter(lambda x: x not in reported, jobnum_list):
				self._defect_counter[jobnum] = self._defect_counter.get(jobnum, 0) + 1
			jobnum_list_kick = lfilter(lambda jobnum: self._defect_counter[jobnum] >= self._defect_tries,
				self._defect_counter)
			if (len(reported) == 0) and (len(jobnum_list) == 1):
				jobnum_list_kick.extend(jobnum_list)
			for jobnum in set(jobnum_list_kick):
				jobnum_list_timeout.append(jobnum)
				self._defect_counter.pop(jobnum)

		return (change, jobnum_list_timeout, reported)

	def _submit_get_jobs(self, task):
		result = JobManager._submit_get_jobs(self, task)
		if self._verify:
			return result[:self._submit_get_jobs_throttled(len(result))]
		return result

	def _submit_get_jobs_throttled(self, job_len_submit):
		# Verification heuristic - check whether enough jobs have succeeded before submitting more
		job_len_active = self.job_db.get_job_len(ClassSelector(JobClass.PROCESSING))
		job_len_success = self.job_db.get_job_len(ClassSelector(JobClass.SUCCESS))
		job_len_done = self.job_db.get_job_len(ClassSelector(JobClass.PROCESSED))
		job_len_total = job_len_done + job_len_active
		verify_idx = bisect.bisect_left(self._verify_chunk_list, job_len_total)
		try:
			success_ratio = job_len_success * 1.0 / self._verify_chunk_list[verify_idx]
			goal = self._verify_chunk_list[verify_idx] * self._verify_threshold_list[verify_idx]
			if self._verify_chunk_list[verify_idx] - job_len_done + job_len_success < goal:
				if not self._unreachable_goal_flag:
					self._log.log_time(logging.WARNING,
						'All remaining jobs are vetoed by an unachieveable verification goal!')
					self._log.log_time(logging.INFO, 'Current goal: %d successful jobs out of %d',
						goal, self._verify_chunk_list[verify_idx])
					self._unreachable_goal_flag = True
				return 0
			if success_ratio < self._verify_threshold_list[verify_idx]:
				return min(job_len_submit, self._verify_chunk_list[verify_idx] - job_len_total)
			else:
				return min(job_len_submit, self._verify_chunk_list[verify_idx + 1] - job_len_total)
		except IndexError:
			clear_current_exception()
			self._log.log_time(logging.DEBUG, 'All verification chunks passed')
			self._log.log_time(logging.DEBUG, 'Verification submission throttle disabled')
			self._verify = False
			return job_len_submit
