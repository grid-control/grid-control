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

import math, time, bisect, random, logging
from grid_control import utils
from grid_control.config import ConfigError
from grid_control.gc_plugin import NamedPlugin
from grid_control.job_db import Job, JobClass, JobDB, JobError
from grid_control.job_selector import AndJobSelector, ClassSelector, JobSelector
from grid_control.output_processor import TaskOutputProcessor
from grid_control.report import Report
from grid_control.utils.file_objects import SafeFile
from grid_control.utils.parsing import strTime
from python_compat import ifilter, imap, izip, lfilter, lmap, set, sorted

class JobManager(NamedPlugin):
	configSections = NamedPlugin.configSections + ['jobs']
	tagName = 'jobmgr'

	def __init__(self, config, name, task, eventhandler):
		NamedPlugin.__init__(self, config, name)
		(self._task, self._eventhandler) = (task, eventhandler)
		self._log = logging.getLogger('jobs.manager')

		self._njobs_limit = config.getInt('jobs', -1, onChange = None)
		self._njobs_inflight = config.getInt('in flight', -1, onChange = None)
		self._njobs_inqueue = config.getInt('in queue', -1, onChange = None)

		self._chunks_enabled = config.getBool('chunks enabled', True, onChange = None)
		self._chunks_submit = config.getInt('chunks submit', 100, onChange = None)
		self._chunks_check = config.getInt('chunks check', 100, onChange = None)
		self._chunks_retrieve = config.getInt('chunks retrieve', 100, onChange = None)

		self._timeout_unknown = config.getTime('unknown timeout', -1, onChange = None)
		self._timeout_queue = config.getTime('queue timeout', -1, onChange = None)
		self._job_retries = config.getInt('max retry', -1, onChange = None)

		selected = JobSelector.create(config.get('selected', '', onChange = None), task = self._task)
		self.jobDB = config.getPlugin('job database', 'TextFileJobDB',
			cls = JobDB, pargs = (self._get_max_jobs(self._task), selected), onChange = None)
		self._disabled_jobs_logfile = config.getWorkPath('disabled')
		self._outputProcessor = config.getPlugin('output processor', 'SandboxProcessor',
			cls = TaskOutputProcessor)

		self._interactive_delete = config.isInteractive('delete jobs', True)
		self._interactive_reset = config.isInteractive('reset jobs', True)
		self._do_shuffle = config.getBool('shuffle', False, onChange = None)
		self._reportClass = Report.getClass(config.get('abort report', 'LocationReport', onChange = None))
		self._show_blocker = True


	def _get_chunk_size(self, user_size, default = -1):
		if self._chunks_enabled and (user_size > 0):
			return user_size
		return default


	def _get_max_jobs(self, task):
		njobs_user = self._njobs_limit
		njobs_task = task.getMaxJobs()
		if njobs_task is None: # Task module doesn't define a maximum number of jobs
			if njobs_user < 0: # User didn't specify a maximum number of jobs
				raise ConfigError('Task module doesn\'t provide max number of Jobs. User specified number of jobs needed!')
			elif njobs_user >= 0: # Run user specified number of jobs
				return njobs_user
		if njobs_user < 0: # No user specified limit => run all jobs
			return njobs_task
		njobs_min = min(njobs_user, njobs_task)
		if njobs_user < njobs_task:
			self._log.warning('Maximum number of jobs in task (%d) was truncated to %d', njobs_task, njobs_min)
		return njobs_min


	def _log_disabled_jobs(self):
		disabled = self.jobDB.getJobs(ClassSelector(JobClass.DISABLED))
		try:
			fp = SafeFile(self._disabled_jobs_logfile, 'w')
			fp.write(str.join('\n', imap(str, disabled)))
			fp.close()
		except Exception:
			raise JobError('Could not write disabled jobs to file %s!' % self._disabled_jobs_logfile)
		if disabled:
			self._log.log_time(logging.WARNING, 'There are %d disabled jobs in this task!', len(disabled))
			self._log.log_time(logging.DEBUG, 'Please refer to %s for a complete list of disabled jobs.', self._disabled_jobs_logfile)


	def _update(self, jobObj, jobNum, state, showWMS = False, message = None):
		if jobObj.state == state:
			return

		oldState = jobObj.state
		jobObj.update(state)
		self.jobDB.commit(jobNum, jobObj)

		jobNumLen = int(math.log10(max(1, len(self.jobDB))) + 1)
		jobStatus = ['Job %s state changed from %s to %s ' % (str(jobNum).ljust(jobNumLen), Job.enum2str(oldState), Job.enum2str(state))]
		if message is not None:
			jobStatus.append(message)
		if showWMS and jobObj.gcID:
			jobStatus.append('(WMS:%s)' % jobObj.gcID.split('.')[1])
		if (state == Job.SUBMITTED) and (jobObj.attempt > 1):
			jobStatus.append('(retry #%s)' % (jobObj.attempt - 1))
		elif (state == Job.QUEUED) and (jobObj.get('dest') != 'N/A'):
			jobStatus.append('(%s)' % jobObj.get('dest'))
		elif (state in [Job.WAITING, Job.ABORTED, Job.DISABLED]) and jobObj.get('reason'):
			jobStatus.append('(%s)' % jobObj.get('reason'))
		elif (state == Job.SUCCESS) and (jobObj.get('runtime', None) is not None):
			jobStatus.append('(runtime %s)' % strTime(jobObj.get('runtime') or 0))
		elif state == Job.FAILED:
			msg = []
			retCode = jobObj.get('retcode')
			if retCode:
				msg.append('error code: %d' % retCode)
				if self._log.isEnabledFor(logging.DEBUG) and (retCode in self._task.errorDict):
					msg.append(self._task.errorDict[retCode])
			if jobObj.get('dest'):
				msg.append(jobObj.get('dest'))
			if len(msg):
				jobStatus.append('(%s)' % str.join(' - ', msg))
		self._log.log_time(logging.INFO, str.join(' ', jobStatus))


	def _sample(self, jobList, size):
		if size >= 0:
			jobList = random.sample(jobList, min(size, len(jobList)))
		return sorted(jobList)


	def _get_map_gcID_jobnum(self, jobnum_list):
		return dict(imap(lambda jobnum: (self.jobDB.getJob(jobnum).gcID, jobnum), jobnum_list))


	def _get_enabled_jobs(self, jobnum_list_ready):
		(n_mod_ok, n_retry_ok, jobnum_list_enabled) = (0, 0, [])
		for jobnum in jobnum_list_ready:
			job_obj = self.jobDB.getJobTransient(jobnum)
			can_retry = (self._job_retries < 0) or (job_obj.attempt - 1 < self._job_retries)
			can_submit = self._task.canSubmit(jobnum)
			if can_retry:
				n_retry_ok += 1
			if can_submit:
				n_mod_ok += 1
			if can_submit and can_retry:
				jobnum_list_enabled.append(jobnum)
			if can_submit and (job_obj.state == Job.DISABLED): # recover jobs
				self._update(job_obj, jobnum, Job.INIT, message = 'reenabled by task module')
			elif not can_submit and (job_obj.state != Job.DISABLED): # disable invalid jobs
				self._update(self.jobDB.getJobPersistent(jobnum), jobnum, Job.DISABLED, message = 'disabled by task module')
		return (n_mod_ok, n_retry_ok, jobnum_list_enabled)


	def _submit_get_jobs(self):
		# Get list of submittable jobs
		jobnum_list_ready = self.jobDB.getJobs(ClassSelector(JobClass.SUBMIT_CANDIDATES))
		(n_mod_ok, n_retry_ok, jobnum_list) = self._get_enabled_jobs(jobnum_list_ready)

		if self._show_blocker and jobnum_list_ready and not jobnum_list: # No submission but ready jobs
			err = []
			err += utils.QM((n_retry_ok > 0) and (n_mod_ok == 0), [], ['have hit their maximum number of retries'])
			err += utils.QM((n_retry_ok == 0) and (n_mod_ok > 0), [], ['are vetoed by the task module'])
			self._log.log_time(logging.WARNING, 'All remaining jobs %s!', str.join(utils.QM(n_retry_ok or n_mod_ok, ' or ', ' and '), err))
		self._show_blocker = not (len(jobnum_list_ready) > 0 and len(jobnum_list) == 0)

		# Determine number of jobs to submit
		submit = len(jobnum_list)
		if self._njobs_inqueue > 0:
			submit = min(submit, self._njobs_inqueue - self.jobDB.getJobsN(ClassSelector(JobClass.ATWMS)))
		if self._njobs_inflight > 0:
			submit = min(submit, self._njobs_inflight - self.jobDB.getJobsN(ClassSelector(JobClass.PROCESSING)))
		if self._chunks_enabled and (self._chunks_submit > 0):
			submit = min(submit, self._chunks_submit)
		submit = max(submit, 0)

		if self._do_shuffle:
			return self._sample(jobnum_list, submit)
		return sorted(jobnum_list)[:submit]


	def submit(self, wms):
		jobnum_list = self._submit_get_jobs()
		if len(jobnum_list) == 0:
			return False

		submitted = []
		for (jobnum, gcID, data) in wms.submitJobs(jobnum_list, self._task):
			submitted.append(jobnum)
			job_obj = self.jobDB.getJobPersistent(jobnum)

			if gcID is None:
				# Could not register at WMS
				self._update(job_obj, jobnum, Job.FAILED)
				continue

			job_obj.assignId(gcID)
			for (key, value) in data.items():
				job_obj.set(key, value)

			self._update(job_obj, jobnum, Job.SUBMITTED)
			self._eventhandler.onJobSubmit(wms, job_obj, jobnum)
			if utils.abort():
				return False
		return len(submitted) != 0


	def _check_jobs_raw(self, wms, jobnum_list): # ask wms and yield (jobnum, job_obj, job_status, job_info)
		gcID_jobnum_Map = self._get_map_gcID_jobnum(jobnum_list)
		for (gcID, job_state, job_info) in wms.checkJobs(gcID_jobnum_Map.keys()):
			if not utils.abort():
				jobnum = gcID_jobnum_Map.pop(gcID, None)
				if jobnum is not None:
					yield (jobnum, self.jobDB.getJob(jobnum), job_state, job_info)
		for jobnum in gcID_jobnum_Map.values(): # missing jobs are returned with Job.UNKNOWN state
			yield (jobnum, self.jobDB.getJob(jobnum), Job.UNKNOWN, {})


	def _checkJobList(self, wms, jobList):
		(change, timeoutList, reported) = (False, [], [])
		if not jobList:
			return (change, timeoutList, reported)
		for (jobNum, jobObj, state, info) in self._check_jobs_raw(wms, jobList):
			if state != Job.UNKNOWN:
				reported.append(jobNum)
			if state != jobObj.state:
				change = True
				for (key, value) in info.items():
					jobObj.set(key, value)
				self._update(jobObj, jobNum, state)
				self._eventhandler.onJobUpdate(wms, jobObj, jobNum, info)
			else:
				# If a job stays too long in an inital state, cancel it
				if jobObj.state in (Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED):
					if self._timeout_queue > 0 and time.time() - jobObj.submitted > self._timeout_queue:
						timeoutList.append(jobNum)
				if jobObj.state == Job.UNKNOWN:
					if self._timeout_unknown > 0 and time.time() - jobObj.submitted > self._timeout_unknown:
						timeoutList.append(jobNum)
			if utils.abort():
				return (None, timeoutList, reported)
		return (change, timeoutList, reported)


	def check(self, wms):
		jobList = self._sample(self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING)), utils.QM(self._chunks_enabled, self._chunks_check, -1))

		# Check jobs in the joblist and return changes, timeouts and successfully reported jobs
		(change, timeoutList, reported) = self._checkJobList(wms, jobList)
		unreported = len(jobList) - len(reported)
		if unreported > 0:
			self._log.log_time(logging.CRITICAL, '%d job(s) did not report their status!', unreported)
		if change is None: # neither True or False => abort
			return False

		# Cancel jobs which took too long
		if len(timeoutList):
			change = True
			self._log.warning('Timeout for the following jobs:')
			self.cancel(wms, timeoutList, interactive = False, showJobs = True)

		# Process task interventions
		self._processIntervention(wms, self._task.getIntervention())

		# Quit when all jobs are finished
		if self.jobDB.getJobsN(ClassSelector(JobClass.ENDSTATE)) == len(self.jobDB):
			self._log_disabled_jobs()
			self._eventhandler.onTaskFinish(len(self.jobDB))
			if self._task.canFinish():
				self._log.log_time(logging.INFO, 'Task successfully completed. Quitting grid-control!')
				utils.abort(True)

		return change


	def _wmsArgs(self, jobList):
		return lmap(lambda jobNum: (self.jobDB.getJob(jobNum).gcID, jobNum), jobList)


	def retrieve(self, wms):
		change = False
		jobList = self._sample(self.jobDB.getJobs(ClassSelector(JobClass.DONE)), utils.QM(self._chunks_enabled, self._chunks_retrieve, -1))

		for (jobNum, retCode, data, outputdir) in wms.retrieveJobs(self._wmsArgs(jobList)):
			jobObj = self.jobDB.getJob(jobNum)
			if jobObj is None:
				continue

			if retCode == 0:
				state = Job.SUCCESS
			elif retCode == 107: # set ABORTED instead of FAILED for errorcode 107
				state = Job.ABORTED
			else:
				state = Job.FAILED

			if state == Job.SUCCESS:
				if not self._outputProcessor.process(outputdir, self._task):
					retCode = 108
					state = Job.FAILED

			if state != jobObj.state:
				change = True
				jobObj.set('retcode', retCode)
				jobObj.set('runtime', data.get('TIME', -1))
				self._update(jobObj, jobNum, state)
				self._eventhandler.onJobOutput(wms, jobObj, jobNum, retCode)

			if utils.abort():
				return False

		return change


	def cancel(self, wms, jobnum_list, interactive, showJobs):
		if len(jobnum_list) == 0:
			return
		if showJobs:
			self._reportClass(self.jobDB, self._task, jobnum_list).display()
		if interactive and not utils.getUserBool('Do you really want to cancel these jobs?', True):
			return

		def mark_cancelled(jobNum):
			jobObj = self.jobDB.getJob(jobNum)
			if jobObj is None:
				return
			self._update(jobObj, jobNum, Job.CANCELLED)
			self._eventhandler.onJobUpdate(wms, jobObj, jobNum, {'reason': 'cancelled'})

		jobnum_list.reverse()
		gcID_jobnum_map = self._get_map_gcID_jobnum(jobnum_list)
		gcIDs = sorted(gcID_jobnum_map, key = lambda gcID: -gcID_jobnum_map[gcID])
		for (gcID,) in wms.cancelJobs(gcIDs):
			# Remove deleted job from todo list and mark as cancelled
			mark_cancelled(gcID_jobnum_map.pop(gcID))

		if gcID_jobnum_map:
			jobnum_list = list(gcID_jobnum_map.values())
			self._log.warning('There was a problem with cancelling the following jobs:')
			self._reportClass(self.jobDB, self._task, jobnum_list).display()
			if (not interactive) or utils.getUserBool('Do you want to mark them as cancelled?', True):
				lmap(mark_cancelled, jobnum_list)
		if interactive:
			utils.wait(2)


	def delete(self, wms, select):
		selector = AndJobSelector(ClassSelector(JobClass.PROCESSING), JobSelector.create(select, task = self._task))
		jobs = self.jobDB.getJobs(selector)
		if jobs:
			self._log.warning('Cancelling the following jobs:')
			self.cancel(wms, jobs, interactive = self._interactive_delete, showJobs = True)


	def reset(self, wms, select):
		jobs = self.jobDB.getJobs(JobSelector.create(select, task = self._task))
		if jobs:
			self._log.warning('Resetting the following jobs:')
			self._reportClass(self.jobDB, self._task, jobs).display()
			if self._interactive_reset or utils.getUserBool('Are you sure you want to reset the state of these jobs?', False):
				self.cancel(wms, self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING), jobs), interactive = False, showJobs = False)
				for jobNum in jobs:
					self.jobDB.commit(jobNum, Job())


	# Process changes of job states requested by task module
	def _processIntervention(self, wms, jobChanges):
		def resetState(jobs, newState):
			jobSet = set(jobs)
			for jobNum in jobs:
				jobObj = self.jobDB.getJobPersistent(jobNum)
				if jobObj.state in [Job.INIT, Job.DISABLED, Job.ABORTED, Job.CANCELLED, Job.DONE, Job.FAILED, Job.SUCCESS]:
					self._update(jobObj, jobNum, newState)
					jobSet.remove(jobNum)
					jobObj.attempt = 0

			if len(jobSet) > 0:
				output = (Job.enum2str(newState), str.join(', ', imap(str, jobSet)))
				raise JobError('For the following jobs it was not possible to reset the state to %s:\n%s' % output)

		(redo, disable, sizeChange) = jobChanges
		if (not redo) and (not disable) and (not sizeChange):
			return
		self._log.log_time(logging.INFO, 'The task module has requested changes to the job database')
		newMaxJobs = self._get_max_jobs(self._task)
		applied_change = False
		if newMaxJobs != len(self.jobDB):
			self._log.log_time(logging.INFO, 'Number of jobs changed from %d to %d', len(self.jobDB), newMaxJobs)
			self.jobDB.setJobLimit(newMaxJobs)
			applied_change = True
		if redo:
			self.cancel(wms, self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING), redo), interactive = False, showJobs = True)
			resetState(redo, Job.INIT)
			applied_change = True
		if disable:
			self.cancel(wms, self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING), disable), interactive = False, showJobs = True)
			resetState(disable, Job.DISABLED)
			applied_change = True
		if applied_change:
			self._log.log_time(logging.INFO, 'All requested changes are applied')


class SimpleJobManager(JobManager):
	def __init__(self, config, name, task, eventhandler):
		JobManager.__init__(self, config, name, task, eventhandler)

		# Job defect heuristic (not persistent!) - remove jobs, which cause errors when doing status queries
		self._defect_tries = config.getInt(['kick offender', 'defect tries'], 10, onChange = None)
		(self._defect_counter, self._defect_raster) = ({}, 0)

		# job verification heuristic - launch jobs in chunks of increasing size if enough jobs succeed
		self._verify = False
		self._verifyChunks = config.getList('verify chunks', [-1], onChange = None, parseItem = int)
		self._verifyThresh = config.getList(['verify reqs', 'verify threshold'], [0.5], onChange = None, parseItem = float)
		if self._verifyChunks:
			self._verify = True
			self._verifyThresh += [self._verifyThresh[-1]] * (len(self._verifyChunks) - len(self._verifyThresh))
			self._log.log_time(logging.INFO1, 'Verification mode active')
			self._log.log_time(logging.INFO1, 'Submission is capped unless the success ratio of a chunk of jobs is sufficent.')
			self._log.log_time(logging.DEBUG, 'Enforcing the following (chunksize x ratio) sequence:')
			self._log.log_time(logging.DEBUG, str.join(' > ', imap(lambda tpl: '%d x %4.2f'%(tpl[0], tpl[1]), izip(self._verifyChunks, self._verifyThresh))))
		self._unreachableGoal = False


	def _submit_get_jobs(self):
		result = JobManager._submit_get_jobs(self)
		if self._verify:
			return result[:self._submit_get_jobs_throttled(len(result))]
		return result


	# Verification heuristic - check whether enough jobs have succeeded before submitting more
	# @submitCount: number of jobs to submit
	def _submit_get_jobs_throttled(self, submitCount):
		jobsActive = self.jobDB.getJobsN(ClassSelector(JobClass.PROCESSING))
		jobsSuccess = self.jobDB.getJobsN(ClassSelector(JobClass.SUCCESS))
		jobsDone = self.jobDB.getJobsN(ClassSelector(JobClass.PROCESSED))
		jobsTotal = jobsDone + jobsActive
		verifyIndex = bisect.bisect_left(self._verifyChunks, jobsTotal)
		try:
			successRatio = jobsSuccess * 1.0 / self._verifyChunks[verifyIndex]
			goal = self._verifyChunks[verifyIndex] * self._verifyThresh[verifyIndex]
			if self._verifyChunks[verifyIndex] - jobsDone + jobsSuccess < goal:
				if not self._unreachableGoal:
					self._log.log_time(logging.WARNING, 'All remaining jobs are vetoed by an unachieveable verification goal!')
					self._log.log_time(logging.INFO, 'Current goal: %d successful jobs out of %d', goal, self._verifyChunks[verifyIndex])
					self._unreachableGoal = True
				return 0
			if successRatio < self._verifyThresh[verifyIndex]:
				return min(submitCount, self._verifyChunks[verifyIndex]-jobsTotal)
			else:
				return min(submitCount, self._verifyChunks[verifyIndex+1]-jobsTotal)
		except IndexError:
			self._log.log_time(logging.DEBUG, 'All verification chunks passed')
			self._log.log_time(logging.DEBUG, 'Verification submission throttle disabled')
			self._verify = False
			return submitCount


	def _checkJobList(self, wms, jobList):
		if self._defect_tries:
			nDefect = len(self._defect_counter) # Waiting list gets larger in case reported == []
			waitList = self._sample(self._defect_counter, nDefect - max(1, int(nDefect / 2**self._defect_raster)))
			jobList = lfilter(lambda x: x not in waitList, jobList)

		(change, timeoutList, reported) = JobManager._checkJobList(self, wms, jobList)
		for jobNum in reported:
			self._defect_counter.pop(jobNum, None)

		if self._defect_tries and (change is not None):
			self._defect_raster = utils.QM(reported, 1, self._defect_raster + 1) # make 'raster' iteratively smaller
			for jobNum in ifilter(lambda x: x not in reported, jobList):
				self._defect_counter[jobNum] = self._defect_counter.get(jobNum, 0) + 1
			kickList = lfilter(lambda jobNum: self._defect_counter[jobNum] >= self._defect_tries, self._defect_counter)
			for jobNum in set(kickList + utils.QM((len(reported) == 0) and (len(jobList) == 1), jobList, [])):
				timeoutList.append(jobNum)
				self._defect_counter.pop(jobNum)

		return (change, timeoutList, reported)
