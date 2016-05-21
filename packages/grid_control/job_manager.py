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
		self._log_user = logging.getLogger('user')
		self._log_user_time = logging.getLogger('user.time')

		self._njobs_limit = config.getInt('jobs', -1, onChange = None)
		self._njobs_inflight = config.getInt('in flight', -1, onChange = None)
		self._njobs_inqueue = config.getInt('in queue', -1, onChange = None)

		self._chunks_enabled = config.getBool('chunks enabled', True, onChange = None)
		self._chunks_submit = config.getInt('chunks submit', 100, onChange = None)
		self._chunks_check = config.getInt('chunks check', 100, onChange = None)
		self._chunks_retrieve = config.getInt('chunks retrieve', 100, onChange = None)

		self._job_timeout = config.getTime('queue timeout', -1, onChange = None)
		self._job_retries = config.getInt('max retry', -1, onChange = None)

		selected = JobSelector.create(config.get('selected', '', onChange = None), task = self._task)
		self.jobDB = config.getPlugin('job database', 'JobDB',
			cls = JobDB, pargs = (self.getMaxJobs(self._task), selected))
		self._disabled_jobs_logfile = config.getWorkPath('disabled')
		self._outputProcessor = config.getPlugin('output processor', 'SandboxProcessor',
			cls = TaskOutputProcessor, pargs = (task,))

		self._do_shuffle = config.getBool('shuffle', False, onChange = None)
		self._reportClass = Report.getClass(config.get('abort report', 'LocationReport', onChange = None))
		self._showBlocker = True


	def getMaxJobs(self, task):
		nJobs = self._njobs_limit
		if nJobs < 0:
			# No valid number of jobs given in config file - task has to provide number of jobs
			nJobs = task.getMaxJobs()
			if nJobs is None:
				raise ConfigError("Task module doesn't provide max number of Jobs!")
		else:
			# Task module doesn't have to provide number of jobs
			try:
				maxJobs = task.getMaxJobs()
				if maxJobs and (nJobs > maxJobs):
					self._log_user.warning('Maximum number of jobs given as %d was truncated to %d', nJobs, maxJobs)
					nJobs = maxJobs
			except Exception:
				pass
		return nJobs


	def _logDisabledJobs(self):
		disabled = self.jobDB.getJobs(ClassSelector(JobClass.DISABLED))
		try:
			fp = SafeFile(self._disabled_jobs_logfile, 'w')
			fp.write(str.join('\n', imap(str, disabled)))
			fp.close()
		except Exception:
			raise JobError('Could not write disabled jobs to file %s!' % self._disabled_jobs_logfile)
		if disabled:
			self._log_user_time.warning('There are %d disabled jobs in this task!', len(disabled))
			self._log_user_time.debug('Please refer to %s for a complete list of disabled jobs.', self._disabled_jobs_logfile)


	def _update(self, jobObj, jobNum, state, showWMS = False):
		if jobObj.state == state:
			return

		oldState = jobObj.state
		jobObj.update(state)
		self.jobDB.commit(jobNum, jobObj)

		jobNumLen = int(math.log10(max(1, len(self.jobDB))) + 1)
		jobStatus = ['Job %s state changed from %s to %s ' % (str(jobNum).ljust(jobNumLen), Job.enum2str(oldState), Job.enum2str(state))]
		if showWMS and jobObj.wmsId:
			jobStatus.append('(WMS:%s)' % jobObj.wmsId.split('.')[1])
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
				if (utils.verbosity() > 0) and (retCode in self._task.errorDict):
					msg.append(self._task.errorDict[retCode])
			if jobObj.get('dest'):
				msg.append(jobObj.get('dest'))
			if len(msg):
				jobStatus.append('(%s)' % str.join(' - ', msg))
		self._log_user_time.info(str.join(' ', jobStatus))


	def _sample(self, jobList, size):
		if size >= 0:
			jobList = random.sample(jobList, min(size, len(jobList)))
		return sorted(jobList)


	def _getSubmissionJobs(self, maxsample):
		# Get list of submittable jobs
		readyList = self.jobDB.getJobs(ClassSelector(JobClass.READY))
		retryOK = readyList
		defaultJob = Job()
		if self._job_retries >= 0:
			retryOK = lfilter(lambda x: self.jobDB.get(x, defaultJob).attempt - 1 < self._job_retries, readyList)
		modOK = lfilter(self._task.canSubmit, readyList)
		jobList = set.intersection(set(retryOK), set(modOK))

		if self._showBlocker and readyList and not jobList: # No submission but ready jobs
			err = []
			err += utils.QM((len(retryOK) > 0) and (len(modOK) == 0), [], ['have hit their maximum number of retries'])
			err += utils.QM((len(retryOK) == 0) and (len(modOK) > 0), [], ['are vetoed by the task module'])
			self._log_user_time.warning('All remaining jobs %s!', str.join(utils.QM(retryOK or modOK, ' or ', ' and '), err))
		self._showBlocker = not (len(readyList) > 0 and len(jobList) == 0)

		# Determine number of jobs to submit
		submit = len(jobList)
		if self._njobs_inqueue > 0:
			submit = min(submit, self._njobs_inqueue - self.jobDB.getJobsN(ClassSelector(JobClass.ATWMS)))
		if self._njobs_inflight > 0:
			submit = min(submit, self._njobs_inflight - self.jobDB.getJobsN(ClassSelector(JobClass.PROCESSING)))
		if self._chunks_enabled and (maxsample > 0):
			submit = min(submit, maxsample)
		submit = max(submit, 0)

		if self._do_shuffle:
			return self._sample(jobList, submit)
		return sorted(jobList)[:submit]


	def _wmsArgs(self, jobList):
		return lmap(lambda jobNum: (self.jobDB.get(jobNum).wmsId, jobNum), jobList)


	def _checkJobList(self, wms, jobList):
		(change, timeoutList, reported) = (False, [], [])
		for (jobNum, _, state, info) in wms.checkJobs(self._wmsArgs(jobList)):
			reported.append(jobNum)
			jobObj = self.jobDB.get(jobNum)
			if state != jobObj.state:
				change = True
				for (key, value) in info.items():
					jobObj.set(key, value)
				self._update(jobObj, jobNum, state)
				self._eventhandler.onJobUpdate(wms, jobObj, jobNum, info)
			else:
				# If a job stays too long in an inital state, cancel it
				if jobObj.state in (Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED):
					if self._job_timeout > 0 and time.time() - jobObj.submitted > self._job_timeout:
						timeoutList.append(jobNum)
			if utils.abort():
				return (None, timeoutList, reported)
		return (change, timeoutList, reported)


	def submit(self, wms):
		jobList = self._getSubmissionJobs(self._chunks_submit)
		if len(jobList) == 0:
			return False

		submitted = []
		for (jobNum, wmsId, data) in wms.submitJobs(jobList, self._task):
			submitted.append(jobNum)
			jobObj = self.jobDB.get(jobNum, create = True)

			if wmsId is None:
				# Could not register at WMS
				self._update(jobObj, jobNum, Job.FAILED)
				continue

			jobObj.assignId(wmsId)
			for (key, value) in data.items():
				jobObj.set(key, value)

			self._update(jobObj, jobNum, Job.SUBMITTED)
			self._eventhandler.onJobSubmit(wms, jobObj, jobNum)
			if utils.abort():
				return False
		return len(submitted) != 0


	def check(self, wms):
		jobList = self._sample(self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING)), utils.QM(self._chunks_enabled, self._chunks_check, -1))

		# Check jobs in the joblist and return changes, timeouts and successfully reported jobs
		(change, timeoutList, reported) = self._checkJobList(wms, jobList)
		unreported = len(jobList) - len(reported)
		if unreported > 0:
			self._log_user_time.critical('%d job(s) did not report their status!', unreported)
		if change is None: # neither True or False => abort
			return False

		# Cancel jobs which took too long
		if len(timeoutList):
			change = True
			self._log_user.warning('Timeout for the following jobs:')
			self.cancel(wms, timeoutList)

		# Process task interventions
		self._processIntervention(wms, self._task.getIntervention())

		# Quit when all jobs are finished
		if self.jobDB.getJobsN(ClassSelector(JobClass.ENDSTATE)) == len(self.jobDB):
			self._logDisabledJobs()
			self._eventhandler.onTaskFinish(len(self.jobDB))
			if self._task.canFinish():
				self._log_user_time.info('Task successfully completed. Quitting grid-control!')
				utils.abort(True)

		return change


	def retrieve(self, wms):
		change = False
		jobList = self._sample(self.jobDB.getJobs(ClassSelector(JobClass.DONE)), utils.QM(self._chunks_enabled, self._chunks_retrieve, -1))

		for (jobNum, retCode, data, outputdir) in wms.retrieveJobs(self._wmsArgs(jobList)):
			jobObj = self.jobDB.get(jobNum)
			if jobObj is None:
				continue

			if retCode == 0:
				state = Job.SUCCESS
			elif retCode == 107: # set ABORTED instead of FAILED for errorcode 107
				state = Job.ABORTED
			else:
				state = Job.FAILED

			if state == Job.SUCCESS:
				if not self._outputProcessor.process(outputdir):
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


	def cancel(self, wms, jobs, interactive = False, showJobs = True):
		if len(jobs) == 0:
			return
		if showJobs:
			self._reportClass(self.jobDB, self._task, jobs).display()
		if interactive and not utils.getUserBool('Do you really want to cancel these jobs?', True):
			return

		def mark_cancelled(jobNum):
			jobObj = self.jobDB.get(jobNum)
			if jobObj is None:
				return
			self._update(jobObj, jobNum, Job.CANCELLED)
			self._eventhandler.onJobUpdate(wms, jobObj, jobNum, {'reason': 'cancelled'})

		jobs.reverse()
		for (jobNum, wmsId) in wms.cancelJobs(self._wmsArgs(jobs)):
			# Remove deleted job from todo list and mark as cancelled
			assert(self.jobDB.get(jobNum).wmsId == wmsId)
			jobs.remove(jobNum)
			mark_cancelled(jobNum)

		if len(jobs) > 0:
			self._log_user.warning('There was a problem with cancelling the following jobs:')
			self._reportClass(self.jobDB, self._task, jobs).display()
			if (interactive and utils.getUserBool('Do you want to mark them as cancelled?', True)) or not interactive:
				lmap(mark_cancelled, jobs)
		if interactive:
			utils.wait(2)


	def delete(self, wms, select):
		selector = AndJobSelector(ClassSelector(JobClass.PROCESSING), JobSelector.create(select, task = self._task))
		jobs = self.jobDB.getJobs(selector)
		if jobs:
			self._log_user.warning('Cancelling the following jobs:')
			self.cancel(wms, jobs, True)


	def reset(self, wms, select):
		jobs = self.jobDB.getJobs(JobSelector.create(select, task = self._task))
		if jobs:
			self._log_user.warning('Resetting the following jobs:')
			self._reportClass(self.jobDB, self._task, jobs).display()
			if utils.getUserBool('Are you sure you want to reset the state of these jobs?', False):
				self.cancel(wms, self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING), jobs), False, False)
				for jobNum in jobs:
					self.jobDB.commit(jobNum, Job())


	# Process changes of job states requested by task module
	def _processIntervention(self, wms, jobChanges):
		def resetState(jobs, newState):
			jobSet = set(jobs)
			for jobNum in jobs:
				jobObj = self.jobDB.get(jobNum)
				if jobObj and jobObj.state in [ Job.INIT, Job.DISABLED, Job.ABORTED, Job.CANCELLED, Job.DONE, Job.FAILED, Job.SUCCESS ]:
					self._update(jobObj, jobNum, newState)
					jobSet.remove(jobNum)
					jobObj.attempt = 0
			if len(jobSet) > 0:
				output = (Job.enum2str(newState), str.join(', ', imap(str, jobSet)))
				raise JobError('For the following jobs it was not possible to reset the state to %s:\n%s' % output)

		if jobChanges:
			(redo, disable, sizeChange) = jobChanges
			if (redo == []) and (disable == []) and (sizeChange is False):
				return
			self._log_user_time.info('The task module has requested changes to the job database')
			newMaxJobs = self.getMaxJobs(self._task)
			applied_change = False
			if newMaxJobs != self.jobDB.jobLimit:
				self._log_user_time.info('Number of jobs changed from %d to %d', len(self.jobDB), newMaxJobs)
				self.jobDB.jobLimit = newMaxJobs
				applied_change = True
			if redo:
				self.cancel(wms, self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING), redo))
				resetState(redo, Job.INIT)
				applied_change = True
			if disable:
				self.cancel(wms, self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING), disable))
				resetState(disable, Job.DISABLED)
				applied_change = True
			if applied_change:
				self._log_user_time.info('All requested changes are applied')


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
			self._log_user_time.log(logging.INFO1, 'Verification mode active')
			self._log_user_time.log(logging.INFO1, 'Submission is capped unless the success ratio of a chunk of jobs is sufficent.')
			self._log_user_time.debug('Enforcing the following (chunksize x ratio) sequence:')
			self._log_user_time.debug(str.join(' > ', imap(lambda tpl: '%d x %4.2f'%(tpl[0], tpl[1]), izip(self._verifyChunks, self._verifyThresh))))
		self._unreachableGoal = False


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


	def _getSubmissionJobs(self, maxsample):
		result = JobManager._getSubmissionJobs(self, maxsample)
		if self._verify:
			return result[:self._getVerificationSubmitThrottle(len(result))]
		return result


	# Verification heuristic - check whether enough jobs have succeeded before submitting more
	# @submitCount: number of jobs to submit
	def _getVerificationSubmitThrottle(self, submitCount):
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
					self._log_user_time.warning('All remaining jobs are vetoed by an unachieveable verification goal!')
					self._log_user_time.info('Current goal: %d successful jobs out of %d', goal, self._verifyChunks[verifyIndex])
					self._unreachableGoal = True
				return 0
			if successRatio < self._verifyThresh[verifyIndex]:
				return min(submitCount, self._verifyChunks[verifyIndex]-jobsTotal)
			else:
				return min(submitCount, self._verifyChunks[verifyIndex+1]-jobsTotal)
		except IndexError:
			self._log_user_time.debug('All verification chunks passed')
			self._log_user_time.debug('Verification submission throttle disabled')
			self._verify = False
			return submitCount
