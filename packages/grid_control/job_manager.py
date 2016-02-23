#-#  Copyright 2007-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import math, time, bisect, random, logging
from grid_control import utils
from grid_control.config import ConfigError
from grid_control.gc_plugin import NamedPlugin
from grid_control.job_db import Job, JobClass, JobDB, JobError
from grid_control.job_selector import AndJobSelector, ClassSelector, JobSelector
from grid_control.output_processor import TaskOutputProcessor
from grid_control.report import Report
from grid_control.utils.parsing import strTime
from python_compat import ifilter, imap, lfilter, lmap, lzip, set, sorted

class JobManager(NamedPlugin):
	configSections = NamedPlugin.configSections + ['jobs']
	tagName = 'jobmgr'

	def __init__(self, config, name, task, eventhandler):
		NamedPlugin.__init__(self, config, name)
		(self._task, self._eventhandler) = (task, eventhandler)
		self._log_user = logging.getLogger('user')
		self._log_user_time = logging.getLogger('user.time')
		self.jobLimit = config.getInt('jobs', -1, onChange = None)
		selected = JobSelector.create(config.get('selected', '', onChange = None), task = self._task)
		self.jobDB = config.getPlugin('jobdb', 'JobDB',
			cls = JobDB, pargs = (self.getMaxJobs(self._task), selected))
		self.disableLog = config.getWorkPath('disabled')
		self._outputProcessor = config.getPlugin('output processor', 'SandboxProcessor',
			cls = TaskOutputProcessor, pargs = (task,))

		self.timeout = config.getTime('queue timeout', -1, onChange = None)
		self.inFlight = config.getInt('in flight', -1, onChange = None)
		self.inQueue = config.getInt('in queue', -1, onChange = None)
		self.doShuffle = config.getBool('shuffle', False, onChange = None)
		self.maxRetry = config.getInt('max retry', -1, onChange = None)
		self.continuous = config.getBool('continuous', False, onChange = None)
		self._reportClass = Report.getClass(config.get('abort report', 'LocationReport', onChange = None))
		self._showBlocker = True


	def getMaxJobs(self, task):
		nJobs = self.jobLimit
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


	def logDisabled(self):
		disabled = self.jobDB.getJobs(ClassSelector(JobClass.DISABLED))
		try:
			open(self.disableLog, 'w').write(str.join('\n', imap(str, disabled)))
		except Exception:
			raise JobError('Could not write disabled jobs to file %s!' % self.disableLog)
		if len(disabled) > 0:
			utils.vprint('There are %d disabled jobs in this task!' % len(disabled), -1, True)
			utils.vprint('Please refer to %s for a complete list.' % self.disableLog, -1, True, once = True)


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
		elif (state == Job.QUEUED) and jobObj.get('dest') != 'N/A':
			jobStatus.append('(%s)' % jobObj.get('dest'))
		elif (state in [Job.WAITING, Job.ABORTED, Job.DISABLED]) and jobObj.get('reason'):
			jobStatus.append('(%s)' % jobObj.get('reason'))
		elif (state == Job.SUCCESS) and jobObj.get('runtime', None) is not None:
			jobStatus.append('(runtime %s)' % strTime(utils.QM(jobObj.get('runtime') != '', jobObj.get('runtime'), 0)))
		elif (state == Job.FAILED):
			msg = []
			if jobObj.get('retcode'):
				msg.append('error code: %d' % jobObj.get('retcode'))
				try:
					if utils.verbosity() > 0:
						msg.append(self._task.errorDict[jobObj.get('retcode')])
				except Exception:
					pass
			if jobObj.get('dest'):
				msg.append(jobObj.get('dest'))
			if len(msg):
				jobStatus.append('(%s)' % str.join(' - ', msg))
		self._log_user_time.info(str.join(' ', jobStatus))


	def sample(self, jobList, size):
		if size >= 0:
			jobList = random.sample(jobList, min(size, len(jobList)))
		return sorted(jobList)


	def getSubmissionJobs(self, maxsample):
		# Get list of submittable jobs
		readyList = self.jobDB.getJobs(ClassSelector(JobClass.READY))
		retryOK = readyList
		defaultJob = Job()
		if self.maxRetry >= 0:
			retryOK = lfilter(lambda x: self.jobDB.get(x, defaultJob).attempt - 1 < self.maxRetry, readyList)
		modOK = lfilter(self._task.canSubmit, readyList)
		jobList = set.intersection(set(retryOK), set(modOK))

		if self._showBlocker and len(readyList) > 0 and len(jobList) == 0: # No submission but ready jobs
			err = []
			err += utils.QM(len(retryOK) > 0 and len(modOK) == 0, [], ['have hit their maximum number of retries'])
			err += utils.QM(len(retryOK) == 0 and len(modOK) > 0, [], ['are vetoed by the task module'])
			utils.vprint('All remaining jobs %s!' % str.join(utils.QM(retryOK or modOK, ' or ', ' and '), err), -1, True)
		self._showBlocker = not (len(readyList) > 0 and len(jobList) == 0)

		# Determine number of jobs to submit
		submit = len(jobList)
		if self.inQueue > 0:
			submit = min(submit, self.inQueue - self.jobDB.getJobsN(ClassSelector(JobClass.ATWMS)))
		if self.inFlight > 0:
			submit = min(submit, self.inFlight - self.jobDB.getJobsN(ClassSelector(JobClass.PROCESSING)))
		if self.continuous:
			submit = min(submit, maxsample)
		submit = max(submit, 0)

		if self.doShuffle:
			return self.sample(jobList, submit)
		else:
			return sorted(jobList)[:submit]


	def submit(self, wms, maxsample = 100):
		jobList = self.getSubmissionJobs(maxsample)
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


	def wmsArgs(self, jobList):
		return lmap(lambda jobNum: (self.jobDB.get(jobNum).wmsId, jobNum), jobList)


	def checkJobList(self, wms, jobList):
		(change, timeoutList, reported) = (False, [], [])
		for (jobNum, _, state, info) in wms.checkJobs(self.wmsArgs(jobList)):
			if jobNum in self.offender:
				self.offender.pop(jobNum)
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
					if self.timeout > 0 and time.time() - jobObj.submitted > self.timeout:
						timeoutList.append(jobNum)
			if utils.abort():
				return (None, timeoutList, reported)
		return (change, timeoutList, reported)


	def check(self, wms, maxsample = 100):
		jobList = self.sample(self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING)), utils.QM(self.continuous, maxsample, -1))

		# Check jobs in the joblist and return changes, timeouts and successfully reported jobs
		(change, timeoutList, reported) = self.checkJobList(wms, jobList)
		if change is None: # neither True or False => abort
			return False

		# Cancel jobs which took too long
		if len(timeoutList):
			change = True
			self._log_user.warning('Timeout for the following jobs:')
			self.cancel(wms, timeoutList)

		# Process task interventions
		self.processIntervention(wms, self._task.getIntervention())

		# Quit when all jobs are finished
		if self.jobDB.getJobsN(ClassSelector(JobClass.ENDSTATE)) == len(self.jobDB):
			self.logDisabled()
			self._eventhandler.onTaskFinish(len(self.jobDB))
			if self._task.canFinish():
				utils.vprint('Task successfully completed. Quitting grid-control!', -1, True)
				utils.abort(True)

		return change


	def retrieve(self, wms, maxsample = 100):
		change = False
		jobList = self.sample(self.jobDB.getJobs(ClassSelector(JobClass.DONE)), utils.QM(self.continuous, maxsample, -1))

		for (jobNum, retCode, data, outputdir) in wms.retrieveJobs(self.wmsArgs(jobList)):
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
		for (jobNum, wmsId) in wms.cancelJobs(self.wmsArgs(jobs)):
			# Remove deleted job from todo list and mark as cancelled
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
	def processIntervention(self, wms, jobChanges):
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
			utils.vprint('The task module has requested changes to the job database', -1, True)
			if sizeChange:
				newMaxJobs = self.getMaxJobs(self._task)
				utils.vprint('Number of jobs changed from %d to %d' % (len(self.jobDB), newMaxJobs), -1, True)
				self.jobDB.jobLimit = newMaxJobs
			self.cancel(wms, self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING), redo))
			resetState(redo, Job.INIT)
			self.cancel(wms, self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING), disable))
			resetState(disable, Job.DISABLED)
			utils.vprint('All requested changes are applied', -1, True)


class SimpleJobManager(JobManager):
	def __init__(self, config, name, task, eventhandler):
		JobManager.__init__(self, config, name, task, eventhandler)

		# Job offender heuristic (not persistent!) - remove jobs, which do not report their status
		self.kickOffender = config.getInt('kick offender', 10, onChange = None)
		(self.offender, self.raster) = ({}, 0)
		# job verification heuristic - launch jobs in chunks of increasing size if enough jobs succeed
		self.verify = False
		self.verifyChunks = config.getList('verify chunks', [-1], onChange = None, parseItem = int)
		self.verifyThresh = config.getList('verify reqs', [0.5], onChange = None, parseItem = float)
		if self.verifyChunks != [-1]:
			self.verify = True
			self.verifyThresh += [self.verifyThresh[-1]] * (len(self.verifyChunks) - len(self.verifyThresh))
			utils.vprint('== Verification mode active ==\nSubmission is capped unless the success ratio of a chunk of jobs is sufficent.', level=0)
			utils.vprint('Enforcing the following (chunksize x ratio) sequence:', level=0)
			utils.vprint(' > '.join(imap(lambda tpl: '%d x %4.2f'%(tpl[0], tpl[1]), lzip(self.verifyChunks, self.verifyThresh))), level=0)
		self._unreachableGoal = False


	def checkJobList(self, wms, jobList):
		if self.kickOffender:
			nOffender = len(self.offender) # Waiting list gets larger in case reported == []
			waitList = self.sample(self.offender, nOffender - max(1, nOffender / 2**self.raster))
			jobList = lfilter(lambda x: x not in waitList, jobList)

		(change, timeoutList, reported) = JobManager.checkJobList(self, wms, jobList)
		if change is None:
			return (change, timeoutList, reported) # abort check

		if self.kickOffender:
			self.raster = utils.QM(reported, 1, self.raster + 1) # make 'raster' iteratively smaller
			for jobNum in ifilter(lambda x: x not in reported, jobList):
				self.offender[jobNum] = self.offender.get(jobNum, 0) + 1
			kickList = lfilter(lambda jobNum: self.offender[jobNum] >= self.kickOffender, self.offender)
			for jobNum in set(kickList + utils.QM((len(reported) == 0) and (len(jobList) == 1), jobList, [])):
				timeoutList.append(jobNum)
				self.offender.pop(jobNum)

		return (change, timeoutList, reported)


	def getSubmissionJobs(self, maxsample):
		result = JobManager.getSubmissionJobs(self, maxsample)
		if self.verify:
			return result[:self.getVerificationSubmitThrottle(len(result))]
		return result


	# Verification heuristic - check whether enough jobs have succeeded before submitting more
	# @submitCount: number of jobs to submit
	def getVerificationSubmitThrottle(self, submitCount):
		jobsActive = self.jobDB.getJobsN(ClassSelector(JobClass.PROCESSING))
		jobsSuccess = self.jobDB.getJobsN(ClassSelector(JobClass.SUCCESS))
		jobsDone = self.jobDB.getJobsN(ClassSelector(JobClass.PROCESSED))
		jobsTotal = jobsDone + jobsActive
		verifyIndex = bisect.bisect_left(self.verifyChunks, jobsTotal)
		try:
			successRatio = jobsSuccess * 1.0 / self.verifyChunks[verifyIndex]
			if ( self.verifyChunks[verifyIndex] - jobsDone) + jobsSuccess < self.verifyChunks[verifyIndex]*self.verifyThresh[verifyIndex]:
				if not self._unreachableGoal:
					utils.vprint('All remaining jobs are vetoed by an unachieveable verification goal.', -1, True)
					utils.vprint('Current goal: %d successful jobs out of %d' % (self.verifyChunks[verifyIndex]*self.verifyThresh[verifyIndex], self.verifyChunks[verifyIndex]), -1, True)
					self._unreachableGoal = True
				return 0
			if successRatio < self.verifyThresh[verifyIndex]:
				return min(submitCount, self.verifyChunks[verifyIndex]-jobsTotal)
			else:
				return min(submitCount, self.verifyChunks[verifyIndex+1]-jobsTotal)
		except IndexError:
			utils.vprint('== All verification chunks passed ==\nVerification submission throttle disabled.', level=0)
			self.verify = False
			return submitCount
