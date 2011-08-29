import sys, os, re, fnmatch, random, math, time, operator
from grid_control import QM, ConfigError, RuntimeError, RethrowError, Job, Report, utils, JobSelector
from python_compat import *

class JobClass:
	mkJobClass = lambda *fList: (reduce(operator.add, map(lambda f: 1 << f, fList)), fList)
	ATWMS = mkJobClass(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED)
	RUNNING = mkJobClass(Job.RUNNING)
	PROCESSING = mkJobClass(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING)
	READY = mkJobClass(Job.INIT, Job.FAILED, Job.ABORTED, Job.CANCELLED)
	DONE = mkJobClass(Job.DONE)
	SUCCESS = mkJobClass(Job.SUCCESS)
	DISABLED = mkJobClass(Job.DISABLED)
	ENDSTATE = mkJobClass(Job.SUCCESS, Job.DISABLED)


class AndJobSelector: # Internally used
	def __init__(self, *args):
		self.selectors = args

	def __call__(self, jobNum, jobObj):
		return reduce(operator.and_, map(lambda selector: selector(jobNum, jobObj), self.selectors))


class ClassSelector(JobSelector):
	def __init__(self, arg, **kwargs):
		self.states = arg[1]

	def __call__(self, jobNum, jobObj):
		return jobObj.state in self.states


class JobDB:
	def __init__(self, config, jobLimit = -1, jobSelector = None):
		(self.jobLimit, self.alwaysSelector) = (jobLimit, jobSelector)
		self.dbPath = os.path.join(config.workDir, 'jobs')
		self.disableLog = os.path.join(config.workDir, 'disabled')
		try:
			if not os.path.exists(self.dbPath):
				if config.opts.init:
					os.mkdir(self.dbPath)
				else:
					raise ConfigError("Not a properly initialized work directory '%s'." % config.workDir)
		except IOError:
			raise RethrowError("Problem creating work directory '%s'" % self.dbPath)

		candidates = fnmatch.filter(os.listdir(self.dbPath), 'job_*.txt')
		(self._jobs, log, maxJobs) = ({}, None, len(candidates))
		for idx, jobFile in enumerate(candidates):
			if (jobLimit >= 0) and (len(self._jobs) >= jobLimit):
				utils.eprint('Stopped reading job infos! The number of job infos in the work directory (%d)' % len(self._jobs), newline = False)
				utils.eprint('is larger than the maximum number of jobs (%d)' % jobLimit)
				break
			try: # 2xsplit is faster than regex
				jobNum = int(jobFile.split(".")[0].split("_")[1])
			except:
				continue
			jobObj = Job.load(os.path.join(self.dbPath, jobFile))
			self._jobs[jobNum] = jobObj
			if idx % 100 == 0:
				del log
				log = utils.ActivityLog('Reading job infos ... %d [%d%%]' % (idx, (100.0 * idx) / maxJobs))
		self.jobLimit = max(self.jobLimit, len(self._jobs))


	def get(self, jobNum, default = None, create = False):
		if create:
			self._jobs[jobNum] = self._jobs.get(jobNum, Job())
		return self._jobs.get(jobNum, default)


	def getJobsIter(self, jobSelector = None, subset = None):
		if subset == None:
			subset = range(self.jobLimit)
		if jobSelector and self.alwaysSelector:
			select = lambda *args: jobSelector(*args) and self.alwaysSelector(*args)
		elif jobSelector or self.alwaysSelector:
			select = QM(jobSelector, jobSelector, self.alwaysSelector)
		else:
			for jobNum in subset:
				yield jobNum
			raise StopIteration
		for jobNum in subset:
			if select(jobNum, self.get(jobNum, Job())):
				yield jobNum


	def getJobs(self, jobSelector = None, subset = None):
		return list(self.getJobsIter(jobSelector, subset))


	def getJobsN(self, jobSelector = None, subset = None):
		counter = 0
		for jobNum in self.getJobsIter(jobSelector, subset):
			counter += 1
		return counter


	def commit(self, jobNum, jobObj):
		jobObj.save(os.path.join(self.dbPath, 'job_%d.txt' % jobNum))
#		if jobObj.state == Job.DISABLED:


	def __len__(self):
		return self.jobLimit


	def logDisabled(self):
		disabled = self.jobDB.getJobs(ClassSelector(JobClass.DISABLED))
		try:
			open(self.disableLog, 'w').write(str.join('\n', map(str, disabled)))
		except:
			raise RuntimeError('Could not write disabled jobs to file %s!' % (jobNum, self.disableLog))
		if len(disabled) > 0:
			utils.vprint('There are %d disabled jobs in this task!' % len(disabled), -1, True)
			utils.vprint('Please refer to %s for a complete list.' % self.disableLog, -1, True)


class JobManager:
	def __init__(self, config, module, monitor):
		(self.module, self.monitor) = (module, monitor)
		self.jobLimit = config.getInt('jobs', 'jobs', -1, volatile=True)
		selected = JobSelector.create(config.get('jobs', 'selected', '', volatile=True), module = self.module)
		self.jobDB = JobDB(config, self.getMaxJobs(self.module), selected)

		self.timeout = utils.parseTime(config.get('jobs', 'queue timeout', '', volatile=True))
		self.inFlight = config.getInt('jobs', 'in flight', -1, volatile=True)
		self.inQueue = config.getInt('jobs', 'in queue', -1, volatile=True)
		self.doShuffle = config.getBool('jobs', 'shuffle', False, volatile=True)
		self.maxRetry = config.getInt('jobs', 'max retry', -1, volatile=True)
		self.continuous = config.getBool('jobs', 'continuous', False, volatile=True)

		# Job offender heuristic (not persistent!) - remove jobs, which do not report their status
		self.kickOffender = config.getInt('jobs', 'kick offender', 10, volatile=True)
		(self.offender, self.raster) = ({}, 0)


	def getMaxJobs(self, module):
		nJobs = self.jobLimit
		if nJobs < 0:
			# No valid number of jobs given in config file - module has to provide number of jobs
			nJobs = module.getMaxJobs()
			if nJobs == None:
				raise ConfigError("Module doesn't provide max number of Jobs!")
		else:
			# Module doesn't have to provide number of jobs
			try:
				maxJobs = module.getMaxJobs()
				if maxJobs and (nJobs > maxJobs):
					print 'Maximum number of jobs given as %d was truncated to %d' % (nJobs, maxJobs)
					nJobs = maxJobs
			except:
				pass
		return nJobs


	def _update(self, jobObj, jobNum, state):
		if jobObj.state == state:
			return

		oldState = jobObj.state
		jobObj.update(state)
		self.jobDB.commit(jobNum, jobObj)

		jobNumLen = int(math.log10(max(1, len(self.jobDB))) + 1)
		utils.vprint('Job %s state changed from %s to %s ' % (str(jobNum).ljust(jobNumLen), Job.states[oldState], Job.states[state]), -1, True, False)
		if (state == Job.SUBMITTED) and (jobObj.attempt > 1):
			print '(retry #%s)' % (jobObj.attempt - 1)
		elif (state == Job.QUEUED) and jobObj.get('dest') != 'N/A':
			print '(%s)' % jobObj.get('dest')
		elif (state in [Job.WAITING, Job.ABORTED, Job.DISABLED]) and jobObj.get('reason'):
			print '(%s)' % jobObj.get('reason')
		elif (state == Job.SUCCESS) and jobObj.get('runtime', None) != None:
			print '(runtime %s)' % utils.strTime(QM(jobObj.get('runtime') != '', jobObj.get('runtime'), 0))
		elif (state == Job.FAILED):
			msg = []
			if jobObj.get('retcode'):
				msg.append('error code: %d' % jobObj.get('retcode'))
				try:
					if utils.verbosity() > 0:
						msg.append(self.module.errorDict[jobObj.get('retcode')])
				except:
					pass
			if jobObj.get('dest'):
				msg.append(jobObj.get('dest'))
			if len(msg):
				print '(%s)' % str.join(' - ', msg),
			print
		else:
			print


	def sample(self, jobList, size):
		if size >= 0:
			jobList = random.sample(jobList, min(size, len(jobList)))
		return sorted(jobList)


	def getSubmissionJobs(self, maxsample, static = {'showBlocker': True}):
		# Get list of submittable jobs
		readyList = self.jobDB.getJobs(ClassSelector(JobClass.READY))
		retryOK = readyList
		if self.maxRetry >= 0:
			retryOK = filter(lambda x: self.jobDB.get(x, Job()).attempt - 1 < self.maxRetry, readyList)
		modOK = filter(self.module.canSubmit, readyList)
		jobList = set.intersection(set(retryOK), set(modOK))

		if static['showBlocker'] and len(readyList) > 0 and len(jobList) == 0: # No submission but ready jobs
			err = []
			err += QM(len(retryOK) > 0 and len(modOK) == 0, [], ['have hit their maximum number of retries'])
			err += QM(len(retryOK) == 0 and len(modOK) > 0, [], ['are vetoed by the job module'])
			utils.vprint('All remaining jobs %s!' % str.join(QM(retryOK or modOK, ' or ', ' and '), err), -1, True)
		static['showBlocker'] = not (len(readyList) > 0 and len(jobList) == 0)

		# Determine number of jobs to submit
		submit = len(jobList)
		if self.inQueue > 0:
			submit = min(submit, self.inQueue - len(self.jobDB.getJobs(ClassSelector(JobClass.ATWMS))))
		if self.inFlight > 0:
			submit = min(submit, self.inFlight - len(self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING))))
		if self.continuous:
			submit = min(submit, maxsample)
		submit = max(submit, 0)

		if self.doShuffle:
			return self.sample(jobList, submit)
		else:
			return sorted(jobList[:submit])


	def submit(self, wms, maxsample = 100):
		jobList = self.getSubmissionJobs(maxsample)
		if len(jobList) == 0:
			return False

		if not wms.bulkSubmissionBegin(len(jobList)):
			return False
		try:
			for jobNum, wmsId, data in wms.submitJobs(jobList):
				jobObj = self.jobDB.get(jobNum, create = True)

				if wmsId == None:
					# Could not register at WMS
					self._update(jobObj, jobNum, Job.FAILED)
					continue

				jobObj.assignId(wmsId)
				for key, value in data.iteritems():
					jobObj.set(key, value)

				self._update(jobObj, jobNum, Job.SUBMITTED)
				self.monitor.onJobSubmit(wms, jobObj, jobNum)
				if utils.abort():
					return False
			return True
		finally:
			wms.bulkSubmissionEnd()


	def wmsArgs(self, jobList):
		return map(lambda jobNum: (self.jobDB.get(jobNum).wmsId, jobNum), jobList)


	def check(self, wms, maxsample = 100):
		(change, timeoutList) = (False, [])
		jobList = self.sample(self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING)), QM(self.continuous, maxsample, -1))

		if self.kickOffender:
			nOffender = len(self.offender) # Waiting list gets larger in case reported == []
			waitList = self.sample(self.offender, nOffender - max(1, nOffender / 2**self.raster))
			jobList = filter(lambda x: x not in waitList, jobList)

		reported = []
		for jobNum, wmsId, state, info in wms.checkJobs(self.wmsArgs(jobList)):
			if jobNum in self.offender:
				self.offender.pop(jobNum)
			reported.append(jobNum)
			jobObj = self.jobDB.get(jobNum)
			if state != jobObj.state:
				change = True
				for key, value in info.items():
					jobObj.set(key, value)
				self._update(jobObj, jobNum, state)
				self.monitor.onJobUpdate(wms, jobObj, jobNum, info)
			else:
				# If a job stays too long in an inital state, cancel it
				if jobObj.state in (Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED):
					if self.timeout > 0 and time.time() - jobObj.submitted > self.timeout:
						timeoutList.append(jobNum)
			if utils.abort():
				return False

		if self.kickOffender:
			self.raster = QM(reported, 1, self.raster + 1) # make "raster" iteratively smaller
			for jobNum in filter(lambda x: x not in reported, jobList):
				self.offender[jobNum] = self.offender.get(jobNum, 0) + 1
			kickList = filter(lambda jobNum: self.offender[jobNum] >= self.kickOffender, self.offender)
			for jobNum in set(list(kickList) + QM((len(reported) == 0) and (len(jobList) == 1), jobList, [])):
				timeoutList.append(jobNum)
				self.offender.pop(jobNum)

		# Cancel jobs who took too long
		if len(timeoutList):
			change = True
			print '\nTimeout for the following jobs:'
			self.cancel(wms, timeoutList)

		# Process module interventions
		self.processIntervention(wms, self.module.getIntervention())

		# Quit when all jobs are finished
		if len(self.jobDB.getJobs(ClassSelector(JobClass.ENDSTATE))) == len(self.jobDB):
			self.jobDB.logDisabled()
			self.monitor.onTaskFinish(len(self.jobDB))
			if self.module.onTaskFinish():
				utils.vprint('Task successfully completed. Quitting grid-control!', -1, True)
				sys.exit(0)

		return change


	def retrieve(self, wms, maxsample = 10):
		change = False
		jobList = self.sample(self.jobDB.getJobs(ClassSelector(JobClass.DONE)), QM(self.continuous, maxsample, -1))

		for jobNum, retCode, data in wms.retrieveJobs(self.wmsArgs(jobList)):
			jobObj = self.jobDB.get(jobNum)
			if jobObj == None:
				continue

			if retCode == 0:
				state = Job.SUCCESS
			else:
				state = Job.FAILED

			if state != jobObj.state:
				change = True
				jobObj.set('retcode', retCode)
				jobObj.set('runtime', data.get('TIME', -1))
				self._update(jobObj, jobNum, state)
				self.monitor.onJobOutput(wms, jobObj, jobNum, retCode)

			if utils.abort():
				return False

		return change


	def cancel(self, wms, jobs, interactive = False):
		if len(jobs) == 0:
			return
		Report(self.jobDB, jobs).details()
		if interactive and not utils.getUserBool('Do you really want to cancel these jobs?', True):
			return

		def mark_cancelled(jobNum):
			jobObj = self.jobDB.get(jobNum)
			if jobObj == None:
				return
			self._update(jobObj, jobNum, Job.CANCELLED)
			self.monitor.onJobUpdate(wms, jobObj, jobNum, {'status': 'cancelled'})

		jobs.reverse()
		for (wmsId, jobNum) in wms.cancelJobs(self.wmsArgs(jobs)):
			# Remove deleted job from todo list and mark as cancelled
			jobs.remove(jobNum)
			mark_cancelled(jobNum)

		if len(jobs) > 0:
			print '\nThere was a problem with cancelling the following jobs:'
			Report(self.jobDB, jobs).details()
			if (interactive and utils.getUserBool('Do you want to mark them as cancelled?', True)) or not interactive:
				map(mark_cancelled, jobs)


	def delete(self, wms, select):
		selector = AndJobSelector(ClassSelector(JobClass.PROCESSING), JobSelector.create(select, module = self.module))
		jobs = self.jobDB.getJobs(selector)
		if jobs:
			print '\nCancelling the following jobs:'
			self.cancel(wms, jobs, True)


	# Process changes of job states requested by job module
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
				output = (Job.states[newState], str.join(', ', map(str, jobSet)))
				raise RuntimeError('For the following jobs it was not possible to reset the state to %s:\n%s' % output)

		if jobChanges:
			(redo, disable) = jobChanges
			newMaxJobs = self.getMaxJobs(self.module)
			if (redo == []) and (disable == []) and (len(self.jobDB) == newMaxJobs):
				return
			utils.vprint('The job module has requested changes to the job database', -1, True)
			if len(self.jobDB) != newMaxJobs:
				utils.vprint('Number of jobs changed from %d to %d' % (len(self.jobDB), newMaxJobs), -1, True)
				self.jobDB.jobLimit = newMaxJobs
			self.cancel(wms, self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING), redo))
			resetState(redo, Job.INIT)
			self.cancel(wms, self.jobDB.getJobs(ClassSelector(JobClass.PROCESSING), disable))
			resetState(disable, Job.DISABLED)
			utils.vprint('All requested changes are applied', -1, True)
