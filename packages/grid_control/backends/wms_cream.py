# | Copyright 2016 Karlsruhe Institute of Technology
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
from grid_control.backends.backend_tools import CheckInfo, CheckJobsViaArguments
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_grid import GridWMS
from grid_control.job_db import Job
from python_compat import imap, irange, md5, tarfile

class CREAM_CheckJobs(CheckJobsViaArguments):
	def __init__(self, config):
		CheckJobsViaArguments.__init__(self, config)
		self._check_exec = utils.resolveInstallPath('glite-ce-job-status')
		self._status_map = {
			'ABORTED':        Job.ABORTED,
			'CANCELLED':      Job.ABORTED,
			'DONE-FAILED':    Job.DONE,
			'DONE-OK':        Job.DONE,
			'HELD':           Job.WAITING,
			'IDLE':           Job.QUEUED,
			'PENDING':        Job.WAITING,
			'REALLY-RUNNING': Job.RUNNING,
			'REGISTERED':     Job.QUEUED,
			'RUNNING':        Job.RUNNING,
			'UNKNOWN':        Job.UNKNOWN,
		}

	def _arguments(self, wmsIDs):
		return [self._check_exec, '--level', '0', '--logfile', '/dev/stderr'] + wmsIDs

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
	alias = ['cream']

	def __init__(self, config, name):
		GridWMS.__init__(self, config, name, checkExecutor = CREAM_CheckJobs(config))
		self._nJobsPerChunk = config.getInt('job chunk size', 10, onChange = None)

		self._submitExec = utils.resolveInstallPath('glite-ce-job-submit')
		self._outputExec = utils.resolveInstallPath('glite-ce-job-output')
		self._cancelExec = utils.resolveInstallPath('glite-ce-job-cancel')
		self._purgeExec = utils.resolveInstallPath('glite-ce-job-purge')
		self._submitParams.update({'-r': self._ce, '--config-vo': self._configVO })

		self._outputRegex = r'.*For JobID \[(?P<rawId>\S+)\] output will be stored in the dir (?P<outputDir>.*)$'
		
		self._useDelegate = False
		if self._useDelegate is False:
			self._submitParams.update({ '-a': ' ' })

	def makeJDL(self, jobNum, module):
		return ['[\n'] + GridWMS.makeJDL(self, jobNum, module) + ['OutputSandboxBaseDestUri = "gsiftp://localhost";\n]']

	# Get output of jobs and yield output dirs
	def _getJobsOutput(self, allIds):
		if len(allIds) == 0:
			raise StopIteration

		basePath = os.path.join(self._outputPath, 'tmp')
		try:
			if len(allIds) == 1:
				# For single jobs create single subdir
				basePath = os.path.join(basePath, md5(allIds[0][0]).hexdigest())
			utils.ensureDirExists(basePath)
		except Exception:
			raise BackendError('Temporary path "%s" could not be created.' % basePath, BackendError)
		
		activity = utils.ActivityLog('retrieving job outputs')
		for ids in imap(lambda x: allIds[x:x+self._nJobsPerChunk], irange(0, len(allIds), self._nJobsPerChunk)):
			jobNumMap = dict(ids)
			jobs = ' '.join(self._getRawIDs(ids))
			log = tempfile.mktemp('.log')

			#print self._outputExec, '--noint --logfile "%s" --dir "%s" %s' % (log, basePath, jobs)
			#import sys
			#sys.exit(1)
			proc = utils.LoggedProcess(self._outputExec,
				'--noint --logfile "%s" --dir "%s" %s' % (log, basePath, jobs))

			# yield output dirs
			todo = jobNumMap.values()
			done = []
			currentJobNum = None
			for line in imap(str.strip, proc.iter()):
				match = re.match(self._outputRegex, line)
				if match:
					currentJobNum = jobNumMap.get(self._createId(match.groupdict()['rawId']))
					todo.remove(currentJobNum)
					done.append(match.groupdict()['rawId'])
					outputDir = match.groupdict()['outputDir']
					if os.path.exists(outputDir):
						if 'GC_WC.tar.gz' in os.listdir(outputDir):
							wildcardTar = os.path.join(outputDir, 'GC_WC.tar.gz')
							try:
								tarfile.TarFile.open(wildcardTar, 'r:gz').extractall(outputDir)
								os.unlink(wildcardTar)
							except Exception:
								utils.eprint("Can't unpack output files contained in %s" % wildcardTar)
					yield (currentJobNum, outputDir)
					currentJobNum = None
			retCode = proc.wait()

			if retCode != 0:
				if 'Keyboard interrupt raised by user' in proc.getError():
					utils.removeFiles([log, basePath])
					raise StopIteration
				else:
					proc.logError(self.errorLog, log = log)
				utils.eprint('Trying to recover from error ...')
				for dirName in os.listdir(basePath):
					yield (None, os.path.join(basePath, dirName))
		del activity

		# return unretrievable jobs
		for jobNum in todo:
			yield (jobNum, None)
		
		purgeLog = tempfile.mktemp('.log')
		purgeProc = utils.LoggedProcess(self._purgeExec, '--noint --logfile "%s" %s' % (purgeLog, " ".join(done)))
		retCode = purgeProc.wait()
		if retCode != 0:
			if self.explainError(purgeProc, retCode):
				pass
			else:
				proc.logError(self.errorLog, log = purgeLog, jobs = done)
		utils.removeFiles([log, purgeLog, basePath])

	def cancelJobs(self, allIds):
		if len(allIds) == 0:
			raise StopIteration

		waitFlag = False
		for ids in imap(lambda x: allIds[x:x+self._nJobsPerChunk], irange(0, len(allIds), self._nJobsPerChunk)):
			# Delete jobs in groups of 5 - with 5 seconds between groups
			if waitFlag and not utils.wait(5):
				break
			waitFlag = True

			jobNumMap = dict(ids)
			jobs = ' '.join(self._getRawIDs(ids))
			log = tempfile.mktemp('.log')

			activity = utils.ActivityLog('cancelling jobs')
			proc = utils.LoggedProcess(self._cancelExec, '--noint --logfile "%s" %s' % (log, jobs))
			retCode = proc.wait()
			del activity

			# select cancelled jobs
			for rawId in self._getRawIDs(ids):
				deletedWMSId = self._createId(rawId)
				yield (jobNumMap.get(deletedWMSId), deletedWMSId)

			if retCode != 0:
				if self.explainError(proc, retCode):
					pass
				else:
					proc.logError(self.errorLog, log = log)
		
			purgeLog = tempfile.mktemp('.log')
			purgeProc = utils.LoggedProcess(self._purgeExec, '--noint --logfile "%s" %s' % (purgeLog, jobs))
			retCode = purgeProc.wait()
			if retCode != 0:
				if self.explainError(purgeProc, retCode):
					pass
				else:
					proc.logError(self.errorLog, log = purgeLog, jobs = jobs)
			
			utils.removeFiles([log, purgeLog])
