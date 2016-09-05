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
from grid_control.backends.aspect_cancel import CancelAndPurgeJobs, CancelJobsWithProcessBlind
from grid_control.backends.aspect_status import CheckInfo, CheckJobsWithProcess
from grid_control.backends.backend_tools import ChunkedExecutor, ProcessCreatorAppendArguments
from grid_control.backends.logged_process import LoggedProcess
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_grid import GridWMS
from grid_control.job_db import Job
from grid_control.utils.activity import Activity
from python_compat import imap, irange, md5, tarfile

class CREAM_CheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		proc_factory = ProcessCreatorAppendArguments(config,
			'glite-ce-job-status', ['--level', '0', '--logfile', '/dev/stderr'])
		CheckJobsWithProcess.__init__(self, config, proc_factory, status_map = {
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


class CREAM_PurgeJobs(CancelJobsWithProcessBlind):
	def __init__(self, config):
		CancelJobsWithProcessBlind.__init__(self, config,
			'glite-ce-job-purge', ['--noint', '--logfile', '/dev/stderr'])


class CREAM_CancelJobs(CancelJobsWithProcessBlind):
	def __init__(self, config):
		CancelJobsWithProcessBlind.__init__(self, config,
			'glite-ce-job-cancel', ['--noint', '--logfile', '/dev/stderr'])


class CreamWMS(GridWMS):
	alias = ['cream']

	def __init__(self, config, name):
		cancelExecutor = CancelAndPurgeJobs(config, CREAM_CancelJobs(config), CREAM_PurgeJobs(config))
		GridWMS.__init__(self, config, name, checkExecutor = CREAM_CheckJobs(config),
			cancelExecutor = ChunkedExecutor(config, 'cancel', cancelExecutor))

		self._nJobsPerChunk = config.getInt('job chunk size', 10, onChange = None)

		self._submitExec = utils.resolveInstallPath('glite-ce-job-submit')
		self._outputExec = utils.resolveInstallPath('glite-ce-job-output')
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
		
		activity = Activity('retrieving %d job outputs' % len(allIds))
		for ids in imap(lambda x: allIds[x:x+self._nJobsPerChunk], irange(0, len(allIds), self._nJobsPerChunk)):
			jobNumMap = dict(ids)
			jobs = ' '.join(self._getRawIDs(ids))
			log = tempfile.mktemp('.log')

			proc = LoggedProcess(self._outputExec,
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
								self._log.error('Can\'t unpack output files contained in %s', wildcardTar)
					yield (currentJobNum, outputDir)
					currentJobNum = None
			retCode = proc.wait()

			if retCode != 0:
				if 'Keyboard interrupt raised by user' in proc.getError():
					utils.removeFiles([log, basePath])
					raise StopIteration
				else:
					proc.logError(self.errorLog, log = log)
				self._log.error('Trying to recover from error ...')
				for dirName in os.listdir(basePath):
					yield (None, os.path.join(basePath, dirName))
		activity.finish()

		# return unretrievable jobs
		for jobNum in todo:
			yield (jobNum, None)

		purgeLog = tempfile.mktemp('.log')
		purgeProc = LoggedProcess(utils.resolveInstallPath('glite-ce-job-purge'),
			'--noint --logfile "%s" %s' % (purgeLog, str.join(' ', done)))
		retCode = purgeProc.wait()
		if retCode != 0:
			if self.explainError(purgeProc, retCode):
				pass
			else:
				proc.logError(self.errorLog, log = purgeLog, jobs = done)
		utils.removeFiles([log, purgeLog, basePath])
