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
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_grid import GridWMS
from grid_control.job_db import Job
from grid_control.utils.activity import Activity
from grid_control.utils.process_base import LocalProcess
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
	alias_list = ['cream']

	def __init__(self, config, name):
		cancel_executor = CancelAndPurgeJobs(config, CREAM_CancelJobs(config), CREAM_PurgeJobs(config))
		GridWMS.__init__(self, config, name, check_executor = CREAM_CheckJobs(config),
			cancel_executor = ChunkedExecutor(config, 'cancel', cancel_executor))

		self._job_lenPerChunk = config.get_int('job chunk size', 10, on_change = None)

		self._submitExec = utils.resolve_install_path('glite-ce-job-submit')
		self._outputExec = utils.resolve_install_path('glite-ce-job-output')
		self._submitParams.update({'-r': self._ce, '--config-vo': self._configVO })

		self._outputRegex = r'.*For JobID \[(?P<rawId>\S+)\] output will be stored in the dir (?P<outputDir>.*)$'

		self._useDelegate = False
		if self._useDelegate is False:
			self._submitParams.update({ '-a': ' ' })

	def makeJDL(self, jobnum, module):
		return ['[\n'] + GridWMS.makeJDL(self, jobnum, module) + ['OutputSandboxBaseDestUri = "gsiftp://localhost";\n]']

	# Get output of jobs and yield output dirs
	def _get_jobs_output(self, allIds):
		if len(allIds) == 0:
			raise StopIteration

		basePath = os.path.join(self._path_output, 'tmp')
		try:
			if len(allIds) == 1:
				# For single jobs create single subdir
				basePath = os.path.join(basePath, md5(allIds[0][0]).hexdigest())
			utils.ensure_dir_exists(basePath)
		except Exception:
			raise BackendError('Temporary path "%s" could not be created.' % basePath, BackendError)
		
		activity = Activity('retrieving %d job outputs' % len(allIds))
		for ids in imap(lambda x: allIds[x:x+self._job_lenPerChunk], irange(0, len(allIds), self._job_lenPerChunk)):
			jobnumMap = dict(ids)
			jobs = ' '.join(self._iter_wms_ids(ids))
			log = tempfile.mktemp('.log')

			proc = LocalProcess(self._outputExec, '--noint', '--logfile', log, '--dir', basePath, jobs)

			# yield output dirs
			todo = jobnumMap.values()
			done = []
			currentJobNum = None
			for line in imap(str.strip, proc.stdout.iter(timeout = 20)):
				match = re.match(self._outputRegex, line)
				if match:
					currentJobNum = jobnumMap.get(self._create_gc_id(match.groupdict()['rawId']))
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
			exit_code = proc.status(timeout = 10, terminate = True)

			if exit_code != 0:
				if 'Keyboard interrupt raised by user' in proc.stdout.read_log():
					utils.remove_files([log, basePath])
					raise StopIteration
				else:
					self._log.log_process(proc)
				self._log.error('Trying to recover from error ...')
				for dirName in os.listdir(basePath):
					yield (None, os.path.join(basePath, dirName))
		activity.finish()

		# return unretrievable jobs
		for jobnum in todo:
			yield (jobnum, None)

		purgeLog = tempfile.mktemp('.log')
		purgeProc = LocalProcess(utils.resolve_install_path('glite-ce-job-purge'),
			'--noint', '--logfile', purgeLog, str.join(' ', done))
		exit_code = purgeProc.status(timeout = 60)
		if exit_code != 0:
			if self.explainError(purgeProc, exit_code):
				pass
			else:
				self._log.log_process(proc)
		utils.remove_files([log, purgeLog, basePath])
