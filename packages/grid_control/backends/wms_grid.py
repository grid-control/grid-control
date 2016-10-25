# | Copyright 2009-2016 Karlsruhe Institute of Technology
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
from grid_control import utils
from grid_control.backends.aspect_cancel import CancelJobsWithProcess
from grid_control.backends.aspect_status import CheckInfo, CheckJobsWithProcess
from grid_control.backends.backend_tools import ProcessCreatorViaStdin
from grid_control.backends.broker_base import Broker
from grid_control.backends.jdl_writer import JDLWriter
from grid_control.backends.wms import BackendError, BasicWMS, WMS
from grid_control.job_db import Job
from grid_control.utils.activity import Activity
from grid_control.utils.file_objects import SafeFile
from grid_control.utils.process_base import LocalProcess
from python_compat import identity, ifilter, imap, lfilter, lmap, md5, parsedate, tarfile


GridStatusMap = {
	'aborted':   Job.ABORTED,
	'cancelled': Job.ABORTED,
	'cleared':   Job.ABORTED,
	'done':      Job.DONE,
	'failed':    Job.ABORTED,
	'queued':    Job.QUEUED,
	'ready':     Job.READY,
	'running':   Job.RUNNING,
	'scheduled': Job.QUEUED,
	'submitted': Job.SUBMITTED,
	'waiting':   Job.WAITING,
}


def jdlEscape(value):
	repl = { '\\': r'\\', '\"': r'\"', '\n': r'\n' }
	return '"' + str.join('', imap(lambda char: repl.get(char, char), value)) + '"'


class Grid_ProcessCreator(ProcessCreatorViaStdin):
	def __init__(self, config, cmd, args):
		ProcessCreatorViaStdin.__init__(self, config)
		(self._cmd, self._args) = (utils.resolve_install_path(cmd), args)

	def _arguments(self):
		return [self._cmd] + self._args

	def _stdin_message(self, wms_id_list):
		return str.join('\n', wms_id_list)


class Grid_CheckJobs(CheckJobsWithProcess):
	def __init__(self, config, check_exec):
		proc_factory = Grid_ProcessCreator(config, check_exec,
			['--verbosity', 1, '--noint', '--logfile', '/dev/stderr', '-i', '/dev/stdin'])
		CheckJobsWithProcess.__init__(self, config, proc_factory, status_map = GridStatusMap)

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
				return
		elif key.startswith('status reason'):
			job_info['reason'] = value
		elif key.startswith('reached') or key.startswith('submitted'):
			try:
				job_info['timestamp'] = int(calendar.timegm(parsedate(value)))
			except Exception:
				return
		elif key.startswith('bookkeeping information'):
			return
		elif value:
			job_info[key] = value

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
				continue

			key = key.lower()
			if key.startswith('status info'):
				yield job_info
				job_info = {CheckInfo.WMSID: value}
			else:
				self._fill(job_info, key, value)
		yield job_info

	def _handle_error(self, proc):
		self._filter_proc_log(proc, self._errormsg, discardlist = ['Keyboard interrupt raised by user'])


class Grid_CancelJobs(CancelJobsWithProcess):
	def __init__(self, config, cancel_exec):
		proc_factory = Grid_ProcessCreator(config, cancel_exec,
			['--noint', '--logfile', '/dev/stderr', '-i', '/dev/stdin'])
		CancelJobsWithProcess.__init__(self, config, proc_factory)

	def _parse(self, wms_id_list, proc): # yield list of (wms_id, job_status)
		for line in ifilter(lambda x: x.startswith('- '), proc.stdout.iter(self._timeout)):
			yield (line.strip('- \n'),)


class GridWMS(BasicWMS):
	config_section_list = BasicWMS.config_section_list + ['grid']
	def __init__(self, config, name, check_executor, cancel_executor, jdlWriter = None):
		config.set('access token', 'VomsProxy')
		BasicWMS.__init__(self, config, name, check_executor = check_executor, cancel_executor = cancel_executor)

		self.brokerSite = config.get_plugin('site broker', 'UserBroker',
			cls = Broker, bkwargs={'tags': [self]}, pargs = ('sites', 'sites', self.getSites))
		self.vo = config.get('vo', self._token.getGroup())

		self._submitParams = {}
		self._ce = config.get('ce', '', on_change = None)
		self._configVO = config.get_path('config', '', on_change = None)
		self._warnSBSize = config.get_int('warn sb size', 5, on_change = None)
		self._jobPath = config.get_work_path('jobs')
		self._jdl_writer = jdlWriter or JDLWriter()


	def getSites(self):
		return None


	def makeJDL(self, jobnum, module):
		cfgPath = os.path.join(self._jobPath, 'job_%d.var' % jobnum)
		sbIn = lmap(lambda d_s_t: d_s_t[1], self._get_in_transfer_info_list(module))
		sbOut = lmap(lambda d_s_t: d_s_t[2], self._get_out_transfer_info_list(module))
		wcList = lfilter(lambda x: '*' in x, sbOut)
		if len(wcList):
			self._write_job_config(cfgPath, jobnum, module, {'GC_WC': str.join(' ', wcList)})
			sandboxOutJDL = lfilter(lambda x: x not in wcList, sbOut) + ['GC_WC.tar.gz']
		else:
			self._write_job_config(cfgPath, jobnum, module, {})
			sandboxOutJDL = sbOut
		# Warn about too large sandboxes
		sbSizes = lmap(os.path.getsize, sbIn)
		if sbSizes and (self._warnSBSize > 0) and (sum(sbSizes) > self._warnSBSize * 1024 * 1024):
			if not utils.get_user_bool('Sandbox is very large (%d bytes) and can cause issues with the WMS! Do you want to continue?' % sum(sbSizes), False):
				sys.exit(os.EX_OK)
			self._warnSBSize = 0

		reqs = self.brokerSite.brokerAdd(module.get_requirement_list(jobnum), WMS.SITES)
		formatStrList = lambda strList: '{ %s }' % str.join(', ', imap(lambda x: '"%s"' % x, strList))
		contents = {
			'Executable': '"gc-run.sh"',
			'Arguments': '"%d"' % jobnum,
			'StdOutput': '"gc.stdout"',
			'StdError': '"gc.stderr"',
			'InputSandbox': formatStrList(sbIn + [cfgPath]),
			'OutputSandbox': formatStrList(sandboxOutJDL),
			'VirtualOrganisation': '"%s"' % self.vo,
			'Rank': '-other.GlueCEStateEstimatedResponseTime',
			'RetryCount': 2
		}
		return self._jdl_writer.format(reqs, contents)


	def writeWMSIds(self, ids):
		try:
			fd, jobs = tempfile.mkstemp('.jobids')
			utils.safe_write(os.fdopen(fd, 'w'), str.join('\n', self._iter_wms_ids(ids)))
		except Exception:
			raise BackendError('Could not write wms ids to %s.' % jobs)
		return jobs


	def explainError(self, proc, code):
		if 'Keyboard interrupt raised by user' in proc.stderr.read_log():
			return True
		return False


	# Submit job and yield (jobnum, WMS ID, other data)
	def _submit_job(self, jobnum, module):
		fd, jdl = tempfile.mkstemp('.jdl')
		try:
			jdlData = self.makeJDL(jobnum, module)
			utils.safe_write(os.fdopen(fd, 'w'), jdlData)
		except Exception:
			utils.remove_files([jdl])
			raise BackendError('Could not write jdl data to %s.' % jdl)

		try:
			submitArgs = []
			for key_value in utils.filter_dict(self._submitParams, value_filter = identity).items():
				submitArgs.extend(key_value)
			submitArgs.append(jdl)

			activity = Activity('submitting job %d' % jobnum)
			proc = LocalProcess(self._submitExec, '--nomsg', '--noint', '--logfile', '/dev/stderr', *submitArgs)

			gc_id = None
			for line in ifilter(lambda x: x.startswith('http'), imap(str.strip, proc.stdout.iter(timeout = 60))):
				gc_id = line
			exit_code = proc.status(timeout = 0, terminate = True)

			activity.finish()

			if (exit_code != 0) or (gc_id is None):
				if self.explainError(proc, exit_code):
					pass
				else:
					self._log.log_process(proc, files = {'jdl': SafeFile(jdl).read()})
		finally:
			utils.remove_files([jdl])
		return (jobnum, utils.QM(gc_id, self._create_gc_id(gc_id), None), {'jdl': str.join('', jdlData)})


	# Get output of jobs and yield output dirs
	def _get_jobs_output(self, ids):
		if len(ids) == 0:
			raise StopIteration

		basePath = os.path.join(self._path_output, 'tmp')
		try:
			if len(ids) == 1:
				# For single jobs create single subdir
				tmpPath = os.path.join(basePath, md5(ids[0][0]).hexdigest())
			else:
				tmpPath = basePath
			utils.ensure_dir_exists(tmpPath)
		except Exception:
			raise BackendError('Temporary path "%s" could not be created.' % tmpPath, BackendError)

		jobnumMap = dict(ids)
		jobs = self.writeWMSIds(ids)

		activity = Activity('retrieving %d job outputs' % len(ids))
		proc = LocalProcess(self._outputExec, '--noint', '--logfile', '/dev/stderr', '-i', jobs, '--dir', tmpPath)

		# yield output dirs
		todo = jobnumMap.values()
		currentJobNum = None
		for line in imap(str.strip, proc.stdout.iter(timeout = 60)):
			if line.startswith(tmpPath):
				todo.remove(currentJobNum)
				outputDir = line.strip()
				if os.path.exists(outputDir):
					if 'GC_WC.tar.gz' in os.listdir(outputDir):
						wildcardTar = os.path.join(outputDir, 'GC_WC.tar.gz')
						try:
							tarfile.TarFile.open(wildcardTar, 'r:gz').extractall(outputDir)
							os.unlink(wildcardTar)
						except Exception:
							self._log.error('Can\'t unpack output files contained in %s', wildcardTar)
				yield (currentJobNum, line.strip())
				currentJobNum = None
			else:
				currentJobNum = jobnumMap.get(self._create_gc_id(line), currentJobNum)
		exit_code = proc.status(timeout = 0, terminate = True)
		activity.finish()

		if exit_code != 0:
			if 'Keyboard interrupt raised by user' in proc.stderr.read(timeout = 0):
				utils.remove_files([jobs, basePath])
				raise StopIteration
			else:
				self._log.log_process(proc, files = {'jobs': SafeFile(jobs).read()})
			self._log.error('Trying to recover from error ...')
			for dirName in os.listdir(basePath):
				yield (None, os.path.join(basePath, dirName))

		# return unretrievable jobs
		for jobnum in todo:
			yield (jobnum, None)

		utils.remove_files([jobs, basePath])
