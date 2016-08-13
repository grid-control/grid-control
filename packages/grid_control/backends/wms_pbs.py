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

from grid_control import utils
from grid_control.backends.aspect_cancel import CancelJobsWithProcessBlind
from grid_control.backends.aspect_status import CheckInfo, CheckJobsMissingState, CheckJobsWithProcess
from grid_control.backends.backend_tools import BackendDiscovery, ProcessCreatorAppendArguments
from grid_control.backends.wms import BackendError, WMS
from grid_control.backends.wms_pbsge import PBSGECommon
from grid_control.job_db import Job
from grid_control.utils.parsing import parseTime
from grid_control.utils.process_base import LocalProcess
from python_compat import identity, ifilter, izip, lmap

class PBS_CheckJobs(CheckJobsWithProcess):
	def __init__(self, config, fqid_fun = identity):
		proc_factory = ProcessCreatorAppendArguments(config, 'qstat', ['-f'], lambda wmsIDs: lmap(fqid_fun, wmsIDs))
		CheckJobsWithProcess.__init__(self, config, proc_factory, status_map = {
			'H': Job.SUBMITTED, 'S': Job.SUBMITTED,
			'W': Job.WAITING,   'Q': Job.QUEUED,
			'R': Job.RUNNING,   'C': Job.DONE,
			'E': Job.DONE,      'T': Job.DONE,
			'fail': Job.DONE,   'success': Job.DONE,
		})

	def _parse(self, proc):
		for section in utils.accumulate(proc.stdout.iter(self._timeout), '', lambda x, buf: x == '\n'):
			try:
				lines = section.replace('\n\t', '').split('\n')
				job_info = utils.DictFormat(' = ').parse(lines[1:])
				job_info[CheckInfo.WMSID] = lines[0].split(':')[1].split('.')[0].strip()
				job_info[CheckInfo.RAW_STATUS] = job_info.pop('job_state')
				job_info[CheckInfo.QUEUE] = job_info.pop('queue', None)
				if 'exec_host' in job_info:
					job_info[CheckInfo.WN] = job_info.pop('exec_host').split('/')[0] + '.' + job_info.get('server', '')
			except Exception:
				raise BackendError('Error reading job info:\n%s' % section)
			yield job_info


class PBS_Discover_Nodes(BackendDiscovery):
	def __init__(self, config):
		BackendDiscovery.__init__(self, config)
		self._exec = utils.resolveInstallPath('pbsnodes')

	def discover(self):
		proc = LocalProcess(self._exec)
		for line in proc.stdout.iter(timeout = 10):
			if not line.startswith(' ') and len(line) > 1:
				node = line.strip()
			if ('state = ' in line) and ('down' not in line) and ('offline' not in line):
				yield {'name': node}
		proc.status_raise(timeout = 0)


class PBS_Discover_Queues(BackendDiscovery):
	def __init__(self, config):
		BackendDiscovery.__init__(self, config)
		self._exec = utils.resolveInstallPath('qstat')

	def discover(self):
		active = False
		keys = [WMS.MEMORY, WMS.CPUTIME, WMS.WALLTIME]
		parser = dict(izip(keys, [int, parseTime, parseTime]))
		proc = LocalProcess(self._exec, '-q')
		for line in proc.stdout.iter(timeout = 10):
			if line.startswith('-'):
				active = True
			elif line.startswith(' '):
				active = False
			elif active:
				fields = lmap(str.strip, line.split()[:4])
				queueInfo = {'name': fields[0]}
				for key, value in ifilter(lambda k_v: not k_v[1].startswith('-'), izip(keys, fields[1:])):
					queueInfo[key] = parser[key](value)
				yield queueInfo
		proc.status_raise(timeout = 0)


class PBS(PBSGECommon):
	configSections = PBSGECommon.configSections + ['PBS']

	def __init__(self, config, name):
		cancelExecutor = CancelJobsWithProcessBlind(config, 'qdel',
			fmt = lambda wmsIDs: lmap(self._fqid, wmsIDs), unknownID = 'Unknown Job Id')
		PBSGECommon.__init__(self, config, name,
			cancelExecutor = cancelExecutor,
			checkExecutor = CheckJobsMissingState(config, PBS_CheckJobs(config, self._fqid)))
		self._nodes_finder = PBS_Discover_Nodes(config)
		self._queues_finder = PBS_Discover_Queues(config)
		self._server = config.get('server', '', onChange = None)


	def _fqid(self, wmsID):
		if not self._server:
			return wmsID
		return '%s.%s' % (wmsID, self._server)


	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr):
		reqMap = { WMS.MEMORY: ('pvmem', lambda m: '%dmb' % m) }
		params = PBSGECommon.getCommonSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr, reqMap)
		# Job requirements
		if reqs.get(WMS.QUEUES):
			params += ' -q %s' % reqs[WMS.QUEUES][0]
		if reqs.get(WMS.SITES):
			params += ' -l host=%s' % str.join('+', reqs[WMS.SITES])
		return params


	def parseSubmitOutput(self, data):
		# 1667161.ekpplusctl.ekpplus.cluster
		return data.split('.')[0].strip()
