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
from grid_control.backends.wms import BackendError, WMS
from grid_control.backends.wms_pbsge import PBSGECommon
from grid_control.job_db import Job
from grid_control.utils.parsing import parseTime
from grid_control.utils.process_base import LocalProcess
from python_compat import ifilter, imap, izip, lmap

class PBS(PBSGECommon):
	configSections = PBSGECommon.configSections + ['PBS']
	_statusMap = {
		'H': Job.SUBMITTED, 'S': Job.SUBMITTED,
		'W': Job.WAITING,   'Q': Job.QUEUED,
		'R': Job.RUNNING,   'C': Job.DONE,
		'E': Job.DONE,      'T': Job.DONE,
		'fail':	Job.FAILED, 'success': Job.SUCCESS
	}

	def __init__(self, config, name):
		PBSGECommon.__init__(self, config, name)
		self._nodesExec = utils.resolveInstallPath('pbsnodes')
		self._server = config.get('server', '', onChange = None)
		self._fqid = lambda wmsId: utils.QM(self._server, '%s.%s' % (wmsId, self._server), wmsId)


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


	def parseStatus(self, status):
		for section in utils.accumulate(status, '', lambda x, buf: x == '\n'):
			try:
				lines = section.replace('\n\t', '').split('\n')
				jobinfo = utils.DictFormat(' = ').parse(lines[1:])
				jobinfo['id'] = lines[0].split(':')[1].split('.')[0].strip()
				jobinfo['status'] = jobinfo.get('job_state')
				jobinfo['dest'] = 'N/A'
				if 'exec_host' in jobinfo:
					jobinfo['dest'] = '%s/%s' % (
						jobinfo.get('exec_host').split('/')[0] + '.' + jobinfo.get('server', ''),
						jobinfo.get('queue')
					)
			except Exception:
				raise BackendError('Error reading job info:\n%s' % section)
			yield jobinfo


	def getCheckArguments(self, wmsIds):
		return '-f %s' % str.join(' ', imap(self._fqid, wmsIds))


	def getCancelArguments(self, wmsIds):
		return str.join(' ', imap(self._fqid, wmsIds))


	def getQueues(self):
		(queues, active) = ({}, False)
		keys = [WMS.MEMORY, WMS.CPUTIME, WMS.WALLTIME]
		parser = dict(izip(keys, [int, parseTime, parseTime]))
		proc = LocalProcess(self.statusExec, '-q')
		for line in proc.stdout.iter(timeout = 10):
			if line.startswith('-'):
				active = True
			elif line.startswith(' '):
				active = False
			elif active:
				fields = lmap(str.strip, line.split()[:4])
				queueInfo = {}
				for key, value in ifilter(lambda k_v: not k_v[1].startswith('-'), izip(keys, fields[1:])):
					queueInfo[key] = parser[key](value)
				queues[fields[0]] = queueInfo
		proc.status_raise(timeout = 0)
		return queues


	def getNodes(self):
		result = []
		proc = LocalProcess(self._nodesExec)
		for line in proc.stdout.iter():
			if not line.startswith(' ') and len(line) > 1:
				node = line.strip()
			if ('state = ' in line) and ('down' not in line) and ('offline' not in line):
				result.append(node)
		proc.status_raise(timeout = 0)
		if len(result) > 0:
			return result
