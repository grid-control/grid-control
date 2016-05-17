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
from grid_control.backends import WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.job_db import Job
from python_compat import sorted

class SLURM(LocalWMS):
	configSections = LocalWMS.configSections + ['SLURM']
	_statusMap = { # dictionary mapping vanilla condor job status to GC job status
		#'0' : Job.WAITING	,# unexpanded (never been run)
		'PENDING' : Job.WAITING	,# idle (waiting for a machine to execute on)
		'RUNNING' : Job.RUNNING	,# running
		'COMPLETED' : Job.DONE	,# running
		'COMPLETING' : Job.DONE	,# running
		'CANCELLED+' : Job.ABORTED	,# removed
		'NODE_FAIL' : Job.ABORTED	,# removed
		'CANCELLED' : Job.ABORTED	,# removed
		#'5' : Job.WAITING	,#DISABLED	,# on hold
		'FAILED' : Job.FAILED	,# submit error
		}

	def __init__(self, config, wmsName = None):
		LocalWMS.__init__(self, config, wmsName,
			submitExec = utils.resolveInstallPath('sbatch'),
			statusExec = utils.resolveInstallPath('sacct'),
			cancelExec = utils.resolveInstallPath('scancel'))


	def unknownID(self):
		return 'not in queue !'


	def getJobArguments(self, jobNum, sandbox):
		return repr(sandbox)


	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr):
		# Job name
		params = ' -J "%s"' % jobName
		# processes and IO paths
		params += ' -o "%s" -e "%s"' % (stdout, stderr)
		if WMS.QUEUES in reqs:
			params += ' -p %s' % reqs[WMS.QUEUES][0]
		return params


	def parseSubmitOutput(self, data):
		# job_submit: Job 121195 has been submitted.
		return int(data.split()[3].strip())

	def parseStatus(self, status):
		for jobline in str.join('', list(status)).split('\n'):
			if 'error' in jobline.lower():
				self._log.warning('got error: %r', jobline)
				yield {None: 'abort'}
			if jobline == '':
				continue

			jobinfo = dict()

			jl = jobline.split()
			try:
				jobid = int(jl[0])
			except Exception:
				continue

			if not jl[2] in self._statusMap.keys():
				self._log.warning('unable to parse status=%r %r %r', jl, jl[2], sorted(list(self._statusMap.keys())))
				continue

			jobinfo['id'] = jobid
			jobinfo['queue'] = jl[1]
			jobinfo['status'] = jl[2]
			yield jobinfo


	def getCheckArguments(self, wmsIds):
		return '-n -o jobid,partition,state,exitcode -j %s' % str.join(',', wmsIds)


	def getCancelArguments(self, wmsIds):
		return str.join(' ', wmsIds)
