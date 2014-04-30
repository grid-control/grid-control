#-#  Copyright 2010-2014 Karlsruhe Institute of Technology
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

from python_compat import next
from grid_control import ConfigError, RethrowError, Job, utils, backends
from grid_control.backends import LocalWMS

class Host(LocalWMS):
	def __init__(self, config, wmsName = None):
		LocalWMS.__init__(self, config, wmsName,
			submitExec = utils.pathShare('gc-host.sh'),
			statusExec = utils.resolveInstallPath('ps'),
			cancelExec = utils.resolveInstallPath('kill'))


	def unknownID(self):
		return 'Unknown Job Id'


	def getJobArguments(self, jobNum, sandbox):
		return ''


	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr):
		return '%d "%s" "%s" "%s"' % (jobNum, sandbox, stdout, stderr)


	def parseSubmitOutput(self, data):
		return data.strip()


	def parseStatus(self, status):
		head = map(lambda x: x.strip('%').lower(), next(status, '').split())
		for entry in map(str.strip, status):
			jobinfo = dict(zip(head, filter(lambda x: x != '', entry.split(None, len(head) - 1))))
			jobinfo.update({'id': jobinfo.get('pid'), 'status': 'R', 'dest': 'localhost/localqueue'})
			yield jobinfo


	def parseJobState(self, state):
		return Job.RUNNING


	def getCheckArguments(self, wmsIds):
		return 'wwup %s' % str.join(' ', wmsIds)


	def getCancelArguments(self, wmsIds):
		return '-9 %s' % str.join(' ', wmsIds)


class Localhost(Host):
	pass
