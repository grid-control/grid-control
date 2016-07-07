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

from grid_control.backends import WMS

class InactiveWMS(WMS):
	alias = ['inactive']

	def __init__(self, config, wmsName):
		WMS.__init__(self, config, wmsName)
		self._token = config.getCompositePlugin(['proxy', 'access token'], 'TrivialAccessToken',
			'MultiAccessToken', cls = 'AccessToken', inherit = True, tags = [self])

	def canSubmit(self, neededTime, canCurrentlySubmit):
		return True

	def getAccessToken(self, gcID):
		return self._token

	def deployTask(self, task, monitor):
		return

	def submitJobs(self, jobNumList, task): # jobNumList = [1, 2, ...]
		self._log.warning('Discarded submission of %d jobs', len(jobNumList))

	def checkJobs(self, gcIDs): # Check status and return (gcID, job_state, job_info) for active jobs
		self._log.warning('Discarded check of %d jobs', len(gcIDs))

	def retrieveJobs(self, gcID_jobNum_List):
		self._log.warning('Discarded retrieval of %d jobs', len(gcID_jobNum_List))

	def cancelJobs(self, gcIDs):
		self._log.warning('Discarded abort of %d jobs', len(gcIDs))
