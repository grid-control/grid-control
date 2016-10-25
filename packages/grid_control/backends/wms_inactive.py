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
	alias_list = ['inactive']

	def __init__(self, config, name):
		WMS.__init__(self, config, name)
		self._token = config.get_composited_plugin(['proxy', 'access token'], 'TrivialAccessToken',
			'MultiAccessToken', cls = 'AccessToken', bkwargs={'inherit': True, 'tags': [self]})

	def can_submit(self, needed_time, can_currently_submit):
		return True

	def get_access_token(self, gc_id):
		return self._token

	def deploy_task(self, task, monitor, transfer_se, transfer_sb):
		return

	def submit_jobs(self, jobnumList, task): # jobnumList = [1, 2, ...]
		self._log.warning('Discarded submission of %d jobs', len(jobnumList))

	def check_jobs(self, gc_id_list): # Check status and return (gc_id, job_state, job_info) for active jobs
		self._log.warning('Discarded check of %d jobs', len(gc_id_list))

	def retrieve_jobs(self, gc_id_list):
		self._log.warning('Discarded retrieval of %d jobs', len(gc_id_list))

	def cancel_jobs(self, gc_id_list):
		self._log.warning('Discarded abort of %d jobs', len(gc_id_list))
