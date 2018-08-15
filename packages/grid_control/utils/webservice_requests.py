# | Copyright 2016-2017 Karlsruhe Institute of Technology
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

import requests
from grid_control.utils.webservice import RestError, RestSession


class RequestsSession(RestSession):
	alias_list = ['requests']
	_session = None

	def __init__(self):
		if not RequestsSession._session:
			# disable ssl ca verification errors
			requests.packages.urllib3.disable_warnings()
			RequestsSession._session = requests.Session()
		RestSession.__init__(self)

	def request(self, mode, url, headers, params=None, data=None, cert=None):
		request_fun = {RestSession.GET: self._session.get, RestSession.PUT: self._session.put,
			RestSession.POST: self._session.post, RestSession.DELETE: self._session.delete}[mode]
		resp = request_fun(url=url, verify=False, cert=cert, headers=headers, params=params, data=data)
		try:
			resp.raise_for_status()
		except Exception:
			raise RestError('Request result: %s' % resp.text)
		return resp.text
