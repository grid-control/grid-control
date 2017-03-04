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

from grid_control.backends.access_grid import GridAccessToken
from python_compat import imap, izip


class ARCAccessToken(GridAccessToken):
	alias_list = ['arc', 'arcproxy']

	def __init__(self, config, name):
		GridAccessToken.__init__(self, config, name, 'arcproxy')

	def get_auth_fn_list(self):
		return [self._get_proxy_info('proxy path')]

	def _get_proxy_info_arguments(self):
		if self._proxy_fn:
			return ['-I', '--proxy', self._proxy_fn]
		return ['-I']

	def _get_timeleft(self, cached):
		return min(
			self._get_proxy_info('time left for proxy', self._parse_time, cached),
			self._get_proxy_info('time left for ac', self._parse_time, cached))

	def _parse_time(self, time_str):
		result = 0
		entry_map = {'yea': 365 * 24 * 60 * 60, 'day': 24 * 60 * 60, 'hou': 60 * 60, 'min': 60, 'sec': 1}
		tmp = time_str.split()
		for (entry, value) in izip(imap(lambda x: x[:3], tmp[1::2]), imap(int, tmp[::2])):
			result += entry_map[entry] * value
		return result
