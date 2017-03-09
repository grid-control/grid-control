# | Copyright 2007-2017 Karlsruhe Institute of Technology
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
from grid_control.utils.parsing import parse_time


class VomsAccessToken(GridAccessToken):
	alias_list = ['voms', 'VomsProxy']

	def __init__(self, config, name):
		GridAccessToken.__init__(self, config, name, 'voms-proxy-info')

	def get_auth_fn_list(self):
		return [self._get_proxy_info('path')]

	def _get_proxy_info_arguments(self):
		if self._proxy_fn:
			return ['--all', '--file', self._proxy_fn]
		return ['--all']

	def _get_timeleft(self, cached):
		return self._get_proxy_info('timeleft', parse_time, cached)
