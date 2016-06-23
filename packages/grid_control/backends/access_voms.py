# | Copyright 2007-2016 Karlsruhe Institute of Technology
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

# Generic base class for authentication proxies GCSCF:

from grid_control.backends.access_grid import GridAccessToken
from grid_control.utils.parsing import parseTime

class VomsAccessToken(GridAccessToken):
	alias = ['voms', 'VomsProxy']

	def __init__(self, config, name):
		GridAccessToken.__init__(self, config, name, 'voms-proxy-info')

	def _getProxyArgs(self):
		if self._proxyPath:
			return ['--all', '--file', self._proxyPath]
		return ['--all']

	def getAuthFiles(self):
		return [self._getProxyInfo('path')]

	def _getTimeleft(self, cached):
		return self._getProxyInfo('timeleft', parseTime, cached)
