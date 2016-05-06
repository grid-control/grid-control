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

from grid_control.backends.access import AccessTokenError, TimedAccessToken
from grid_control.utils import DictFormat, resolveInstallPath
from grid_control.utils.parsing import parseTime
from grid_control.utils.process_base import LocalProcess
from python_compat import identity

class VomsAccessToken(TimedAccessToken):
	alias = ['voms', 'VomsProxy']

	def __init__(self, config, name):
		TimedAccessToken.__init__(self, config, name)
		self._infoExec = resolveInstallPath('voms-proxy-info')
		self._proxyPath = config.get('proxy path', '')
		self._ignoreWarning = config.getBool('ignore warnings', False, onChange = None)
		self._cache = None

	def getUsername(self):
		return self._getProxyInfo('identity').split('CN=')[1].strip()

	def getFQUsername(self):
		return self._getProxyInfo('identity')

	def getGroup(self):
		return self._getProxyInfo('vo')

	def getAuthFiles(self):
		return [self._getProxyInfo('path')]

	def _getTimeleft(self, cached):
		return self._getProxyInfo('timeleft', parseTime, cached)

	def _parseProxy(self, cached = True):
		# Return cached results if requested
		if cached and self._cache:
			return self._cache
		# Call voms-proxy-info and parse results
		args = ['--all']
		if self._proxyPath:
			args.extend(['--file', self._proxyPath])
		proc = LocalProcess(self._infoExec, *args)
		(retCode, stdout, stderr) = proc.finish(timeout = 10)
		if (retCode != 0) and not self._ignoreWarning:
			msg = ('voms-proxy-info output:\n%s\n%s\n' % (stdout, stderr)).replace('\n\n', '\n')
			msg += 'If job submission is still possible, you can set [access] ignore warnings = True\n'
			raise AccessTokenError(msg + 'voms-proxy-info failed with return code %d' % retCode)
		self._cache = DictFormat(':').parse(stdout)
		return self._cache

	def _getProxyInfo(self, key, parse = identity, cached = True):
		info = self._parseProxy(cached)
		try:
			return parse(info[key])
		except Exception:
			raise AccessTokenError("Can't access %s in proxy information:\n%s" % (key, info))
