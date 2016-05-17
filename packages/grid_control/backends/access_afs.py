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

import os, time
from grid_control.backends.access import AccessTokenError, RefreshableAccessToken
from grid_control.utils import resolveInstallPath
from grid_control.utils.process_base import LocalProcess
from python_compat import imap, lmap, rsplit

class AFSAccessToken(RefreshableAccessToken):
	alias = ['afs', 'AFSProxy']

	def __init__(self, config, name):
		RefreshableAccessToken.__init__(self, config, name)
		self._kinitExec = resolveInstallPath('kinit')
		self._klistExec = resolveInstallPath('klist')
		self._cache = None
		self._authFiles = dict(imap(lambda name: (name, config.getWorkPath('proxy.%s' % name)), ['KRB5CCNAME', 'KRBTKFILE']))
		self._backupTickets(config)
		self._tickets = config.getList('tickets', [], onChange = None)

	def _backupTickets(self, config):
		import stat, shutil
		for name in self._authFiles: # store kerberos files in work directory for persistency
			if name in os.environ:
				fn = os.environ[name].replace('FILE:', '')
				if fn != self._authFiles[name]:
					shutil.copyfile(fn, self._authFiles[name])
				os.chmod(self._authFiles[name], stat.S_IRUSR | stat.S_IWUSR)
				os.environ[name] = self._authFiles[name]

	def _refreshAccessToken(self):
		return LocalProcess(self._kinitExec, '-R').finish(timeout = 10)

	def _parseTickets(self, cached = True):
		# Return cached results if requested
		if cached and self._cache:
			return self._cache
		# Call klist and parse results
		proc = LocalProcess(self._klistExec)
		self._cache = {}
		try:
			for line in proc.stdout.iter(timeout = 10):
				if line.count('@') and (line.count(':') > 1):
					issued_expires, principal = rsplit(line, '  ', 1)
					issued_expires = issued_expires.replace('/', ' ').split()
					assert(len(issued_expires) % 2 == 0)
					issued_str = str.join(' ', issued_expires[:int(len(issued_expires) / 2)])
					expires_str = str.join(' ', issued_expires[int(len(issued_expires) / 2):])
					parseDate = lambda value, format: time.mktime(time.strptime(value, format))
					if expires_str.count(' ') == 3:
						if len(expires_str.split()[2]) == 2:
							expires = parseDate(expires_str, '%m %d %y %H:%M:%S')
						else:
							expires = parseDate(expires_str, '%m %d %Y %H:%M:%S')
					elif expires_str.count(' ') == 2: # year information is missing
						currentYear = int(time.strftime('%Y'))
						expires = parseDate(expires_str + ' %d' % currentYear, '%b %d %H:%M:%S %Y')
						issued = parseDate(issued_str + ' %d' % currentYear, '%b %d %H:%M:%S %Y')
						if expires < issued: # wraparound at new year
							expires = parseDate(expires_str + ' %d' % (currentYear + 1), '%b %d %H:%M:%S %Y')
					self._cache.setdefault('tickets', {})[principal] = expires
				elif line.count(':') == 1:
					key, value = lmap(str.strip, line.split(':', 1))
					self._cache[key.lower()] = value
		except Exception:
			raise AccessTokenError('Unable to parse kerberos ticket information!')
		proc.status_raise(timeout = 0)
		return self._cache

	def _getTimeleft(self, cached):
		info = self._parseTickets(cached)['tickets']
		time_current = time.time()
		time_end = time_current
		for ticket in info:
			if (self._tickets and (ticket not in self._tickets)) or not ticket:
				continue
			time_end = max(info[ticket], time_end)
		return time_end - time_current

	def _getPrincipal(self):
		info = self._parseTickets()
		return info.get('default principal', info.get('principal'))

	def getUsername(self):
		return self._getPrincipal().split('@')[0]

	def getFQUsername(self):
		return self._getPrincipal()

	def getGroup(self):
		return self._getPrincipal().split('@')[1]

	def getAuthFiles(self):
		return self._authFiles.values()
