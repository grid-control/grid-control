#-#  Copyright 2007-2015 Karlsruhe Institute of Technology
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

# Generic base class for authentication proxies GCSCF:

import os, time
from grid_control import utils
from grid_control.abstract import NamedObject
from grid_control.exceptions import AbstractError, UserError

class AccessToken(NamedObject):
	configSections = NamedObject.configSections + ['proxy', 'access']
	tagName = 'access'

	def getUsername(self):
		raise AbstractError

	def getFQUsername(self):
		return self.getUsername()

	def getGroup(self):
		raise AbstractError

	def getAuthFiles(self):
		raise AbstractError

	def canSubmit(self, neededTime, canCurrentlySubmit):
		raise AbstractError


class MultiAccessToken(AccessToken):
	def __init__(self, config, name, subtokenBuilder):
		AccessToken.__init__(self, config, name)
		self._subtokenList = map(lambda tbuilder: tbuilder(), subtokenBuilder)

	def getUsername(self):
		return self._subtokenList[0].getUsername()

	def getFQUsername(self):
		return self._subtokenList[0].getFQUsername()

	def getGroup(self):
		return self._subtokenList[0].getGroup()

	def getAuthFiles(self):
		return self._subtokenList[0].getAuthFiles()

	def canSubmit(self, neededTime, canCurrentlySubmit):
		for subtoken in self._subtokenList:
			canCurrentlySubmit = canCurrentlySubmit and subtoken.canSubmit(neededTime, canCurrentlySubmit)
		return canCurrentlySubmit


class TrivialAccessToken(AccessToken):
	def getUsername(self):
		for var in ('LOGNAME', 'USER', 'LNAME', 'USERNAME'):
			result = os.environ.get(var)
			if result:
				return result
		raise RuntimeError('Unable to determine username!')

	def getGroup(self):
		return os.environ.get('GROUP', 'None')

	def getAuthFiles(self):
		return []

	def canSubmit(self, neededTime, canCurrentlySubmit):
		return True


class TimedAccessToken(AccessToken):
	def __init__(self, config, name):
		AccessToken.__init__(self, config, name)
		self._lowerLimit = config.getTime('min lifetime', 300, onChange = None)
		self._maxQueryTime = config.getTime('max query time',  5 * 60, onChange = None)
		self._minQueryTime = config.getTime('min query time', 30 * 60, onChange = None)
		self._lastUpdate = 0

	def canSubmit(self, neededTime, canCurrentlySubmit):
		if not self._checkTimeleft(self._lowerLimit):
			raise UserError('Your access token only has %d seconds left! (Required are %s)' %
				(self._getTimeleft(cached = True), utils.strTime(self._lowerLimit)))
		if not self._checkTimeleft(self._lowerLimit + neededTime) and canCurrentlySubmit:
			utils.vprint('Access token lifetime (%s) does not meet the access and walltime (%s) requirements!' %
				(utils.strTime(self._getTimeleft(cached = False)), utils.strTime(self._lowerLimit + neededTime)), -1, printTime = True)
			utils.vprint('Disabling job submission', -1, printTime = True)
			return False
		return True

	def _getTimeleft(self, cached):
		raise AbstractError

	def _checkTimeleft(self, neededTime): # check for time left
		delta = time.time() - self._lastUpdate
		timeleft = max(0, self._getTimeleft(cached = True) - delta)
		# recheck token => after > 30min have passed or when time is running out (max every 5 minutes)
		if (delta > self._minQueryTime) or (timeleft < neededTime and delta > self._maxQueryTime):
			self._lastUpdate = time.time()
			timeleft = self._getTimeleft(cached = False)
			verbosity = utils.QM(timeleft < neededTime, -1, 0)
			utils.vprint('The access token now has %s left' % utils.strTime(timeleft), verbosity, printTime = True)
		return timeleft >= neededTime


class VomsProxy(TimedAccessToken):
	def __init__(self, config, name):
		TimedAccessToken.__init__(self, config, name)
		self._infoExec = utils.resolveInstallPath('voms-proxy-info')
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
		return self._getProxyInfo('timeleft', utils.parseTime, cached)

	def _parseProxy(self, cached = True):
		# Return cached results if requested
		if cached and self._cache:
			return self._cache
		# Call voms-proxy-info and parse results
		proc = utils.LoggedProcess(self._infoExec, '--all')
		retCode = proc.wait()
		if (retCode != 0) and not self._ignoreWarning:
			msg = ('voms-proxy-info output:\n%s\n%s\n' % (proc.getOutput(), proc.getError())).replace('\n\n', '\n')
			msg += 'If job submission is still possible, you can set [access] ignore warnings = True\n'
			raise RuntimeError(msg + 'voms-proxy-info failed with return code %d' % retCode)
		self._cache = utils.DictFormat(':').parse(proc.getOutput())
		return self._cache

	def _getProxyInfo(self, key, parse = lambda x: x, cached = True):
		info = self._parseProxy(cached)
		try:
			return parse(info[key])
		except Exception:
			raise RuntimeError("Can't access %s in proxy information:\n%s" % (key, info))


class RefreshableAccessToken(TimedAccessToken):
	def __init__(self, config, name):
		TimedAccessToken.__init__(self, config, name)
		self._refresh = config.getTime('access refresh', 60*60, onChange = None)

	def _refreshAccessToken(self):
		raise AbstractError

	def _checkTimeleft(self, neededTime): # check for time left
		if self._getTimeleft(True) < self._refresh:
			self._refreshAccessToken()
			self._getTimeleft(False)
		return TimedAccessToken._checkTimeleft(self, neededTime)


class AFSAccessToken(RefreshableAccessToken):
	def __init__(self, config, name):
		RefreshableAccessToken.__init__(self, config, name)
		self._kinitExec = utils.resolveInstallPath('kinit')
		self._klistExec = utils.resolveInstallPath('klist')
		self._cache = None
		self._backupTickets(config)
		self._tickets = config.getList('tickets', [], onChange = None)

	def _backupTickets(self, config):
		import os, stat, shutil
		for name in ['KRB5CCNAME', 'KRBTKFILE']: # store kerberos files in work directory for persistency
			if name in os.environ:
				oldFN = os.environ[name].replace('FILE:', '')
				newFN = config.getWorkPath('proxy.%s' % name)
				shutil.copyfile(oldFN, newFN)
				os.chmod(newFN, stat.S_IRUSR | stat.S_IWUSR)
				os.environ[name] = newFN

	def _refreshAccessToken(self):
		return utils.LoggedProcess(self._kinitExec, '-R').wait()

	def _parseTickets(self, cached = True):
		# Return cached results if requested
		if cached and self._cache:
			return self._cache
		# Call klist and parse results
		proc = utils.LoggedProcess(self._klistExec)
		retCode = proc.wait()
		self._cache = {}
		for line in proc.getOutput().splitlines():
			if line.count('@') and (line.count(':') > 1):
				(issued, expires, principal) = line.split('  ')
				parseDate = lambda value, format: time.mktime(time.strptime(value, format))
				if (expires.count('/') == 2) and (expires.count(':') == 2):
					expires = parseDate(expires, '%m/%d/%y %H:%M:%S')
				else:
					currentYear = int(time.strftime('%Y'))
					expires = parseDate('%s %d' % (expires, currentYear), '%b %d %H:%M:%S %Y')
					issued = parseDate('%s %d' % (issued, currentYear), '%b %d %H:%M:%S %Y')
					if expires < issued: # wraparound at new year
						expires = parseDate('%s %d' % (expires, currentYear + 1), '%b %d %H:%M:%S %Y')
				self._cache[principal] = expires
		return self._cache

	def _getTimeleft(self, cached):
		info = self._parseTickets(cached)
		time_current = time.time()
		time_end = time_current
		for ticket in info:
			if (self._tickets and (ticket not in self._tickets)) or not ticket:
				continue
			time_end = max(info[ticket], time_end)
		return time_end - time_current


class TrivialProxy(TrivialAccessToken):
	pass
