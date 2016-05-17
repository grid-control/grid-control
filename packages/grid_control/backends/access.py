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

import os, time, logging
from grid_control.gc_exceptions import UserError
from grid_control.gc_plugin import NamedPlugin
from grid_control.utils.parsing import strTime
from hpfwk import AbstractError, NestedException

class AccessTokenError(NestedException):
	pass

class AccessToken(NamedPlugin):
	configSections = NamedPlugin.configSections + ['proxy', 'access']
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
	def __init__(self, config, name, tokenList):
		AccessToken.__init__(self, config, name)
		self._subtokenList = tokenList

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
	alias = ['trivial', 'TrivialProxy']

	def getUsername(self):
		for var in ('LOGNAME', 'USER', 'LNAME', 'USERNAME'):
			result = os.environ.get(var)
			if result:
				return result
		raise AccessTokenError('Unable to determine username!')

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
		self._ignoreTime = config.getBool('ignore walltime', False, onChange = None)
		self._lastUpdate = 0
		self._logUser = logging.getLogger('user.time')

	def canSubmit(self, neededTime, canCurrentlySubmit):
		if not self._checkTimeleft(self._lowerLimit):
			raise UserError('Your access token (%s) only has %d seconds left! (Required are %s)' %
				(self.getObjectName(), self._getTimeleft(cached = True), strTime(self._lowerLimit)))
		if self._ignoreTime:
			return True
		if not self._checkTimeleft(self._lowerLimit + neededTime) and canCurrentlySubmit:
			self._logUser.warning('Access token (%s) lifetime (%s) does not meet the access and walltime (%s) requirements!',
				self.getObjectName(), strTime(self._getTimeleft(cached = False)), strTime(self._lowerLimit + neededTime))
			self._logUser.warning('Disabling job submission')
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
			self._logUser.info('Time left for access token "%s": %s', self.getObjectName(), strTime(timeleft))
		return timeleft >= neededTime


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
