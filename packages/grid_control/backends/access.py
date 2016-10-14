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
from grid_control.utils.parsing import str_time_long
from hpfwk import AbstractError, NestedException


class AccessTokenError(NestedException):
	pass

class AccessToken(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['proxy', 'access']
	config_tag_name = 'access'

	def getUsername(self):
		raise AbstractError

	def getFQUsername(self):
		return self.getUsername()

	def getGroup(self):
		raise AbstractError

	def getAuthFiles(self):
		raise AbstractError

	def can_submit(self, needed_time, can_currently_submit):
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

	def can_submit(self, needed_time, can_currently_submit):
		for subtoken in self._subtokenList:
			can_currently_submit = can_currently_submit and subtoken.can_submit(needed_time, can_currently_submit)
		return can_currently_submit


class TrivialAccessToken(AccessToken):
	alias_list = ['trivial', 'TrivialProxy']

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

	def can_submit(self, needed_time, can_currently_submit):
		return True


class TimedAccessToken(AccessToken):
	def __init__(self, config, name):
		AccessToken.__init__(self, config, name)
		self._lowerLimit = config.get_time('min lifetime', 300, on_change = None)
		self._maxQueryTime = config.get_time(['max query time', 'urgent query time'],  5 * 60, on_change = None)
		self._minQueryTime = config.get_time(['min query time', 'query time'], 30 * 60, on_change = None)
		self._ignoreTime = config.get_bool(['ignore walltime', 'ignore needed time'], False, on_change = None)
		self._lastUpdate = 0

	def can_submit(self, needed_time, can_currently_submit):
		if not self._checkTimeleft(self._lowerLimit):
			raise UserError('Your access token (%s) only has %d seconds left! (Required are %s)' %
				(self.get_object_name(), self._get_timeleft(cached = True), str_time_long(self._lowerLimit)))
		if self._ignoreTime or (needed_time < 0):
			return True
		if not self._checkTimeleft(self._lowerLimit + needed_time) and can_currently_submit:
			self._log.log_time(logging.WARNING, 'Access token (%s) lifetime (%s) does not meet the access and walltime (%s) requirements!',
				self.get_object_name(), str_time_long(self._get_timeleft(cached = False)), str_time_long(self._lowerLimit + needed_time))
			self._log.log_time(logging.WARNING, 'Disabling job submission')
			return False
		return True

	def _get_timeleft(self, cached):
		raise AbstractError

	def _checkTimeleft(self, needed_time): # check for time left
		delta = time.time() - self._lastUpdate
		timeleft = max(0, self._get_timeleft(cached = True) - delta)
		# recheck token => after > 30min have passed or when time is running out (max every 5 minutes)
		if (delta > self._minQueryTime) or (timeleft < needed_time and delta > self._maxQueryTime):
			self._lastUpdate = time.time()
			timeleft = self._get_timeleft(cached = False)
			self._log.log_time(logging.INFO, 'Time left for access token "%s": %s', self.get_object_name(), str_time_long(timeleft))
		return timeleft >= needed_time


class RefreshableAccessToken(TimedAccessToken):
	def __init__(self, config, name):
		TimedAccessToken.__init__(self, config, name)
		self._refresh = config.get_time('access refresh', 60*60, on_change = None)

	def _refreshAccessToken(self):
		raise AbstractError

	def _checkTimeleft(self, needed_time): # check for time left
		if self._get_timeleft(True) < self._refresh:
			self._refreshAccessToken()
			self._get_timeleft(False)
		return TimedAccessToken._checkTimeleft(self, needed_time)
