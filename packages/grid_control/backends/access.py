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

# Generic base class for authentication proxies

import os, time, logging
from grid_control.gc_exceptions import UserError
from grid_control.gc_plugin import NamedPlugin
from grid_control.utils import get_local_username
from grid_control.utils.parsing import str_time_long
from hpfwk import AbstractError, NestedException


class AccessTokenError(NestedException):
	pass


class AccessToken(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['proxy', 'access']
	config_tag_name = 'access'

	def can_submit(self, needed_time, can_currently_submit):
		raise AbstractError

	def get_auth_fn_list(self):
		raise AbstractError

	def get_fq_user_name(self):
		return self.get_user_name()

	def get_group(self):
		raise AbstractError

	def get_user_name(self):
		raise AbstractError


class MultiAccessToken(AccessToken):
	alias_list = ['multi']

	def __init__(self, config, name, token_list):
		AccessToken.__init__(self, config, name)
		self._subtoken_list = token_list

	def can_submit(self, needed_time, can_currently_submit):
		for subtoken in self._subtoken_list:
			subtoken_can_submit = subtoken.can_submit(needed_time, can_currently_submit)
			can_currently_submit = can_currently_submit and subtoken_can_submit
		return can_currently_submit

	def get_auth_fn_list(self):
		return self._subtoken_list[0].get_auth_fn_list()

	def get_fq_user_name(self):
		return self._subtoken_list[0].get_fq_user_name()

	def get_group(self):
		return self._subtoken_list[0].get_group()

	def get_user_name(self):
		return self._subtoken_list[0].get_user_name()


class TimedAccessToken(AccessToken):
	def __init__(self, config, name):
		AccessToken.__init__(self, config, name)
		self._min_life_time = config.get_time('min lifetime', 300, on_change=None)
		self._max_query_time = config.get_time(['max query time', 'urgent query time'],
			5 * 60, on_change=None)
		self._min_query_time = config.get_time(['min query time', 'query time'],
			30 * 60, on_change=None)
		self._ignore_time = config.get_bool(['ignore walltime', 'ignore needed time'],
			False, on_change=None)
		self._last_update = 0

	def can_submit(self, needed_time, can_currently_submit):
		if not self._check_time_left(self._min_life_time):
			raise UserError('Your access token (%s) only has %d seconds left! (Required are %s)' %
				(self.get_object_name(), self._get_timeleft(cached=True), str_time_long(self._min_life_time)))
		if self._ignore_time or (needed_time < 0):
			return True
		if not self._check_time_left(self._min_life_time + needed_time) and can_currently_submit:
			self._log.log_time(logging.WARNING,
				'Access token (%s) lifetime (%s) does not meet the access and walltime (%s) requirements!',
				self.get_object_name(), str_time_long(self._get_timeleft(cached=False)),
				str_time_long(self._min_life_time + needed_time))
			self._log.log_time(logging.WARNING, 'Disabling job submission')
			return False
		return True

	def _check_time_left(self, needed_time):  # check for time left
		delta = time.time() - self._last_update
		timeleft = max(0, self._get_timeleft(cached=True) - delta)
		# recheck token => after > 30min have passed or when time is running out (max every 5 minutes)
		if (delta > self._min_query_time) or (timeleft < needed_time and delta > self._max_query_time):
			self._last_update = time.time()
			timeleft = self._get_timeleft(cached=False)
			self._log.log_time(logging.INFO, 'Time left for access token "%s": %s',
				self.get_object_name(), str_time_long(timeleft))
		return timeleft >= needed_time

	def _get_timeleft(self, cached):
		raise AbstractError


class TrivialAccessToken(AccessToken):
	alias_list = ['trivial', 'TrivialProxy']

	def can_submit(self, needed_time, can_currently_submit):
		return True

	def get_auth_fn_list(self):
		return []

	def get_group(self):
		return os.environ.get('GROUP', 'None')

	def get_user_name(self):
		username = get_local_username()
		if not username:
			raise AccessTokenError('Unable to determine username!')
		return username


class RefreshableAccessToken(TimedAccessToken):
	def __init__(self, config, name):
		TimedAccessToken.__init__(self, config, name)
		self._refresh = config.get_time('access refresh', 60 * 60, on_change=None)

	def _check_time_left(self, needed_time):  # check for time left
		if self._get_timeleft(True) < self._refresh:
			self._refresh_access_token()
			self._get_timeleft(False)
		return TimedAccessToken._check_time_left(self, needed_time)

	def _refresh_access_token(self):
		raise AbstractError
