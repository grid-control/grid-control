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

import os, time, logging
from grid_control.backends.access import AccessTokenError, RefreshableAccessToken
from grid_control.utils import resolve_install_path
from grid_control.utils.parsing import str_time_long
from grid_control.utils.process_base import LocalProcess
from grid_control.utils.thread_tools import GCLock, with_lock
from python_compat import imap, lmap, rsplit


class AFSAccessToken(RefreshableAccessToken):
	alias_list = ['afs', 'AFSProxy', 'KerberosAccessToken']
	env_lock = GCLock()

	def __init__(self, config, name):
		RefreshableAccessToken.__init__(self, config, name)
		self._kinit_exec = resolve_install_path('kinit')
		self._klist_exec = resolve_install_path('klist')
		self._aklog_exec = resolve_install_path('aklog')
		self._cache = None
		self._map_auth_name2fn = dict(imap(lambda name: (name, config.get_work_path('proxy.%s' % name)),
			['KRB5CCNAME', 'KRBTKFILE']))
		self._auth_fn_list = []
		with_lock(AFSAccessToken.env_lock, self._backup_tickets, config)
		self._tickets = config.get_list('tickets', [], on_change=None)

	def get_auth_fn_list(self):
		return self._auth_fn_list

	def get_fq_user_name(self):
		return self._get_principal()

	def get_group(self):
		return self._get_principal().split('@')[1]

	def get_user_name(self):
		return self._get_principal().split('@')[0]

	def _backup_tickets(self, config):
		import stat, shutil
		for name in self._map_auth_name2fn:  # store kerberos files in work directory for persistency
			if name in os.environ:
				fn = os.environ[name].replace('FILE:', '')
				if fn != self._map_auth_name2fn[name]:
					shutil.copyfile(fn, self._map_auth_name2fn[name])
				os.chmod(self._map_auth_name2fn[name], stat.S_IRUSR | stat.S_IWUSR)
				os.environ[name] = self._map_auth_name2fn[name]
				self._auth_fn_list.append(os.environ[name])

	def _get_principal(self):
		info = self._parse_tickets()
		return info.get('default principal', info.get('principal'))

	def _get_timeleft(self, cached):
		info = self._parse_tickets(cached)['tickets']
		time_current = time.time()
		time_end = None
		for ticket in info:
			if (self._tickets and (ticket not in self._tickets)) or not ticket:
				continue
			if time_end is None:
				time_end = info[ticket]
			time_end = min(info[ticket], time_end)
		time_end = time_end or 0
		return time_end - time_current

	def _parse_tickets(self, cached=True):
		# Return cached results if requested
		if cached and self._cache:
			return self._cache
		# Call klist and parse results
		proc = LocalProcess(self._klist_exec)
		self._cache = {}
		try:
			for line in proc.stdout.iter(timeout=10):
				if line.count('@') and (line.count(':') > 1):
					issued_expires, principal = rsplit(line, '  ', 1)
					issued_expires = issued_expires.replace('/', ' ').split()
					assert len(issued_expires) % 2 == 0
					issued_str = str.join(' ', issued_expires[:int(len(issued_expires) / 2)])
					expires_str = str.join(' ', issued_expires[int(len(issued_expires) / 2):])

					if expires_str.count(' ') == 3:
						if len(expires_str.split()[2]) == 2:
							expires = _parse_date(expires_str, '%m %d %y %H:%M:%S')
						elif len(expires_str.split()[2]) == 4:
							expires = _parse_date(expires_str, '%m %d %Y %H:%M:%S')  # here
						else:
							# On NAF, we get an entirely different format now: Sep 2 12:31:34 2021
							expires = _parse_date(expires_str, '%b %d %H:%M:%S %Y')
					elif expires_str.count(' ') == 2:  # year information is missing
						cur_year = int(time.strftime('%Y'))
						expires = _parse_date(expires_str + ' %d' % cur_year, '%b %d %H:%M:%S %Y')
						issued = _parse_date(issued_str + ' %d' % cur_year, '%b %d %H:%M:%S %Y')
						if expires < issued:  # wraparound at new year
							expires = _parse_date(expires_str + ' %d' % (cur_year + 1), '%b %d %H:%M:%S %Y')
					self._cache.setdefault('tickets', {})[principal] = expires
				elif line.count(':') == 1:
					(key, value) = lmap(str.strip, line.split(':', 1))
					self._cache[key.lower()] = value
		except Exception:
			raise AccessTokenError('Unable to parse kerberos ticket information!')
		proc.status_raise(timeout=0)
		return self._cache

	def _refresh_access_token(self):
		timeleft_before = str_time_long(self._get_timeleft(cached=False))
		LocalProcess(self._kinit_exec, '-R').finish(timeout=10)
		LocalProcess(self._aklog_exec).finish(timeout=10)
		timeleft_after = str_time_long(self._get_timeleft(cached=False))
		self._log.log(logging.INFO2, 'Time left for access token "%s" changed from %s to %s',
			self.get_object_name(), timeleft_before, timeleft_after)


def _parse_date(value, format):
	return time.mktime(time.strptime(value, format))
