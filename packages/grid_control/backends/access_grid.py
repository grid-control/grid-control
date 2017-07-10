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

from grid_control.backends.access import AccessTokenError, TimedAccessToken
from grid_control.utils import DictFormat, resolve_install_path
from grid_control.utils.process_base import LocalProcess
from hpfwk import AbstractError
from python_compat import identity


class GridAccessToken(TimedAccessToken):
	def __init__(self, config, name, proxy_exec):
		TimedAccessToken.__init__(self, config, name)
		self._proxy_info_exec = resolve_install_path(proxy_exec)
		self._proxy_fn = config.get('proxy path', '')
		self._ignore_warning = config.get_bool('ignore warnings', False, on_change=None)
		self._cache = None

	def get_fq_user_name(self):
		return self._get_proxy_info('identity')

	def get_group(self):
		return self._get_proxy_info('vo')

	def get_proxy_fn(self):
		return self._proxy_fn

	def get_user_name(self):
		return self._get_proxy_info('identity').split('CN=')[1].strip()

	def _get_proxy_info(self, key, parse=identity, cached=True):
		info = self._parse_proxy(cached)
		try:
			return parse(info[key])
		except Exception:
			raise AccessTokenError("Can't access %r in proxy information:\n%s" % (key, info))

	def _get_proxy_info_arguments(self):
		raise AbstractError

	def _parse_proxy(self, cached=True):
		# Return cached results if requested
		if cached and self._cache:
			return self._cache
		# Call voms-proxy-info and parse results
		proc = LocalProcess(self._proxy_info_exec, *self._get_proxy_info_arguments())
		(exit_code, stdout, stderr) = proc.finish(timeout=10)
		if (exit_code != 0) and not self._ignore_warning:
			msg = ('%s output:\n%s\n%s\n' % (self._proxy_info_exec, stdout, stderr)).replace('\n\n', '\n')
			msg += 'If job submission is still possible, you can set [access] ignore warnings = True\n'
			msg += '%s failed with return code %d' % (self._proxy_info_exec, exit_code)
			raise AccessTokenError(msg)
		self._cache = DictFormat(':').parse(stdout)
		if not self._cache:
			msg = 'Unable to parse access token information:\n\t%s\n\t%s\n'
			raise AccessTokenError(msg % (stdout.strip(), stderr.strip()))
		return self._cache
