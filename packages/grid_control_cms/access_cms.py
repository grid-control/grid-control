# | Copyright 2017 Karlsruhe Institute of Technology
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

import os, getpass, logging
from grid_control.backends import AccessToken
from grid_control.config import create_config
from grid_control.utils import resolve_install_path
from grid_control.utils.process_base import LocalProcess
from hpfwk import NestedException, ignore_exception


class CMSAuthenticationException(NestedException):
	pass


def get_cms_cert(config=None, ignore_errors=False):
	logging.getLogger('access.cms-proxy').setLevel(logging.ERROR)
	if not ignore_errors:
		return _get_cms_cert(config or create_config())
	return ignore_exception(Exception, None, _get_cms_cert, config or create_config())


def _get_cms_cert(config):
	config = config.change_view(set_sections=['cms', 'access', 'proxy'])
	try:
		access = AccessToken.create_instance('VomsAccessToken', config, 'cms-proxy')
	except Exception:
		if os.environ.get('X509_USER_PROXY'):
			return os.environ['X509_USER_PROXY']
		raise CMSAuthenticationException('Unable to find grid environment')
	can_submit = ignore_exception(Exception, False, access.can_submit, 5 * 60, True)
	if not can_submit:
		logging.getLogger('access.cms').warning('The grid proxy has expired or is invalid!')
		role = config.get_list('new proxy roles', '', on_change=None)
		timeout = config.get_time('new proxy timeout', 10, on_change=None)
		lifetime = config.get_time('new proxy lifetime', 192 * 60, on_change=None)
		# password in variable name removes it from debug log
		password = getpass.getpass('Please enter proxy password: ')
		try:
			proxy_init_exec = resolve_install_path('voms-proxy-init')
			proc = LocalProcess(proxy_init_exec, '--voms', str.join(':', ['cms'] + role),
				'--valid', '%d:%d' % (lifetime / 60, lifetime % 60), logging=False)
			if password:
				proc.stdin.write(password + '\n')
				proc.stdin.close()
			proc.get_output(timeout=timeout)
		except Exception:
			raise CMSAuthenticationException('Unable to create new grid proxy')
		access = AccessToken.create_instance('VomsAccessToken', config, 'cms-proxy')  # new instance
		can_submit = ignore_exception(Exception, False, access.can_submit, 5 * 60, True)
		if not can_submit:
			raise CMSAuthenticationException('Newly created grid proxy is also invalid')
	return access.get_auth_fn_list()[0]
