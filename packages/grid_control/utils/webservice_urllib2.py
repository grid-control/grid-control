# | Copyright 2016-2017 Karlsruhe Institute of Technology
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

import sys
from grid_control.utils.webservice import RestSession
from hpfwk import ignore_exception
from python_compat import bytes2str, resolve_fun, str2bytes


def disable_ca_cert_check():  # fix ca verification error in Python 2.7.9
	import ssl
	if hasattr(ssl, '_create_unverified_context'):
		setattr(ssl, '_create_default_https_context', getattr(ssl, '_create_unverified_context'))


def resolve_sfun(*args):
	return staticmethod(resolve_fun(*args))


def urllib2_path(package):
	if sys.version_info[0:2] < (2, 7):
		return 'python_compat_' + package  # use bundled urllib in old python versions
	return package


class Urllib2Session(RestSession):
	alias_list = ['urllib2']
	ignore_exception(Exception, None, disable_ca_cert_check)
	build_opener = resolve_sfun('urllib.request:build_opener', urllib2_path('urllib2:build_opener'))
	HTTPSConnection = resolve_sfun('http.client:HTTPSConnection', 'httplib:HTTPSConnection')
	HTTPSHandler = resolve_sfun('urllib.request:HTTPSHandler', urllib2_path('urllib2:HTTPSHandler'))
	Request = resolve_sfun('urllib.request:Request', urllib2_path('urllib2:Request'))
	urlencode = resolve_sfun('urllib.parse:urlencode', 'urllib:urlencode')

	def request(self, mode, url, headers, params=None, data=None, cert=None):
		request_fun = {RestSession.GET: lambda: 'GET', RestSession.PUT: lambda: 'PUT',
			RestSession.POST: lambda: 'POST', RestSession.DELETE: lambda: 'DELETE'}[mode]
		if params:
			url += '?%s' % Urllib2Session.urlencode(params)
		if data:
			data = str2bytes(data)
		request = Urllib2Session.Request(url=url, data=data, headers=headers)
		request.get_method = request_fun
		return bytes2str(self._get_opener(cert).open(request).read())

	def _get_opener(self, cert):
		class HTTPSClientAuthHandler(Urllib2Session.HTTPSHandler):
			def https_open(self, req):
				return self.do_open(self._get_connection, req)

			def _get_connection(self, host, timeout=None):
				return Urllib2Session.HTTPSConnection(host, key_file=cert, cert_file=cert)
		setattr(HTTPSClientAuthHandler, 'getConnection',
			getattr(HTTPSClientAuthHandler, '_get_connection'))

		if cert:
			return Urllib2Session.build_opener(HTTPSClientAuthHandler())
		return Urllib2Session.build_opener()
