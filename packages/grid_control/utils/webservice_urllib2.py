# | Copyright 2016 Karlsruhe Institute of Technology
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
from hpfwk import clear_current_exception
from python_compat import bytes2str, str2bytes

try:
	import ssl # fix ca verification error in Python 2.7.9
	if hasattr(ssl, '_create_unverified_context'):
		setattr(ssl, '_create_default_https_context', getattr(ssl, '_create_unverified_context'))
except Exception:
	clear_current_exception()

try:
	from http.client import HTTPSConnection
	from urllib.parse import urlencode
	from urllib.request import HTTPSHandler, Request, build_opener
except Exception:
	from httplib import HTTPSConnection
	from urllib import urlencode
	if sys.version_info[0:2] < (2, 7):
		from python_compat_urllib2 import HTTPSHandler, Request, build_opener
	else:
		from urllib2 import HTTPSHandler, Request, build_opener


class Urllib2Session(RestSession):
	alias = ['urllib2']

	def request(self, mode, url, headers, params = None, data = None, cert = None):
		request_fun = {RestSession.GET: lambda: 'GET', RestSession.PUT: lambda: 'PUT',
			RestSession.POST: lambda: 'POST', RestSession.DELETE: lambda: 'DELETE'}[mode]
		if params:
			url += '?%s' % urlencode(params)
		if data:
			data = str2bytes(data)
		request = Request(url = url, data = data, headers = headers)
		request.get_method = request_fun
		if cert:
			class HTTPSClientAuthHandler(HTTPSHandler):
				def https_open(self, req):
					return self.do_open(self.getConnection, req)
				def getConnection(self, host, timeout = None):
					return HTTPSConnection(host, key_file = cert, cert_file = cert)
			opener = build_opener(HTTPSClientAuthHandler())
		else:
			opener = build_opener()
		return bytes2str(opener.open(request).read())
