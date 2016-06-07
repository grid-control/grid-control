# | Copyright 2013-2016 Karlsruhe Institute of Technology
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

import os, logging
from grid_control.utils.data_structures import makeEnum
from grid_control.utils.parsing import parseJSON
from hpfwk import AbstractError, NestedException, Plugin
from python_compat import identity, json

try:
	from urllib import urlencode
except Exception:
	from urllib.parse import urlencode


class RestError(NestedException):
	pass


class RestSession(Plugin):
	def __init__(self):
		pass

	def request(self, mode, url, headers, params = None, data = None, cert = None):
		raise AbstractError
makeEnum(['GET', 'PUT', 'POST', 'DELETE'], RestSession)


class RestClient(object):
	def __init__(self, cert = None, url = None, default_headers = None,
			process_result = None, process_data = None, session = None):
		self._log = logging.getLogger('webservice')
		(self._cert, self._url, self._headers) = (cert, url, default_headers)
		(self._process_result, self._process_data) = (process_result or identity, process_data or urlencode)
		if not session:
			try:
				self._session = RestSession.createInstance('RequestsSession')
			except Exception: # incompatible dependencies pulled in can cause many types of exceptions
				self._session = RestSession.createInstance('Urllib2Session')

	def _request(self, mode, url, api, headers, params = None, data = None):
		request_headers = dict(self._headers or {})
		request_headers.update(headers or {})
		if url is None:
			url = self._url
		if api:
			url += '/%s' % api
		return self._session.request(mode, url = url, headers = request_headers, params = params, data = data, cert = self._cert)

	def get(self, url = None, api = None, headers = None, params = None):
		return self._process_result(self._request(RestSession.GET, url, api, headers, params = params))

	def post(self, url = None, api = None, headers = None, data = None):
		if data and (self._process_data != identity):
			data = self._process_data(data)
		return self._process_result(self._request(RestSession.POST, url, api, headers, data = data))

	def put(self, url = None, api = None, headers = None, params = None, data = None):
		if data and (self._process_data != identity):
			data = self._process_data(data)
		return self._process_result(self._request(RestSession.PUT, url, api, headers, params = params, data = data))

	def delete(self, url = None, api = None, headers = None, params = None):
		return self._process_result(self._request(RestSession.DELETE, url, api, headers, params = params))


class JSONRestClient(RestClient):
	def __init__(self, cert = None, url = None, default_headers = None, process_result = None):
		RestClient.__init__(self, cert, url,
			default_headers or {'Content-Type': 'application/json', 'Accept': 'application/json'},
			process_result = process_result or self._process_json_result, process_data = json.dumps)

	def _process_json_result(self, value):
		try:
			return parseJSON(value)
		except Exception:
			if not value:
				raise RestError('Received empty reply')
			raise RestError('Received invalid JSON reply: %r' % value)


class GridJSONRestClient(JSONRestClient):
	def __init__(self, url = None, cert_errror_msg = '', cert_errror_cls = Exception, cert = None):
		(self._cert_errror_msg, self._cert_errror_cls) = (cert_errror_msg, cert_errror_cls)
		if cert is None:
			cert = os.environ.get('X509_USER_PROXY', '')
		JSONRestClient.__init__(self, cert = cert, url = url)
		if (not cert) or (not os.path.exists(cert)):
			self._log.warning(self._fmt_cert_error('Unable to use this grid webservice!'))

	def _fmt_cert_error(self, msg):
		return (self._cert_errror_msg + ' ' + msg).strip()

	def _request(self, request_fun, url, api, headers, params = None, data = None):
		if not self._cert:
			raise self._cert_errror_cls(self._fmt_cert_error('Environment variable X509_USER_PROXY is not set!'))
		proxyPath = os.path.expandvars(os.path.normpath(os.path.expanduser(self._cert)))
		if not os.path.exists(proxyPath):
			raise self._cert_errror_cls(self._fmt_cert_error('Environment variable X509_USER_PROXY points to missing file "%s"' % proxyPath))
		return JSONRestClient._request(self, request_fun, url, api, headers, params, data)
