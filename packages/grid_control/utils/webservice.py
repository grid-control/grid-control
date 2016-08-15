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
			except Exception: # pulling in incompatible dependencies can cause many different types of exceptions
				self._session = RestSession.createInstance('Urllib2Session')

	def _request(self, mode, url, api, headers, params = None, data = None):
		hdr = dict(self._headers or {})
		hdr.update(headers or {})
		if url is None:
			url = self._url
		if api:
			url += '/%s' % api
		try:
			return self._session.request(mode, url = url, headers = hdr, params = params, data = data, cert = self._cert)
		except Exception:
			raise RestError('Unable to query %r (params: %r, cert: %r, session: %r)' % (url, params, self._cert, self._session.__class__.__name__))

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
		if not value:
			raise RestError('Received empty reply')
		try:
			return parseJSON(value)
		except Exception:
			raise RestError('Received invalid JSON reply: %r' % value)


class GridJSONRestClient(JSONRestClient):
	def __init__(self, url = None, cert_error_msg = '', cert_error_cls = Exception):
		(self._cert_error_msg, self._cert_error_cls) = (cert_error_msg, cert_error_cls)
		JSONRestClient.__init__(self, url = url)
		try:
			self._get_current_cert()
		except Exception:
			self._log.warning(self._fmt_cert_error('Using this webservice requires a valid grid proxy!'))

	def _fmt_cert_error(self, msg):
		return (self._cert_error_msg + ' ' + msg).strip()

	def _get_current_cert(self):
		cert = os.environ.get('X509_USER_PROXY', '')
		if not cert:
			raise self._cert_error_cls(self._fmt_cert_error('Environment variable X509_USER_PROXY is not set!'))
		cert = os.path.expandvars(os.path.normpath(os.path.expanduser(cert)))
		if os.path.exists(cert) and os.path.isfile(cert) and os.access(cert, os.R_OK):
			return cert
		raise self._cert_error_cls(self._fmt_cert_error('Environment variable X509_USER_PROXY points to invalid path "%s"' % cert))

	def _request(self, request_fun, url, api, headers, params = None, data = None):
		self._cert = self._get_current_cert()
		return JSONRestClient._request(self, request_fun, url, api, headers, params, data)
