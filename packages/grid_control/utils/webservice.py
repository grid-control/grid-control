# | Copyright 2013-2017 Karlsruhe Institute of Technology
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
from grid_control.utils.data_structures import make_enum
from grid_control.utils.parsing import parse_json
from hpfwk import AbstractError, NestedException, Plugin, clear_current_exception, ignore_exception
from python_compat import identity, json, resolve_fun


class RestError(NestedException):
	pass


class RestClient(object):
	def __init__(self, cert=None, url=None, default_headers=None,
			process_result=None, process_data=None, session=None):
		self._log = logging.getLogger('webservice')
		(self._cert, self._url, self._headers, self._session) = (cert, url, default_headers, session)
		self._process_result = process_result or identity
		self._process_data = process_data or resolve_fun('urllib.parse:urlencode', 'urllib:urlencode')
		if not session:
			try:
				self._session = RestSession.create_instance('RequestsSession')
				# pulling in incompatible dependencies can cause many different types of exceptions
			except Exception:
				clear_current_exception()
				self._session = RestSession.create_instance('Urllib2Session')

	def delete(self, url=None, api=None, headers=None, params=None):
		return self._process_result(self._request(RestSession.DELETE, url, api, headers, params=params))

	def get(self, url=None, api=None, headers=None, params=None):
		return self._process_result(self._request(RestSession.GET, url, api, headers, params=params))

	def post(self, url=None, api=None, headers=None, data=None):
		if data and (self._process_data != identity):
			data = self._process_data(data)
		return self._process_result(self._request(RestSession.POST, url, api, headers, data=data))

	def put(self, url=None, api=None, headers=None, params=None, data=None):
		if data and (self._process_data != identity):
			data = self._process_data(data)
		request_result = self._request(RestSession.PUT, url, api, headers, params=params, data=data)
		return self._process_result(request_result)

	def _request(self, mode, url, api, headers, params=None, data=None):
		header_dict = dict(self._headers or {})
		header_dict.update(headers or {})
		if url is None:
			url = self._url
		if api:
			url += '/%s' % api
		try:
			return self._session.request(mode, url=url, headers=header_dict,
				params=params, data=data, cert=self._cert)
		except Exception:
			raise RestError('Unable to query %r (params: %r, cert: %r, session: %r)' %
				(url, params, self._cert, self._session.__class__.__name__))


class RestSession(Plugin):
	def request(self, mode, url, headers, params=None, data=None, cert=None):
		raise AbstractError

make_enum(['GET', 'PUT', 'POST', 'DELETE'], RestSession)


class JSONRestClient(RestClient):
	def __init__(self, cert=None, url=None, default_headers=None, process_result=None):
		RestClient.__init__(self, cert, url,
			default_headers or {'Content-Type': 'application/json', 'Accept': 'application/json'},
			process_result=process_result or self._process_json_result, process_data=json.dumps)

	def _process_json_result(self, value):
		if not value:
			raise RestError('Received empty reply')
		try:
			return parse_json(value)
		except Exception:
			raise RestError('Received invalid JSON reply: %r' % value)


class GridJSONRestClient(JSONRestClient):
	def __init__(self, cert=None, url=None, cert_error_msg='', cert_error_cls=Exception):
		(self._cert_error_msg, self._cert_error_cls) = (cert_error_msg, cert_error_cls)
		JSONRestClient.__init__(self, cert, url)
		if not self._cert:
			self._cert = ignore_exception(Exception, None, self._get_grid_cert)
		if not self._cert:
			self._log.warning(self._fmt_cert_error('Using this webservice requires a valid grid proxy!'))

	def _fmt_cert_error(self, msg):
		return (self._cert_error_msg + ' ' + msg).strip()

	def _get_grid_cert(self):
		cert = os.environ.get('X509_USER_PROXY', '')
		if not cert:
			self._raise_cert_error('Environment variable X509_USER_PROXY is not set!')
		cert = os.path.expandvars(os.path.normpath(os.path.expanduser(cert)))
		if os.path.exists(cert) and os.path.isfile(cert) and os.access(cert, os.R_OK):
			return cert
		self._raise_cert_error('Environment variable X509_USER_PROXY points to invalid path "%s"' % cert)

	def _raise_cert_error(self, msg):
		raise self._cert_error_cls(self._fmt_cert_error(msg))

	def _request(self, request_fun, url, api, headers, params=None, data=None):
		if not self._cert:
			self._cert = self._get_grid_cert()
		return JSONRestClient._request(self, request_fun, url, api, headers, params, data)
