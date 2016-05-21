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
from grid_control.utils.parsing import parseJSON
from hpfwk import AbstractError, NestedException
from python_compat import bytes2str, identity, json, str2bytes, urllib2

try:
	from urllib import urlencode
except Exception:
	from urllib.parse import urlencode

class RestError(NestedException):
	pass


class RestClientBase(object):
	def __init__(self, cert = None, url = None, default_headers = None,
			process_result = None, process_data = None,
			rf_get = None, rf_post = None, rf_put = None, rf_delete = None):
		self._log = logging.getLogger('webservice')
		(self._cert, self._url, self._headers) = (cert, url, default_headers)
		(self._rf_get, self._rf_post, self._rf_put, self._rf_delete) = (rf_get, rf_post, rf_put, rf_delete)
		(self._process_result, self._process_data) = (process_result or identity, process_data or urlencode)

	def _get_headers(self, headers):
		request_headers = dict(self._headers or {})
		request_headers.update(headers or {})
		return request_headers

	def _get_url(self, url, api):
		if url is None:
			url = self._url
		if api:
			url += '/%s' % api
		return url

	def _request(self, request_fun, url, api, headers, params = None, data = None):
		raise AbstractError

	def get(self, url = None, api = None, headers = None, params = None):
		return self._process_result(self._request(self._rf_get, url, api, headers, params = params))

	def post(self, url = None, api = None, headers = None, data = None):
		if data and (self._process_data != identity):
			data = self._process_data(data)
		return self._process_result(self._request(self._rf_post, url, api, headers, data = data))

	def put(self, url = None, api = None, headers = None, params = None, data = None):
		if data and (self._process_data != identity):
			data = self._process_data(data)
		return self._process_result(self._request(self._rf_put, url, api, headers, params = params, data = data))

	def delete(self, url = None, api = None, headers = None, params = None):
		return self._process_result(self._request(self._rf_delete, url, api, headers, params = params))


class RequestsRestClient(RestClientBase):
	_session = None

	def __init__(self, cert = None, url = None, default_headers = None, process_result = None, process_data = None):
		if not self._session:
			#disable ssl ca verification errors
			requests.packages.urllib3.disable_warnings()
			self._session = requests.Session()
		RestClientBase.__init__(self, cert = cert, url = url, default_headers = default_headers,
			process_result = process_result, process_data = process_data,
			rf_get = self._session.get, rf_post = self._session.post,
			rf_put = self._session.put, rf_delete = self._session.delete)

	def _request(self, request_fun, url, api, headers, params = None, data = None):
		request_headers = self._get_headers(headers)
		response = request_fun(url = self._get_url(url, api), verify = False,
			cert = self._cert, headers = request_headers, params = params, data = data)
		try:
			response.raise_for_status()
		except Exception:
			raise RestError('Request result: %s' % response.text)
		return response.text


class Urllib2RestClient(RestClientBase):
	def __init__(self, cert = None, url = None, default_headers = None, process_result = None, process_data = None):
		RestClientBase.__init__(self, cert = cert, url = url, default_headers = default_headers,
			process_result = process_result, process_data = process_data,
			rf_get = lambda: 'GET', rf_post = lambda: 'POST',
			rf_put = lambda: 'PUT', rf_delete = lambda: 'DELETE')

		class HTTPSClientAuthHandler(HTTPSHandler):
			def https_open(self, req):
				return self.do_open(self.getConnection, req)
			def getConnection(self, host, timeout = None):
				return HTTPSConnection(host, key_file = cert, cert_file = cert)
		self._https_handler = HTTPSClientAuthHandler

	def _request(self, request_fun, url, api, headers, params = None, data = None):
		url = self._get_url(url, api)
		if params:
			url += '?%s' % urlencode(params)
		request_headers = self._get_headers(headers)
		if data:
			data = str2bytes(data)
		request = Request(url = url, data = data, headers = request_headers)
		request.get_method = request_fun
		if self._cert:
			cert_handler = self._https_handler()
			opener = build_opener(cert_handler)
		else:
			opener = build_opener()
		return bytes2str(opener.open(request).read())

try:
	import requests
	RestClient = RequestsRestClient
except Exception: # incompatible dependencies pulled in can cause many types of exceptions
	try:
		import ssl # fix ca verification error in Python 2.7.9
		if hasattr(ssl, '_create_unverified_context'):
			setattr(ssl, '_create_default_https_context', getattr(ssl, '_create_unverified_context'))
	except Exception:
		pass
	try:
		from http.client import HTTPSConnection
		from urllib.request import HTTPSHandler, Request, build_opener
	except Exception:
		from httplib import HTTPSConnection
		HTTPSHandler = urllib2.HTTPSHandler
		Request = urllib2.Request
		build_opener = urllib2.build_opener
	RestClient = Urllib2RestClient # fall back to urllib2


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
			raise self._cert_errror_cls(self._fmt_cert_error('Environment variable X509_USER_PROXY is "%s"' % proxyPath))
		return JSONRestClient._request(self, request_fun, url, api, headers, params, data)
