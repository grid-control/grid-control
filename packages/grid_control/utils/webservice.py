#-#  Copyright 2013-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from grid_control.utils.parsing import parseJSON
from hpfwk import AbstractError, NestedException

class RestError(NestedException):
	pass


class RestClientBase(object):
	def __init__(self, cert = None, default_headers = None, rf_get = None, rf_post = None, rf_put = None, rf_delete = None):
		self._cert = cert
		self._headers = default_headers or {'Content-type': 'application/json', 'Accept': 'application/json'}
		(self._rf_get, self._rf_post, self._rf_put, self._rf_delete) = (rf_get, rf_post, rf_put, rf_delete)

	def _get_headers(self, headers):
		request_headers = dict(self._headers)
		request_headers.update(headers or {})
		return request_headers

	def _get_url(self, url, api):
		if api:
			url += '/%s' % api
		return url

	def _request(self, request_fun, url, api, headers, **kwargs):
		raise AbstractError

	def get(self, url, api = None, headers = None, params = None):
		return self._request(self._rf_get, url, api, headers, params = params)

	def post(self, url, api = None, headers = None, data = None):
		return self._request(self._rf_post, url, api, headers, data = data)

	def put(self, url, api = None, headers = None, params = None, data = None):
		return self._request(self._rf_put, url, api, headers, params = params, data = data)

	def delete(self, url, api = None, headers = None, params = None):
		return self._request(self._rf_delete, url, api, headers, params = params)


class RequestsRestClient(RestClientBase):
	_session = None

	def __init__(self, cert = None, default_headers = None):
		if not self._session:
			#disable ssl ca verification errors
			requests.packages.urllib3.disable_warnings()
			self._session = requests.Session()
		RestClientBase.__init__(self, cert = cert, default_headers = default_headers,
			rf_get = self._session.get, rf_post = self._session.post,
			rf_put = self._session.put, rf_delete = self._session.delete)

	def _request(self, request_fun, url, api, headers, **kwargs):
		request_headers = self._get_headers(headers)
		response = request_fun(url = self._get_url(url, api), verify = False,
			cert = self._cert, headers = request_headers, **kwargs)
		try:
			response.raise_for_status()
		except Exception:
			raise RestError('Request result: %s' % response.text)
		return response.text


class Urllib2RestClient(RestClientBase):
	def __init__(self, cert = None, default_headers = None):
		RestClientBase.__init__(self, cert = cert, default_headers = default_headers,
			rf_get = lambda: 'GET', rf_post = lambda: 'POST',
			rf_put = lambda: 'PUT', rf_delete = lambda: 'DELETE')

		class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
			def https_open(self, req):
				return self.do_open(self.getConnection, req)
			def getConnection(self, host, timeout = None):
				return httplib.HTTPSConnection(host, key_file = cert, cert_file = cert)
		self._https_handler = HTTPSClientAuthHandler

	def _request(self, request_fun, url, api, headers, params = None, data = None):
		url = self._get_url(url, api)
		if params:
			url += '?%s' % urllib.urlencode(params)
		request_headers = self._get_headers(headers)
		if data:
			#necessary to optimize performance of CherryPy
			request_headers['Content-length'] = str(len(data))
		request = urllib2.Request(url = url, data = data, headers = request_headers)
		request.get_method = request_fun
		if self._cert:
			cert_handler = self._https_handler()
			opener = urllib2.build_opener(cert_handler)
		else:
			opener = urllib2.build_opener()
		return opener.open(request).read()

try:
	import requests
	RestClient = RequestsRestClient
except Exception: # incompatible dependencies pulled in can cause many types of exceptions
	try:
		import ssl # fix ca verification error in Python 2.7.9
		ssl._create_default_https_context = ssl._create_unverified_context
	except Exception:
		pass
	import httplib, urllib, urllib2
	RestClient = Urllib2RestClient # fall back to urllib2


def readURL(url, params = None, headers = None, cert = None):
	rest_client = RestClient(cert)
	return rest_client.get(url = url, headers = headers, params = params)


def readJSON(url, params = None, headers = None, cert = None):
	return parseJSON(readURL(url, params, headers, cert))
