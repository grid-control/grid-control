#-#  Copyright 2013-2015 Karlsruhe Institute of Technology
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

# Helper to access CMS web-services (DBS, SiteDB, PhEDEx)

import urllib
import urllib2
import httplib

try:
	import requests
except ImportError:
	#fall back to urllib2

	#fix ca verification error in Python 2.7.9
	try:
		import ssl
		ssl._create_default_https_context = ssl._create_unverified_context
	except AttributeError:
		pass

	class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
		def __init__(self, key=None, cert=None):
			urllib2.HTTPSHandler.__init__(self)
			self.key, self.cert = key, cert
		def https_open(self, req):
			return self.do_open(self.getConnection, req)
		def getConnection(self, host, timeout = None):
			return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

	class RestClient(object):
		def __init__(self, cert=None, default_headers=None):
			self.cert = cert
			self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
			if default_headers:
				self.headers = default_headers

		def _build_opener(self):
			if self.cert:
				cert_handler = HTTPSClientAuthHandler(self.cert, self.cert)
				return urllib2.build_opener(cert_handler)
			else:
				return urllib2.build_opener()

		def _format_url(self, url, api, params):
			if api:
				url += '/%s' % api

			if params:
				url += '?%s' % urllib.urlencode(params)

			return url

		def _request_headers(self, headers):
			request_headers = {}
			request_headers.update(self.headers)

			if headers:
				request_headers.update(headers)

			return request_headers

		def get(self, url, api=None, headers=None, params=None):
			request_headers = self._request_headers(headers)

			url = self._format_url(url, api, params)
			opener = self._build_opener()
			return opener.open(urllib2.Request(url, None, request_headers)).read()

		def post(self, url, api, data=None, headers=None):
			request_headers = self._request_headers(headers)
			#necessary to optimize performance of CherryPy
			request_headers['Content-length'] = str(len(data))

			request = urllib2.Request(url='%s/%s' % (url, api) if api else url, data=data, headers=headers)
			request.get_method = lambda: 'POST'

			opener = self._build_opener()
			return opener.open(request).read()

		def put(self, url, api, data=None, headers=None, params=None):
			request_headers = self._request_headers(headers)
			#necessary to optimize performance of CherryPy
			request_headers['Content-length'] = str(len(data))

			url = self._format_url(url, api, params)
			request = urllib2.Request(url=url, data=data, headers=headers)
			request.get_method = lambda: 'PUT'

			opener = self._build_opener()
			return opener.open(request).read()

		def delete(self, url, api, headers=None, params=None):
			request_headers = self._request_headers(headers)

			url = self._format_url(url, api, params)
			request = urllib2.Request(url=url, headers=request_headers)
			request.get_method = lambda: 'DELETE'

			opener = self._build_opener()
			return opener.open(request).read()

else:
	from requests.exceptions import HTTPError
	#disable ssl ca verification errors
	requests.packages.urllib3.disable_warnings()

	class RestClient(object):
		_requests_client_session = None

		def __init__(self, cert = None, default_headers = None):
			if not self._requests_client_session:
				self._requests_client_session = requests.Session()
			self.cert = cert
			self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
			if default_headers:
				self.headers = default_headers

		def _raise_for_status(self, response):
			"""
			checks for status not ok and raises corresponding http error
			"""
			try:
				response.raise_for_status()
			except HTTPError as http_error:
				raise HTTPError("Server response: %s\nInfos: %s" % (http_error.message, response.text))

		def _request(self, request_fun, url, api = None, headers = None, **kwargs):
			request_headers = {}
			request_headers.update(self.headers)
			if headers:
				request_headers.update(headers)

			if api:
				url = '%s/%s' % (url, api)
			response = request_fun(url = url, verify = False,
				cert = self.cert, headers = request_headers, **kwargs)
			self._raise_for_status(response)
			return response.text

		def get(self, url, api = None, headers = None, params = None):
			return self._request(self._requests_client_session.get, url, api, headers,
				params = params)

		def post(self, url, api, headers = None, data = None):
			return self._request(self._requests_client_session.post, url, api, headers,
				data = data)

		def put(self, url, api, data=None, headers=None, params=None):
			return self._request(self._requests_client_session.put, url, api, headers,
				params = params, data = data)

		def delete(self, url, api, headers=None, params=None):
			return self._request(self._requests_client_session.delete, url, api, headers,
				params = params)


def readURL(url, params=None, headers=None, cert=None):
	rest_client = RestClient(cert)
	return rest_client.get(url=url, headers=headers, params=params)

def readJSON(url, params = None, headers = {}, cert = None):
	return parseJSON(readURL(url, params, headers, cert))
