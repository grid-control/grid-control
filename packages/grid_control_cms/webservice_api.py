#-#  Copyright 2013-2014 Karlsruhe Institute of Technology
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

# Helper to access CMS webservices (SiteDB, Phedex)

def removeUnicode(obj):
	if type(obj) in (list, tuple, set):
		(obj, oldType) = (list(obj), type(obj))
		for i, v in enumerate(obj):
			obj[i] = removeUnicode(v)
		obj = oldType(obj)
	elif isinstance(obj, dict):
		result = {}
		for k, v in obj.iteritems():
			result[removeUnicode(k)] = removeUnicode(v)
		return result
	elif isinstance(obj, unicode):
		return str(obj)
	return obj

def readURL(url, params = None, headers = {}, cert = None):
	import urllib, urllib2, httplib
	class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
		def __init__(self, key, cert):
			urllib2.HTTPSHandler.__init__(self)
			(self.key, self.cert) = (key, cert)
		def https_open(self, req):
			return self.do_open(self.getConnection, req)
		def getConnection(self, host, timeout = None):
			return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

	if cert:
		cert_handler = HTTPSClientAuthHandler(cert, cert)
		opener = urllib2.build_opener(cert_handler)
		urllib2.install_opener(opener)

	if params:
		url += '?%s' % urllib.urlencode(params)
	return urllib2.urlopen(urllib2.Request(url, None, headers)).read()

def parseJSON(data):
	import json
	return removeUnicode(json.loads(data.replace('\'', '"')))

def readJSON(url, params = None, headers = {}, cert = None):
	return parseJSON(readURL(url, params, headers, cert))
