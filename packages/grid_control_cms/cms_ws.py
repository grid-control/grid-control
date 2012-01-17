# Helper to access CMS webservices (SiteDB, Phedex)
from python_compat import *

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

def readURL(url, params, headers = {}):
	import json, urllib, urllib2
	req = urllib2.Request(url, headers)
	return urllib2.urlopen(req, urllib.urlencode(params)).read()

def readJSON(url, params, headers = {}):
	import json
	return removeUnicode(json.loads(readURL(url, params, headers).replace('\'', '"')))
