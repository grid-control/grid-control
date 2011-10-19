# Helper to access CMS webservices (SiteDB, Phedex)
from python_compat import *

def readJSON(url, params):
	import json, urllib, urllib2
	tmp = urllib2.urlopen('%s?%s' % (url, urllib.urlencode(params))).read()
	def removeUnicode(obj):
		oldType = type(obj)
		if type(obj) in (list, tuple, set):
			obj = list(obj)
			for i, v in enumerate(obj):
				obj[i] = removeUnicode(v)
			obj = oldType(obj)
		elif isinstance(obj, dict):
			for k, v in obj.iteritems():
				obj[removeUnicode(k)] = removeUnicode(v)
		elif isinstance(obj, unicode):
			return str(obj)
		return obj
	return removeUnicode(json.loads(tmp))
