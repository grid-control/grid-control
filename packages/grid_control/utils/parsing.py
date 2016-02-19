#-#  Copyright 2015-2016 Karlsruhe Institute of Technology
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

from python_compat import identity, imap, ismap, json, lfilter, lmap, reduce, set, sorted, unicode

def removeUnicode(obj):
	if unicode == str:
		return obj
	if type(obj) in (list, tuple, set):
		(obj, oldType) = (list(obj), type(obj))
		for i, v in enumerate(obj):
			obj[i] = removeUnicode(v)
		obj = oldType(obj)
	elif isinstance(obj, dict):
		result = {}
		for k, v in obj.items():
			result[removeUnicode(k)] = removeUnicode(v)
		return result
	elif isinstance(obj, unicode):
		return obj.encode('utf-8')
	return obj


def parseJSON(data):
	return removeUnicode(json.loads(data))


def parseStr(value, cls, default = None):
	try:
		return cls(value)
	except Exception:
		return default


def parseType(value):
	try:
		if '.' in value:
			return float(value)
		return int(value)
	except ValueError:
		return value


def parseDict(entries, parserValue = identity, parserKey = identity):
	(result, resultParsed, order) = ({}, {}, [])
	key = None
	for entry in entries.splitlines():
		if '=>' in entry:
			key, entry = lmap(str.strip, entry.split('=>', 1))
			if key and (key not in order):
				order.append(key)
		if (key is not None) or entry.strip() != '':
			result.setdefault(key, []).append(entry.strip())
	def parserKeyIntern(key):
		if key:
			return parserKey(key)
	for key, value in result.items():
		value = parserValue(str.join('\n', value).strip())
		resultParsed[parserKeyIntern(key)] = value
	return (resultParsed, lmap(parserKeyIntern, order))


def parseBool(x):
	if x.lower() in ('yes', 'y', 'true', 't', 'ok', '1', 'on'):
		return True
	if x.lower() in ('no', 'n', 'false', 'f', 'fail', '0', 'off'):
		return False


def parseList(value, delimeter, doFilter = lambda x: x not in ['', '\n']):
	if value:
		return lfilter(doFilter, imap(str.strip, value.split(delimeter)))
	return []


def parseTime(usertime):
	if usertime is None or usertime == '':
		return -1
	tmp = lmap(int, usertime.split(':'))
	while len(tmp) < 3:
		tmp.append(0)
	if tmp[2] > 59 or tmp[1] > 59 or len(tmp) > 3:
		raise Exception('Invalid time format: %s' % usertime)
	return reduce(lambda x, y: x * 60 + y, tmp)


def strTime(secs, fmt = '%dh %0.2dmin %0.2dsec'):
	if secs >= 0:
		return fmt % (secs / 60 / 60, (secs / 60) % 60, secs % 60)
	return ''
strTimeShort = lambda secs: strTime(secs, '%d:%0.2d:%0.2d')

strGuid = lambda guid: '%s-%s-%s-%s-%s' % (guid[:8], guid[8:12], guid[12:16], guid[16:20], guid[20:])

def strDict(d):
	def fmtKeyValue(key, value):
		return '%s = %s' % (key, repr(value))
	return str.join(', ', ismap(fmtKeyValue, sorted(d.items())))
