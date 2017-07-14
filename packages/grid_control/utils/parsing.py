# | Copyright 2015-2017 Karlsruhe Institute of Technology
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

from hpfwk import clear_current_exception, ignore_exception
from python_compat import identity, imap, json, lfilter, lmap, next, reduce, set, sorted, unicode


def parse_bool(value):
	if value.lower() in ('yes', 'y', 'true', 't', 'ok', '1', 'on'):
		return True
	if value.lower() in ('no', 'n', 'false', 'f', 'fail', '0', 'off'):
		return False


def parse_dict_cfg(entries, parser_value=identity, parser_key=identity):
	(result, result_parsed, order) = ({}, {}, [])
	key = None
	for entry in entries.splitlines():
		if '=>' in entry:
			key, entry = entry.split('=>', 1)
			key = key.strip()
			if key and (key not in order):
				order.append(key)
		if (key is not None) or entry.strip() != '':
			result.setdefault(key, []).append(entry.strip())

	def _parse_key(key):
		if key:
			return parser_key(key)

	for key, value in result.items():
		value = parser_value(str.join('\n', value).strip())
		result_parsed[_parse_key(key)] = value
	return (result_parsed, lmap(_parse_key, order))


def parse_json(data):
	try:
		return remove_unicode(json.loads(data))
	except Exception:
		clear_current_exception()
		return remove_unicode(json.loads(data.replace("'", '"')))


def parse_list(value, delimeter, filter_fun=lambda x: x not in ['', '\n']):
	if value:
		return lfilter(filter_fun, imap(str.strip, value.split(delimeter)))
	return []


def parse_str(value, cls, default=None):
	return ignore_exception(Exception, default, cls, value)


def parse_time(usertime):
	if usertime is None or usertime == '':
		return -1
	tmp = lmap(int, usertime.split(':'))
	while len(tmp) < 3:
		tmp.append(0)
	if tmp[2] > 59 or tmp[1] > 59 or len(tmp) > 3:
		raise Exception('Invalid time format: %s' % usertime)
	return reduce(lambda x, y: x * 60 + y, tmp)


def parse_type(value):
	def _parse_number(value):
		if '.' in value:
			return float(value)
		return int(value)
	return ignore_exception(ValueError, value, _parse_number, value)


def remove_unicode(obj):
	if unicode == str:  # protection against certain external & invasive compatibility layers
		return obj
	elif isinstance(obj, (list, tuple, set)):
		obj = type(obj)(imap(remove_unicode, obj))
	elif isinstance(obj, dict):
		result = type(obj)()
		for (key, value) in obj.items():
			result[remove_unicode(key)] = remove_unicode(value)
		return result
	elif isinstance(obj, unicode):
		return obj.encode('utf-8')
	return obj


def split_advanced(tokens, do_emit, add_emit_token,
		quotes=None, brackets=None, exception_type=Exception):
	buffer = None
	tokens = split_brackets(split_quotes(tokens, quotes, exception_type), brackets, exception_type)
	token = next(tokens, None)
	while token:
		if buffer is None:
			buffer = ''
		if do_emit(token):
			yield buffer
			buffer = ''
			if add_emit_token(token):
				yield token
		else:
			buffer += token
		token = next(tokens, None)
	if buffer is not None:
		yield buffer


def split_brackets(tokens, brackets=None, exception_type=Exception):
	map_close_to_open = dict(imap(lambda x: (x[1], x[0]), brackets or ['()', '{}', '[]']))

	def _raise_backet_error(msg, token, position):
		raise exception_type('Closing bracket %r at position %d %s' % (token, position, msg))

	def _split_brackets(position, token, stack):
		if token in map_close_to_open.values():
			stack.append((token, position))
		elif token in map_close_to_open.keys():
			if not stack:
				_raise_backet_error('is without opening bracket', token, position)
			elif stack[-1][0] == map_close_to_open[token]:
				stack.pop()
				if not stack:
					return True
			else:
				_raise_backet_error('does not match bracket %r at position %d' % stack[-1], token, position)
	return _split_with_stack(tokens, _split_brackets, 'Unclosed brackets: %s', exception_type)


def split_quotes(tokens, quotes=None, exception_type=Exception):
	quotes = quotes or ['"', "'"]

	def _split_quotes(position, token, stack):
		if token in quotes:
			if stack and (stack[-1][0] == token):
				stack.pop()
				if not stack:
					return True
			else:
				stack.append((token, position))
	return _split_with_stack(tokens, _split_quotes, 'Unclosed quotes: %s', exception_type)


def str_dict_cfg(value, parser=identity, strfun=str):
	def _getmax(src):
		return max(lmap(lambda x: len(str(x)), src) + [0])

	(srcdict, srckeys) = value
	result = ''
	if srcdict.get(None) is not None:
		result = strfun(srcdict[None])
	fmt = '\n\t%%%ds => %%%ds' % (_getmax(srckeys), _getmax(srcdict.values()))
	return result + str.join('', imap(lambda k: fmt % (k, strfun(srcdict[k])), srckeys))


def str_dict_linear(mapping, keys_order=None):
	keys_sorted = sorted(mapping.keys(), key=repr)
	if keys_order is None:
		keys_order = keys_sorted
	else:
		keys_order = list(keys_order)
	keys_order.extend(lfilter(lambda x: x not in keys_order, keys_sorted))
	return str.join(', ', imap(lambda k: '%s = %s' % (k, repr(mapping.get(k))), keys_order))


def str_guid(guid):
	return '%s-%s-%s-%s-%s' % (guid[:8], guid[8:12], guid[12:16], guid[16:20], guid[20:])


def str_time_long(secs, fmt='%dh %0.2dmin %0.2dsec'):
	if (secs is not None) and (secs >= 0):
		return fmt % (secs / 60 / 60, (secs / 60) % 60, secs % 60)
	return ''


def str_time_short(secs):
	return str_time_long(secs, '%d:%0.2d:%0.2d')


def _split_with_stack(tokens, process_token, exception_msg, exception_type=Exception):
	buffer = ''
	stack = []
	position = 0
	for token in tokens:
		position += len(token)  # store position for proper error messages
		if process_token(position, token, stack):  # check if buffer should be emitted
			buffer += token
			yield buffer
			buffer = ''
		elif stack:
			buffer += token
		else:
			yield token
	if stack:
		msg_pos = str.join('; ', imap(lambda item_pos: '%r at position %d' % item_pos, stack))
		raise exception_type(exception_msg % msg_pos)
