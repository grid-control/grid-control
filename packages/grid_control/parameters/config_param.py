# | Copyright 2012-2016 Karlsruhe Institute of Technology
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

import shlex
from grid_control import utils
from grid_control.config import ConfigError
from grid_control.utils.parsing import parse_dict, split_advanced, split_brackets
from python_compat import imap, irange, lmap, lzip, unspecified


def frange(start, end = None, num = None, steps = None, format = '%g'):
	if (end is None) and (num is None):
		raise ConfigError('frange: No exit condition!')
	if (end is not None) and (num is not None) and (steps is not None):
		raise ConfigError('frange: Overdetermined parameters!')
	if (end is not None) and (num is not None) and (steps is None):
		steps = float(end - start) / (num - 1)
	if (end is not None) and (num is None):
		steps = steps or 1
		num = int(1 + (end - start) / steps)
	return lmap(lambda x: format % x, imap(lambda i: start + (steps or 1) * i, irange(num)))


def parse_parameter_option(option):
	# first token is variable / tuple - rest is option specifier: "a option" or "(a,b) option"
	tokens = list(split_brackets(option.lower()))
	if len(tokens) and '(' in tokens[0]:
		# parse tuple in as general way as possible
		def valid_char(c):
			return c.isalnum() or (c in ['_'])
		result = [tuple(utils.accumulate(tokens[0], '', lambda i, b: not valid_char(i), lambda i, b: valid_char(i)))]
		if tokens[1:]:
			result.append(str.join('', tokens[1:]).strip())
	else:
		result = str.join('', tokens).strip().split(' ', 1)
	if len(result) == 1:
		result.append(None)
	return tuple(result)


def parse_parameter_option_list(option_list):
	(map_vn2varexpr, map_varexpr_suffix2opt) = ({}, {})
	for opt in option_list:
		varexpr, suffix = parse_parameter_option(opt)
		map_varexpr_suffix2opt[(varexpr, suffix)] = opt
		if suffix is None:
			if isinstance(varexpr, tuple):
				for vn in varexpr:
					map_vn2varexpr[vn] = varexpr
			else:
				map_vn2varexpr[varexpr] = varexpr
	return (map_vn2varexpr, map_varexpr_suffix2opt)


def parse_tuple(t, delimeter):
	t = t.strip()
	if t.startswith('('):
		return tuple(imap(str.strip, split_advanced(t[1:-1], lambda tok: tok == delimeter, lambda tok: False)))
	return (t,)


class ParameterConfig(object):
	def __init__(self, config):
		(self._config, self._changes) = (config, [])
		(self._map_vn2varexpr, self._map_varexpr_suffix2opt) = parse_parameter_option_list(config.getOptions())

	def get(self, varexpr, suffix = None, default = unspecified):
		return self._config.get(self._get_opt(varexpr, suffix), default, onChange = self._on_change)

	def get_config(self, *args, **kwargs):
		return self._config.changeView(*args, **kwargs)

	def get_parameter(self, vn):
		varexpr = self._get_varexpr(vn)

		if isinstance(varexpr, tuple):
			outer_idx = list(varexpr).index(vn.lower())
			outer_value = self.get(varexpr, None, '')
			outer_type = self.get(varexpr, 'type', 'tuple')
			inner_type = self.get(vn, 'type', 'verbatim')
			def parse_value(value): # extract the outer_idx-nth variable and parse it just like a normal value
				return self._process_parameter_list(vn, self._parse_parameter_tuple(vn, value, outer_type, inner_type, outer_idx))
			return self._handle_dict(vn, outer_value, parse_value)
		else:
			parameter_value = self.get(vn, None, '')
			parameter_type = self.get(vn, 'type', 'default')
			def parse_value(value): # parse the parameter value - using the specified interpretation
				return self._process_parameter_list(vn, self._parse_parameter(vn, value, parameter_type))
			return self._handle_dict(vn, parameter_value, parse_value)

	def getBool(self, varexpr, suffix = None, default = unspecified): # needed for Matcher configuration
		return self._config.getBool(self._get_opt(varexpr, suffix), default, onChange = self._on_change)

	def show_changes(self):
		pass

	def _get_opt(self, varexpr, suffix = None):
		return self._map_varexpr_suffix2opt.get((varexpr, suffix), ('%s %s' % (varexpr, suffix or '')).replace('\'', ''))

	def _get_varexpr(self, vn):
		try:
			return self._map_vn2varexpr[vn.lower()]
		except Exception:
			raise ConfigError('Variable %s is undefined' % vn)

	def _handle_dict(self, vn, value, parse_value):
		if '=>' in value:
			if self.getBool(self._get_varexpr(vn), 'parse dict', True):
				return self._parse_dict(vn, value, parse_value)
		return parse_value(value)

	def _on_change(self, config, old_obj, cur_obj, cur_entry, obj2str):
		self._changes.append((old_obj, cur_obj, cur_entry, obj2str))
		return cur_obj

	def _parse_dict(self, vn, dict_str, value_parser):
		keytuple_delimeter = self.get(self._get_varexpr(vn), 'key delimeter', ',')
		return parse_dict(dict_str, value_parser, lambda k: parse_tuple(k, keytuple_delimeter))

	def _parse_parameter(self, vn, value, parameter_type):
		if parameter_type == 'verbatim':
			return [value]
		elif parameter_type == 'split':
			delimeter = self.get(self._get_varexpr(vn), 'delimeter', ',')
			return lmap(str.strip, value.split(delimeter))
		elif parameter_type == 'lines':
			return value.splitlines()
		elif parameter_type in ('expr', 'eval'):
			result = eval(value) # pylint:disable=eval-used
			if isinstance(result, (list, type(range(1)))):
				return list(result)
			return [result]
		elif parameter_type == 'default':
			return shlex.split(value)
		elif parameter_type == 'format':
			fsource = self.get(self._get_varexpr(vn), 'source')
			fdefault = self.get(self._get_varexpr(vn), 'default', '')
			return (parameter_type, vn, value, fsource, fdefault) # special format!
		raise ConfigError('[Variable: %s] Invalid parameter type: %s' % (vn, parameter_type))

	def _parse_parameter_tuple(self, vn, outer_value, outer_type, inner_type, outer_idx):
		if outer_type == 'tuple': # eg. '(A|11) (B|12) (C|13)' -> [('A', 11), ('B', 12), ('C', 13)] -> [11, 12, 13]
			tuple_delimeter = self.get(self._get_varexpr(vn), 'delimeter', ',')
			tuple_token_list = lmap(str.strip, split_advanced(outer_value, lambda tok: tok in ' \n', lambda tok: False))
			tuple_list = lmap(lambda tuple_token: parse_tuple(tuple_token, tuple_delimeter), tuple_token_list)
		elif outer_type == 'binning': # eg. '11 12 13 14' -> [(11, 12), (12, 13), (13, 14)] -> [12, 13, 14]
			tuple_token_list = outer_value.split()
			tuple_list = lzip(tuple_token_list, tuple_token_list[1:])
		else:
			raise ConfigError('[Variable: %s] Invalid tuple type: %s' % (vn, outer_type))

		result = []
		for tuple_entry in tuple_list:
			try:
				tmp = self._parse_parameter(vn, tuple_entry[outer_idx], inner_type)
			except Exception:
				raise ConfigError('[Variable: %s] Unable to parse %r' % (vn, (tuple_entry, tuple_token_list)))
			if isinstance(tmp, list):
				if len(tmp) != 1:
					raise ConfigError('[Variable: %s] Tuple entry (%s) expands to multiple variable entries (%s)!' % (vn, tuple_entry[outer_idx], tmp))
				result.append(tmp[0])
			else:
				result.append(tmp)
		return result

	def _process_parameter_list(self, vn, values): # ensure common parameter format and apply repeat settings
		if isinstance(values, tuple): # special case - eg. used for type 'format'
			return values # this is not a list of parameter values, but a set of parameter settings!
		result = list(values)
		for idx, value in enumerate(values):
			value_repeat = int(self.get(vn, 'repeat idx %d' % idx, '1'))
			assert(value_repeat >= 0)
			if value_repeat > 1:
				result.extend((value_repeat - 1) * [value])
		parameter_repeat = int(self.get(vn, 'repeat', '1'))
		return parameter_repeat * result
