# | Copyright 2012-2017 Karlsruhe Institute of Technology
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

import os, shlex
from grid_control.config import ConfigError
from grid_control.utils import clean_path
from grid_control.utils.algos import accumulate
from grid_control.utils.parsing import parse_dict_cfg, split_advanced, split_brackets
from grid_control.utils.process_base import LocalProcess
from hpfwk import AbstractError, Plugin, rethrow
from python_compat import imap, irange, lmap, lzip, unspecified


def frange(start, end=None, num=None, steps=None, format='%g'):
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


def is_valid_parameter_char(value):
	return value.isalnum() or (value in ['_'])


def parse_parameter_option(option):
	# first token is variable / tuple - rest is option specifier: "a option" or "(a,b) option"
	tokens = list(split_brackets(option.lower()))
	if len(tokens) and '(' in tokens[0]:
		# parse tuple in as general way as possible
		result = [tuple(accumulate(tokens[0], '',
			do_emit=lambda i, b: not is_valid_parameter_char(i),
			do_add=lambda i, b: is_valid_parameter_char(i)))]
		if tokens[1:]:
			result.append(str.join('', tokens[1:]).strip())
	else:
		result = str.join('', tokens).strip().split(' ', 1)
	if len(result) == 1:
		result.append(None)
	return tuple(result)


def parse_tuple(token, delimeter):
	token = token.strip()
	if token.startswith('('):
		return tuple(imap(str.strip, split_advanced(token[1:-1],
			do_emit=lambda tok: tok == delimeter,
			add_emit_token=lambda tok: False)))
	return (token,)


class ParameterConfig(object):
	def __init__(self, config):
		(self._config, self._changes) = (config, [])
		option_list = config.get_option_list()
		(self._map_vn2varexpr, self._map_varexpr_suffix2opt) = _parse_parameter_option_list(option_list)

	def get(self, varexpr, suffix=None, default=unspecified, **kwargs):
		return self._config.get(self._get_var_opt(varexpr, suffix), default,
			on_change=self._on_change, **kwargs)

	def get_bool(self, varexpr, suffix=None, default=unspecified):  # needed for Matcher configuration
		return self._config.get_bool(self._get_var_opt(varexpr, suffix), default,
			on_change=self._on_change)

	def get_config(self, *args, **kwargs):
		return self._config.change_view(*args, **kwargs)

	def get_parameter(self, vn):
		varexpr = self._get_varexpr(vn)

		if isinstance(varexpr, tuple):
			outer_idx = list(varexpr).index(vn.lower())
			outer_value = self.get(varexpr, None, '')
			outer_type = self.get(varexpr, 'type', 'default')
			inner_type = self.get(vn, 'type', 'verbatim')

			def _parse_value(value):  # extract the outer_idx-nth variable and parse it as usual
				return self._process_parameter_list(vn,
					self._parse_parameter_tuple(vn, value, outer_type, inner_type, outer_idx))
			return self._handle_dict(vn, outer_value, _parse_value)
		else:
			parameter_value = self.get(vn, None, '')
			parameter_type = self.get(vn, 'type', 'default')

			def _parse_value(value):  # parse the parameter value - using the specified interpretation
				return self._process_parameter_list(vn,
					self._parse_parameter(vn, value, parameter_type))
			return self._handle_dict(vn, parameter_value, _parse_value)

	def _get_var_opt(self, varexpr, suffix=None):
		if isinstance(varexpr, list):
			varexpr = varexpr[-1]
		opt_default = ('%s %s' % (varexpr, suffix or '')).replace('\'', '')
		return self._map_varexpr_suffix2opt.get((varexpr, suffix), opt_default)

	def _get_varexpr(self, vn):
		return _var_rethrow(vn, 'Undefined variable', self._map_vn2varexpr.__getitem__, vn.lower())

	def _handle_dict(self, vn, value, parse_value):
		if '=>' in value:
			if self.get_bool(self._get_varexpr(vn), 'parse dict', True):
				return self._parse_dict(vn, value, parse_value)
		return parse_value(value)

	def _on_change(self, config, old_obj, cur_obj, cur_entry, obj2str):
		self._changes.append((old_obj, cur_obj, cur_entry, obj2str))
		return cur_obj

	def _parse_dict(self, vn, dict_str, value_parser):
		keytuple_delimeter = self.get(self._get_varexpr(vn), 'key delimeter', ',')
		return parse_dict_cfg(dict_str, value_parser, lambda k: parse_tuple(k, keytuple_delimeter))

	def _parse_parameter(self, vn, value, parameter_type):
		parameter_parser = _var_rethrow(vn, 'Invalid parameter type: %s' % parameter_type,
			ParameterParser.create_instance, parameter_type)
		return _var_rethrow(vn, 'Invalid parameter value: %s (type: %s)' % (value, parameter_type),
			parameter_parser.parse_value, self, self._get_varexpr(vn), vn, value)

	def _parse_parameter_tuple(self, vn, outer_value, outer_type, inner_type, outer_idx):
		tuple_parser = _var_rethrow(vn, 'Invalid tuple type: %s' % outer_type,
			ParameterTupleParser.create_instance, outer_type)
		tuple_list = _var_rethrow(vn, 'Invalid tuple value: %s (type: %s)' % (outer_value, outer_type),
			tuple_parser.parse_tuples, self, self._get_varexpr(vn), vn, outer_value)
		result = []
		for tuple_entry in tuple_list:
			value = _var_rethrow(vn, 'Unable to access parameter index %d: %r' % (outer_idx, tuple_entry),
				tuple_entry.__getitem__, outer_idx)
			tmp = _var_rethrow(vn, 'Unable to parse %s' % repr(tuple_entry),
				self._parse_parameter, vn, value, inner_type)
			if isinstance(tmp, list):
				if len(tmp) != 1:
					raise VarError(vn, 'Tuple entry (%s) expands to multiple variable entries (%s)!' % (
						tuple_entry[outer_idx], tmp))
				result.append(tmp[0])
			else:
				result.append(tmp)
		return result

	def _process_parameter_list(self, vn, values):
		# ensure common parameter format and apply repeat settings
		if isinstance(values, tuple):  # special case - eg. used for type 'format'
			return values  # constructor arguments for ParameterSource
		result = list(values)
		for idx, value in enumerate(values):
			value_repeat = int(self.get(vn, 'repeat idx %d' % idx, '1'))
			if value_repeat < 0:
				raise ConfigError('Invalid parameter repeat index: %r' % value_repeat)
			if value_repeat > 1:
				result.extend((value_repeat - 1) * [value])
		parameter_repeat = int(self.get(vn, 'repeat', '1'))
		return parameter_repeat * result


class VarError(ConfigError):
	def __init__(self, vn, msg):
		ConfigError.__init__(self, '[Variable: %s] %s' % (vn, msg))


class ParameterParser(Plugin):
	def parse_value(self, pconfig, varexpr, vn, value):
		raise AbstractError


class ParameterTupleParser(Plugin):
	def parse_tuples(self, pconfig, varexpr, vn, value):
		raise AbstractError


class ExprParameterParser(ParameterParser):
	alias_list = ['expr', 'eval']

	def parse_value(self, pconfig, varexpr, vn, value):
		result = eval(value)  # pylint:disable=eval-used
		if isinstance(result, (list, type(irange(1)))):
			return list(result)
		return [result]


class FormatParameterParser(ParameterParser):
	alias_list = ['format']

	def parse_value(self, pconfig, varexpr, vn, value):
		source = pconfig.get(varexpr, 'source')
		default = pconfig.get(varexpr, 'default', '')
		return ('FormatterParameterSource', vn, value, source, default)  # class init tuple


class GitParameterParser(ParameterParser):
	alias_list = ['git']

	def parse_value(self, pconfig, varexpr, vn, value):
		version = pconfig.get(vn + ' version', default=self._get_version(value), persistent=True)
		return [version]

	def _get_version(self, value):
		old_wd = os.getcwd()
		os.chdir(clean_path(value))
		git_proc = LocalProcess('git', 'rev-parse', '--short', 'HEAD')
		version = git_proc.get_output(timeout=10, raise_errors=False)
		os.chdir(old_wd)
		return version.strip() or 'undefined'


class LinesParameterParser(ParameterParser):
	alias_list = ['lines']

	def parse_value(self, pconfig, varexpr, vn, value):
		return value.splitlines()


class RegexTransformParameterParser(ParameterParser):
	alias_list = ['regex_transform']

	def parse_value(self, pconfig, varexpr, vn, value):
		default = pconfig.get(varexpr, 'default', '')
		setup_dict = parse_dict_cfg(pconfig.get(varexpr, 'transform', ''))
		return ('RegexTransformParameterSource', vn, value,
			setup_dict[0], setup_dict[1], default)  # class init tuple


class ShellParameterParser(ParameterParser):
	alias_list = ['shell', 'default']

	def parse_value(self, pconfig, varexpr, vn, value):
		return shlex.split(value)


class SplitParameterParser(ParameterParser):
	alias_list = ['split']

	def parse_value(self, pconfig, varexpr, vn, value):
		delimeter = pconfig.get(varexpr, 'delimeter', ',')
		return lmap(str.strip, value.split(delimeter))


class SvnParameterParser(ParameterParser):
	alias_list = ['svn']

	def parse_value(self, pconfig, varexpr, vn, value):
		version = pconfig.get(vn + ' version', default=self._get_version(value), persistent=True)
		return [version]

	def _get_version(self, value):
		svn_proc = LocalProcess('svnversion', clean_path(value))
		version = svn_proc.get_output(timeout=10, raise_errors=False).strip().lower()
		# different SVN versions yield different output for unversioned directories:
		if ('exported' in version) or ('unversioned' in version):
			version = None
		return version or 'undefined'


class TransformParameterParser(ParameterParser):
	alias_list = ['transform']

	def parse_value(self, pconfig, varexpr, vn, value):
		default = pconfig.get(varexpr, 'default', '')
		return ('TransformParameterSource', vn, value, default)  # class init tuple


class VerbatimParameterParser(ParameterParser):
	alias_list = ['verbatim']

	def parse_value(self, pconfig, varexpr, vn, value):
		return [value]


class BinningTupleParser(ParameterTupleParser):
	alias_list = ['binning']

	def parse_tuples(self, pconfig, varexpr, vn, value):
		# eg. '11 12 13 14' -> [(11, 12), (12, 13), (13, 14)] -> [12, 13, 14]
		tuple_token_list = value.split()
		return lzip(tuple_token_list, tuple_token_list[1:])


class DefaultTupleParser(ParameterTupleParser):
	alias_list = ['tuple', 'default']

	def parse_tuples(self, pconfig, varexpr, vn, value):
		# eg. '(A|11) (B|12) (C|13)' -> [('A', 11), ('B', 12), ('C', 13)] -> [11, 12, 13]
		tuple_delimeter = pconfig.get(varexpr, 'delimeter', ',')
		tuple_token_list = lmap(str.strip, split_advanced(value,
			do_emit=lambda tok: tok in ' \n', add_emit_token=lambda tok: False))
		return lmap(lambda tuple_str: parse_tuple(tuple_str, tuple_delimeter), tuple_token_list)


def _parse_parameter_option_list(option_list):
	(map_vn2varexpr, map_varexpr_suffix2opt) = ({}, {})
	for opt in option_list:
		(varexpr, suffix) = parse_parameter_option(opt)
		map_varexpr_suffix2opt[(varexpr, suffix)] = opt
		if suffix is None:
			if isinstance(varexpr, tuple):
				for vn in varexpr:
					map_vn2varexpr[vn] = varexpr
			else:
				map_vn2varexpr[varexpr] = varexpr
	return (map_vn2varexpr, map_varexpr_suffix2opt)


def _var_rethrow(vn, msg, fun, *args, **kwargs):
	return rethrow(VarError(vn, msg), fun, *args, **kwargs)
