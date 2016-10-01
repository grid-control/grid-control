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

from grid_control.config import ConfigError
from grid_control.parameters.config_param import is_valid_parameter_char
from grid_control.parameters.pfactory_base import ParameterError, UserParameterFactory
from grid_control.parameters.psource_base import NullParameterSource, ParameterSource
from grid_control.parameters.psource_lookup import parse_lookup_factory_args
from hpfwk import APIError
from python_compat import imap, irange, lchain, lfilter, lmap, next, reduce


def clear_operator_stack(operator_list, operator_stack, token_stack):
	while len(operator_stack) and (operator_stack[-1][0] in operator_list):
		operator = operator_stack.pop()
		tmp = []
		for dummy in irange(len(operator) + 1):
			tmp.append(token_stack.pop())
		tmp.reverse()
		token_stack.append((operator[0], tmp))


def tok2inlinetok(token_iter, operator_list):
	token = next(token_iter, None)
	prev_token_was_expr = None
	while token:
		# insert '*' between two expressions - but not between "<expr> ["
		if prev_token_was_expr and token not in (['[', ']', ')', '>', '}'] + operator_list):
			yield '*'
		yield token
		prev_token_was_expr = token not in (['[', '(', '<', '{'] + operator_list)
		token = next(token_iter, None)


def tok2tree(value, precedence):
	value = list(value)
	error_template = str.join('', imap(str, value))
	token_iter = iter(value)
	token = next(token_iter, None)
	token_stack = []
	operator_stack = []

	def collect_nested_tokens(token_iter, left, right, error_msg):
		level = 1
		token = next(token_iter, None)
		while token:
			if token == left:
				level += 1
			elif token == right:
				level -= 1
				if level == 0:
					break
			yield token
			token = next(token_iter, None)
		if level != 0:
			raise ConfigError(error_msg)

	while token:
		if token == '(':
			tmp = list(collect_nested_tokens(token_iter, '(', ')', "Parenthesis error: " + error_template))
			token_stack.append(tok2tree(tmp, precedence))
		elif token == '<':
			tmp = list(collect_nested_tokens(token_iter, '<', '>', "Parenthesis error: " + error_template))
			token_stack.append(('ref', tmp))
		elif token == '[':
			tmp = list(collect_nested_tokens(token_iter, '[', ']', "Parenthesis error: " + error_template))
			token_stack.append(('lookup', [token_stack.pop(), tok2tree(tmp, precedence)]))
		elif token == '{':
			tmp = list(collect_nested_tokens(token_iter, '{', '}', "Parenthesis error: " + error_template))
			token_stack.append(('pspace', tmp))
		elif token in precedence:
			clear_operator_stack(precedence[token], operator_stack, token_stack)
			if operator_stack and operator_stack[-1].startswith(token):
				operator_stack[-1] = operator_stack[-1] + token
			else:
				operator_stack.append(token)
		else:
			token_stack.append(token)
		token = next(token_iter, None)

	clear_operator_stack(precedence.keys(), operator_stack, token_stack)
	if len(token_stack) != 1:
		raise Exception('Invalid stack state detected: %s' % repr(token_stack))
	return token_stack[0]


def tokenize(value, token_list):
	(pos, start) = (0, 0)
	while pos < len(value):
		start = pos
		if (value[pos] == '!') or value[pos].isalpha():
			pos += 1
			while (pos < len(value)) and is_valid_parameter_char(value[pos]):
				pos += 1
			yield value[start:pos]
			continue
		if value[pos].isdigit():
			while (pos < len(value)) and value[pos].isdigit():
				pos += 1
			yield int(value[start:pos])
			continue
		if value[pos] in token_list:
			yield value[pos]
		pos += 1


def tree2names(node):  # return list of referenced variable names in tree
	if isinstance(node, tuple):
		result = []
		for op_args in node[1:]:
			for arg in op_args:
				result.extend(tree2names(arg))
		return result
	else:
		return [node]


class SimpleParameterFactory(UserParameterFactory):
	alias_list = ['simple']

	def __init__(self, config):
		UserParameterFactory.__init__(self, config)
		self._psrc_list_nested = []  # Switch statements are elevated to global scope
		self._precedence = {'*': [], '+': ['*'], ',': ['*', '+']}

	def _create_psrc(self, psrc_name, repository, *args):
		psrc_type = ParameterSource.get_class(psrc_name)
		return psrc_type.create_psrc(self._parameter_config, repository, *args)

	def _create_psrc_meta(self, psrc_name, *args):
		number_list = lfilter(lambda expr: isinstance(expr, int), args)
		args = lfilter(lambda expr: not isinstance(expr, int), args)  # remove numbers from args
		repeat = reduce(lambda a, b: a * b, number_list, 1)
		if args:
			result = ParameterSource.create_instance(psrc_name, *args)
			if number_list:
				result = ParameterSource.create_instance('RepeatParameterSource', result, repeat)
			return result
		elif number_list:
			return repeat
		return NullParameterSource()

	def _create_psrc_pspace(self, args, repository):
		if len(args) == 1:
			return self._create_psrc('SubSpaceParameterSource', repository, args[0])
		elif len(args) == 3:
			return self._create_psrc('SubSpaceParameterSource', repository, args[2], args[0])
		else:
			raise APIError('Invalid subspace reference!: %r' % args)

	def _create_psrc_ref(self, arg, repository):
		ref_type_default = 'dataset'
		if 'dataset:' + arg not in repository:
			ref_type_default = 'csv'
		ref_type = self._parameter_config.get(arg, 'type', ref_type_default)
		if ref_type == 'dataset':
			return self._create_psrc('DataParameterSource', repository, arg)
		elif ref_type == 'csv':
			return self._create_psrc('CSVParameterSource', repository, arg)
		raise APIError('Unknown reference type: "%s"' % ref_type)

	def _create_psrc_var(self, var_list, lookup_list):  # create variable source
		psrc_list = []
		psrc_info_list = parse_lookup_factory_args(self._parameter_config, var_list, lookup_list)
		for (is_nested, psrc_type, args) in psrc_info_list:
			if is_nested:  # switch needs elevation beyond local scope
				self._psrc_list_nested.append((psrc_type, args))
			else:
				psrc_list.append(psrc_type(*args))
		# Optimize away unnecessary cross operations
		return ParameterSource.create_instance('CrossParameterSource', *psrc_list)

	def _get_psrc_user(self, pexpr, repository):
		token_iter = tokenize(pexpr, lchain([self._precedence.keys(), list('()[]<>{}')]))
		token_list = list(tok2inlinetok(token_iter, list(self._precedence.keys())))
		self._log.debug('Parsing parameter string: "%s"', str.join(' ', imap(str, token_list)))
		tree = tok2tree(token_list, self._precedence)
		psrc = self._tree2expr(tree, repository)
		for (psrc_type, args) in self._psrc_list_nested:
			psrc = psrc_type.create_instance(psrc_type.__name__, psrc, *args)
		return psrc

	def _tree2expr(self, node, repository):
		if isinstance(node, tuple):
			(operator, args) = node
			if operator == 'lookup':
				if len(args) != 2:
					raise ParameterError('Invalid arguments for lookup: %s' % repr(args))
				result = self._create_psrc_var(tree2names(args[0]), tree2names(args[1]))
			elif operator == 'ref':
				if len(args) != 1:
					raise ParameterError('Invalid arguments for reference: %s' % repr(args))
				result = self._create_psrc_ref(args[0], repository)
			elif operator == 'pspace':
				result = self._create_psrc_pspace(args, repository)
			else:
				args_complete = lmap(lambda node: self._tree2expr(node, repository), args)
				if operator == '*':
					return self._create_psrc_meta('CrossParameterSource', *args_complete)
				elif operator == '+':
					return self._create_psrc_meta('ChainParameterSource', *args_complete)
				elif operator == ',':
					return self._create_psrc_meta('ZipLongParameterSource', *args_complete)
				raise APIError('Unknown token: "%s"' % operator)
			return result
		elif isinstance(node, int):
			return node
		else:
			return self._create_psrc_var([node], None)
