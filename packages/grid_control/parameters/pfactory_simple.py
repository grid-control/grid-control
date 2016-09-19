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
from grid_control.parameters.pfactory_base import UserParameterFactory
from grid_control.parameters.psource_base import NullParameterSource, ParameterSource
from grid_control.parameters.psource_lookup import parse_lookup_factory_args
from hpfwk import APIError
from python_compat import ifilter, imap, irange, lchain, lfilter, lmap, next, reduce

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
	assert(len(token_stack) == 1)
	return token_stack[0]


def tokenize(value, token_list):
	(pos, start) = (0, 0)
	while pos < len(value):
		start = pos
		if (value[pos] == '!') or value[pos].isalpha():
			pos += 1
			valid_char = lambda c: c.isalnum() or (c in ['_'])
			while (pos < len(value)) and valid_char(value[pos]):
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


def tree2names(node): # return list of referenced variable names in tree
	if isinstance(node, tuple):
		result = []
		for op_args in node[1:]:
			for arg in op_args:
				result.extend(tree2names(arg))
		return result
	else:
		return [node]


class SimpleParameterFactory(UserParameterFactory):
	alias = ['simple']

	def __init__(self, config):
		UserParameterFactory.__init__(self, config)
		self._psrc_list_nested = [] # Switch statements are elevated to global scope
		self._precedence = {'*': [], '+': ['*'], ',': ['*', '+']}

	def _combine_psrc_list(self, cls_name, args):
		repeat = reduce(lambda a, b: a * b, ifilter(lambda expr: isinstance(expr, int), args), 1)
		args = lfilter(lambda expr: not isinstance(expr, int), args)
		if args:
			result = ParameterSource.create_instance(cls_name, *args)
			if repeat > 1:
				return ParameterSource.create_instance('RepeatParameterSource', result, repeat)
			return result
		elif repeat > 1:
			return repeat
		return NullParameterSource()

	def _create_psrc_pspace(self, args, repository):
		SubSpaceParameterSource = ParameterSource.getClass('SubSpaceParameterSource')
		if len(args) == 1:
			return SubSpaceParameterSource.create_psrc(self._parameter_config, repository, args[0])
		elif len(args) == 3:
			return SubSpaceParameterSource.create_psrc(self._parameter_config, repository, args[2], args[0])
		else:
			raise APIError('Invalid subspace reference!: %r' % args)

	def _create_psrc_ref(self, arg, repository):
		ref_type_default = 'dataset'
		DataParameterSource = ParameterSource.getClass('DataParameterSource')
		if 'dataset:' + arg not in repository:
			ref_type_default = 'csv'
		ref_type = self._parameter_config.get(arg, 'type', ref_type_default)
		if ref_type == 'dataset':
			return DataParameterSource.create_psrc(self._parameter_config, repository, arg)
		elif ref_type == 'csv':
			return ParameterSource.getClass('CSVParameterSource').create_psrc(self._parameter_config, repository, arg)
		raise APIError('Unknown reference type: "%s"' % ref_type)

	def _create_psrc_var(self, var_list, lookup_list): # create variable source
		psource_list = []
		for (is_nested, PSourceClass, args) in parse_lookup_factory_args(self._parameter_config, var_list, lookup_list):
			if is_nested: # switch needs elevation beyond local scope
				self._psrc_list_nested.append((PSourceClass, args))
			else:
				psource_list.append(PSourceClass(*args))
		# Optimize away unnecessary cross operations
		return ParameterSource.create_instance('CrossParameterSource', *psource_list)

	def _get_source_user(self, pexpr, repository):
		token_iter = tokenize(pexpr, lchain([self._precedence.keys(), list('()[]<>{}')]))
		token_list = list(tok2inlinetok(token_iter, list(self._precedence.keys())))
		self._log.debug('Parsing parameter string: "%s"', str.join(' ', imap(str, token_list)))
		tree = tok2tree(token_list, self._precedence)
		source = self._tree2expr(tree, repository)
		for (PSourceClass, args) in self._psrc_list_nested:
			source = PSourceClass.create_instance(PSourceClass.__name__, source, *args)
		return source

	def _tree2expr(self, node, repository):
		if isinstance(node, tuple):
			(operator, args) = node
			if operator == 'lookup':
				assert(len(args) == 2)
				return self._create_psrc_var(tree2names(args[0]), tree2names(args[1]))
			elif operator == 'ref':
				assert(len(args) == 1)
				return self._create_psrc_ref(args[0], repository)
			elif operator == 'pspace':
				return self._create_psrc_pspace(args, repository)
			else:
				args_complete = lmap(lambda node: self._tree2expr(node, repository), args)
				if operator == '*':
					return self._combine_psrc_list('CrossParameterSource', args_complete)
				elif operator == '+':
					return self._combine_psrc_list('ChainParameterSource', args_complete)
				elif operator == ',':
					return self._combine_psrc_list('ZipLongParameterSource', args_complete)
				raise APIError('Unknown token: "%s"' % operator)
		elif isinstance(node, int):
			return node
		else:
			return self._create_psrc_var([node], None)
