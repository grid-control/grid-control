# | Copyright 2013-2017 Karlsruhe Institute of Technology
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

import logging
from grid_control.parameters.config_param import is_valid_parameter_char
from grid_control.parameters.pfactory_base import ParameterError, UserParameterFactory
from grid_control.parameters.psource_base import ParameterSource
from hpfwk import APIError
from python_compat import imap, lmap, next


class SimpleParameterFactory(UserParameterFactory):
	alias_list = ['simple']

	def __init__(self, config):
		UserParameterFactory.__init__(self, config)
		self._operator_map_raw = {  # Mapping of operators - using raw arguments
			'<>': 'InternalReferenceParameterSource',
			'{}': 'SubSpaceParameterSource',
		}
		self._operator_map_eval = {  # Mapping of operators - using evaluated arguments
			'*': 'CrossParameterSource',
			'+': 'ChainParameterSource',
			',': 'ZipLongParameterSource'
		}

	def _get_psrc_user(self, pexpr, repository):
		# Split "A B <data>" into ['A', 'B', '<data>']
		token_list = list(_str2token_list(pexpr, list('*+,()[]<>{}')))
		self._log.debug('Parsing parameter string: "%s"', str.join(' ', imap(str, token_list)))
		# Apply operator precendece and resolve
		# ['A', 'B', '<data>'] into ('*', ['A', 'B', ('ref', ['data'])])
		tree = _token_list2token_tree(token_list)
		self._log.log(logging.DEBUG2, 'Parsed token tree: %r', tree)
		# Translate token tree into expression
		# ('*', ['A', 'B', ('ref', ['data'])]) into cross(var('A'), var('B'), data())
		return self._tree2expr(tree, repository)

	def _tree2expr(self, node, repository):
		if isinstance(node, int):
			return node
		elif isinstance(node, tuple):
			(operator, args) = node
			if operator == '[]':
				psrc_list = []
				for output_vn in _tree2names(args[0]):
					psrc_list.append(ParameterSource.create_psrc_safe('InternalAutoParameterSource',
						self._parameter_config, repository, output_vn, _tree2names(args[1])))
				return ParameterSource.create_psrc_safe('CrossParameterSource',
					self._parameter_config, repository, *psrc_list)
			elif operator in self._operator_map_raw:
				return ParameterSource.create_psrc_safe(self._operator_map_raw[operator],
					self._parameter_config, repository, *args)
			elif operator in self._operator_map_eval:
				evaluated_args = lmap(lambda node: self._tree2expr(node, repository), args)
				return ParameterSource.create_psrc_safe(self._operator_map_eval[operator],
					self._parameter_config, repository, *evaluated_args)
		else:
			return ParameterSource.create_psrc_safe('InternalAutoParameterSource',
				self._parameter_config, repository, node)
		raise APIError('Unable to parse node %s!' % repr(node))


def _eval_operators(resolve_list, token_stack, operator_stack):
	while operator_stack and (operator_stack[-1] in resolve_list):
		expr_2 = token_stack.pop()
		expr_1 = token_stack.pop()
		token_stack.append((operator_stack.pop(), [expr_1, expr_2]))


def _str2token_list(value, token_list):
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


def _token_list2token_tree(value):
	token_list = list(value)
	error_template = str.join('', imap(str, token_list))
	token_iter = iter(token_list)
	token = next(token_iter, None)
	token_stack = []
	operator_stack = []
	add_operator = False

	def _collect_nested_tokens(token_iter, left, right, error_msg):
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
			raise ParameterError(error_msg)

	while token:
		if add_operator and (token not in ['*', '+', ',', '[']):
			operator_stack.append('*')
		if token == '(':  # replace tokens between < > with evaluated tree
			tmp = list(_collect_nested_tokens(token_iter, '(', ')', 'Parenthesis error: ' + error_template))
			token_stack.append(_token_list2token_tree(tmp))
		elif token == '<':  # forward raw tokens between < >
			tmp = list(_collect_nested_tokens(token_iter, '<', '>', 'Parenthesis error: ' + error_template))
			token_stack.append(('<>', tmp))
		elif token == '{':  # forward raw tokens between { }
			tmp = list(_collect_nested_tokens(token_iter, '{', '}', 'Parenthesis error: ' + error_template))
			token_stack.append(('{}', tmp))
		elif token == '[':  # pack token_tree in front of [] and token within [] together
			tmp = list(_collect_nested_tokens(token_iter, '[', ']', 'Parenthesis error: ' + error_template))
			token_stack.append(('[]', [token_stack.pop(), _token_list2token_tree(tmp)]))
		elif token == ',':
			_eval_operators('*+', token_stack, operator_stack)
			operator_stack.append(token)
		elif token == '+':
			_eval_operators('*', token_stack, operator_stack)
			operator_stack.append(token)
		elif token == '*':
			operator_stack.append(token)
		else:
			token_stack.append(token)
		add_operator = (token not in ['*', '+', ','])
		token = next(token_iter, None)
	_eval_operators('*+,', token_stack, operator_stack)

	if len(token_stack) != 1:
		raise ParameterError('Invalid stack state detected: %r %r' % (token_stack, operator_stack))
	return token_stack[0]


def _tree2names(node):  # return list of referenced variable names in tree
	if isinstance(node, tuple):
		result = []
		for op_args in node[1:]:
			for arg in op_args:
				result.extend(_tree2names(arg))
		return result
	else:
		return [node]
