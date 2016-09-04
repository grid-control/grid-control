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
from grid_control.parameters.psource_lookup import createLookupHelper
from hpfwk import APIError
from python_compat import ifilter, imap, irange, lchain, lfilter, lmap, next, reduce

def tokenize(value, tokList):
	(pos, start) = (0, 0)
	while pos < len(value):
		start = pos
		if (value[pos] == '!') or value[pos].isalpha():
			pos += 1
			validChar = lambda c: c.isalnum() or (c in ['_'])
			while (pos < len(value)) and validChar(value[pos]):
				pos += 1
			yield value[start:pos]
			continue
		if value[pos].isdigit():
			while (pos < len(value)) and value[pos].isdigit():
				pos += 1
			yield int(value[start:pos])
			continue
		if value[pos] in tokList:
			yield value[pos]
		pos += 1


def tok2inlinetok(tokens, operatorList):
	token = next(tokens, None)
	lastTokenExpr = None
	while token:
		# insert '*' between two expressions - but not between "<expr> ["
		if lastTokenExpr and token not in (['[', ']', ')', '>', '}'] + operatorList):
			yield '*'
		yield token
		lastTokenExpr = token not in (['[', '(', '<', '{'] + operatorList)
		token = next(tokens, None)


def clearOPStack(opList, opStack, tokStack):
	while len(opStack) and (opStack[-1][0] in opList):
		operator = opStack.pop()
		tmp = []
		for dummy in irange(len(operator) + 1):
			tmp.append(tokStack.pop())
		tmp.reverse()
		tokStack.append((operator[0], tmp))


def tok2tree(value, precedence):
	value = list(value)
	errorStr = str.join('', imap(str, value))
	tokens = iter(value)
	token = next(tokens, None)
	tokStack = []
	opStack = []

	def collectNestedTokens(tokens, left, right, errMsg):
		level = 1
		token = next(tokens, None)
		while token:
			if token == left:
				level += 1
			elif token == right:
				level -= 1
				if level == 0:
					break
			yield token
			token = next(tokens, None)
		if level != 0:
			raise ConfigError(errMsg)

	while token:
		if token == '(':
			tmp = list(collectNestedTokens(tokens, '(', ')', "Parenthesis error: " + errorStr))
			tokStack.append(tok2tree(tmp, precedence))
		elif token == '<':
			tmp = list(collectNestedTokens(tokens, '<', '>', "Parenthesis error: " + errorStr))
			tokStack.append(('ref', tmp))
		elif token == '[':
			tmp = list(collectNestedTokens(tokens, '[', ']', "Parenthesis error: " + errorStr))
			tokStack.append(('lookup', [tokStack.pop(), tok2tree(tmp, precedence)]))
		elif token == '{':
			tmp = list(collectNestedTokens(tokens, '{', '}', "Parenthesis error: " + errorStr))
			tokStack.append(('pspace', tmp))
		elif token in precedence:
			clearOPStack(precedence[token], opStack, tokStack)
			if opStack and opStack[-1].startswith(token):
				opStack[-1] = opStack[-1] + token
			else:
				opStack.append(token)
		else:
			tokStack.append(token)
		token = next(tokens, None)

	clearOPStack(precedence.keys(), opStack, tokStack)
	assert(len(tokStack) == 1)
	return tokStack[0]


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

	def __init__(self, config, repository):
		UserParameterFactory.__init__(self, config, repository)
		self._nestedSources = [] # Switch statements are elevated to global scope
		self._precedence = {'*': [], '+': ['*'], ',': ['*', '+']}


	def _combineSources(self, clsName, args):
		repeat = reduce(lambda a, b: a * b, ifilter(lambda expr: isinstance(expr, int), args), 1)
		args = lfilter(lambda expr: not isinstance(expr, int), args)
		if args:
			result = ParameterSource.createInstance(clsName, *args)
			if repeat > 1:
				return ParameterSource.createInstance('RepeatParameterSource', result, repeat)
			return result
		elif repeat > 1:
			return repeat
		return NullParameterSource()


	def _createVarSource(self, var_list, lookup_list): # create variable source
		psource_list = []
		for (doElevate, PSourceClass, args) in createLookupHelper(self._paramConfig, var_list, lookup_list):
			if doElevate: # switch needs elevation beyond local scope
				self._nestedSources.append((PSourceClass, args))
			else:
				psource_list.append(PSourceClass(*args))
		# Optimize away unnecessary cross operations
		return ParameterSource.createInstance('CrossParameterSource', *psource_list)


	def _createRef(self, arg):
		refTypeDefault = 'dataset'
		DataParameterSource = ParameterSource.getClass('DataParameterSource')
		if 'dataset:' + arg not in self._repository:
			refTypeDefault = 'csv'
		refType = self._paramConfig.get(arg, 'type', refTypeDefault)
		if refType == 'dataset':
			return DataParameterSource.create(self._paramConfig, self._repository, arg)
		elif refType == 'csv':
			return ParameterSource.getClass('CSVParameterSource').create(self._paramConfig, self._repository, arg)
		raise APIError('Unknown reference type: "%s"' % refType)


	def _createPSpace(self, args):
		SubSpaceParameterSource = ParameterSource.getClass('SubSpaceParameterSource')
		if len(args) == 1:
			return SubSpaceParameterSource.create(self._paramConfig, self._repository, args[0])
		elif len(args) == 3:
			return SubSpaceParameterSource.create(self._paramConfig, self._repository, args[2], args[0])
		else:
			raise APIError('Invalid subspace reference!: %r' % args)


	def _tree2expr(self, node):
		if isinstance(node, tuple):
			(operator, args) = node
			if operator == 'lookup':
				assert(len(args) == 2)
				return self._createVarSource(tree2names(args[0]), tree2names(args[1]))
			elif operator == 'ref':
				assert(len(args) == 1)
				return self._createRef(args[0])
			elif operator == 'pspace':
				return self._createPSpace(args)
			else:
				args_complete = lmap(self._tree2expr, args)
				if operator == '*':
					return self._combineSources('CrossParameterSource', args_complete)
				elif operator == '+':
					return self._combineSources('ChainParameterSource', args_complete)
				elif operator == ',':
					return self._combineSources('ZipLongParameterSource', args_complete)
				raise APIError('Unknown token: "%s"' % operator)
		elif isinstance(node, int):
			return node
		else:
			return self._createVarSource([node], None)


	def _getUserSource(self, pExpr):
		tokens = tokenize(pExpr, lchain([self._precedence.keys(), list('()[]<>{}')]))
		tokens = list(tok2inlinetok(tokens, list(self._precedence.keys())))
		self._log.debug('Parsing parameter string: "%s"', str.join(' ', imap(str, tokens)))
		tree = tok2tree(tokens, self._precedence)
		source = self._tree2expr(tree)
		for (PSourceClass, args) in self._nestedSources:
			source = PSourceClass.createInstance(PSourceClass.__name__, source, *args)
		return source
