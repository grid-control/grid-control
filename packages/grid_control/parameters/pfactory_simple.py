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

from grid_control import utils
from grid_control.config import ConfigError
from grid_control.parameters.pfactory_base import BasicParameterFactory
from grid_control.parameters.psource_data import DataParameterSource
from grid_control.parameters.psource_file import CSVParameterSource
from grid_control.parameters.psource_lookup import createLookupHelper
from grid_control.parameters.psource_meta import ChainParameterSource, CrossParameterSource, RepeatParameterSource, ZipLongParameterSource
from grid_control.utils.gc_itertools import lchain
from hpfwk import APIError
from python_compat import ifilter, imap, irange, lfilter, next, reduce

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
		if lastTokenExpr and token not in (['[', ']', ')', '>'] + operatorList):
			yield '*'
		yield token
		lastTokenExpr = token not in (['[', '(', '<'] + operatorList)
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


class SimpleParameterFactory(BasicParameterFactory):
	def __init__(self, config, name):
		BasicParameterFactory.__init__(self, config, name)
		self._pExpr = self._paramConfig.get('parameters', None, '')
		self.elevatedSwitch = [] # Switch statements are elevated to global scope
		self.precedence = {'*': [], '+': ['*'], ',': ['*', '+']}


	def combineSources(self, PSourceClass, args):
		repeat = reduce(lambda a, b: a * b, ifilter(lambda expr: isinstance(expr, int), args), 1)
		args = lfilter(lambda expr: not isinstance(expr, int), args)
		if len(args) > 1:
			result = PSourceClass(*args)
		elif len(args) > 0:
			result = args[0]
		else:
			return utils.QM(repeat > 1, [repeat], [])
		if repeat > 1:
			return [RepeatParameterSource(result, repeat)]
		return [result]


	def _createVarSource(self, var_list, lookup_list): # create variable source
		psource_list = []
		for (doElevate, PSourceClass, args) in createLookupHelper(self._paramConfig, var_list, lookup_list):
			if doElevate: # switch needs elevation beyond local scope
				self.elevatedSwitch.append((PSourceClass, args))
			else:
				psource_list.append(PSourceClass(*args))
		# Optimize away unnecessary cross operations
		if len(lfilter(lambda p: p.getMaxParameters() is not None, psource_list)) <= 1:
			return psource_list # simply forward list of psources
		return [CrossParameterSource(*psource_list)]


	def tree2expr(self, node):
		if isinstance(node, tuple):
			(operator, args) = node
			if operator == 'lookup':
				assert(len(args) == 2)
				return self._createVarSource(tree2names(args[0]), tree2names(args[1]))
			elif operator == 'ref':
				assert(len(args) == 1)
				refTypeDefault = 'dataset'
				if args[0] not in DataParameterSource.datasetsAvailable:
					refTypeDefault = 'csv'
				refType = self._paramConfig.get(args[0], 'type', refTypeDefault)
				if refType == 'dataset':
					return [DataParameterSource.create(self._paramConfig, args[0])]
				elif refType == 'csv':
					return [CSVParameterSource.create(self._paramConfig, args[0])]
				raise APIError('Unknown reference type: "%s"' % refType)
			else:
				args_complete = lchain(imap(self.tree2expr, args))
				if operator == '*':
					return self.combineSources(CrossParameterSource, args_complete)
				elif operator == '+':
					return self.combineSources(ChainParameterSource, args_complete)
				elif operator == ',':
					return self.combineSources(ZipLongParameterSource, args_complete)
				raise APIError('Unknown token: "%s"' % operator)
		elif isinstance(node, int):
			return [node]
		else:
			return self._createVarSource([node], None)


	def _getUserSource(self, pExpr, parent):
		tokens = tokenize(pExpr, lchain([self.precedence.keys(), list('()[]<>')]))
		tokens = list(tok2inlinetok(tokens, list(self.precedence.keys())))
		utils.vprint('Parsing parameter string: "%s"' % str.join(' ', imap(str, tokens)), 0)
		tree = tok2tree(tokens, self.precedence)

		source_list = self.tree2expr(tree)
		if DataParameterSource.datasetsAvailable and not DataParameterSource.datasetsUsed:
			source_list.insert(0, DataParameterSource.create())
		if parent:
			source_list.append(parent)
		if len(lfilter(lambda p: p.getMaxParameters() is not None, source_list)) > 1:
			source = self.combineSources(CrossParameterSource, source_list)
		else:
			source = self.combineSources(ZipLongParameterSource, source_list) # zip more efficient
		assert(len(source) == 1)
		source = source[0]
		for (PSourceClass, args) in self.elevatedSwitch:
			source = PSourceClass(source, *args)
		utils.vprint('Parsing output: %r' % source, 0)
		return source


	def _getRawSource(self, parent):
		if self._pExpr:
			parent = self._getUserSource(self._pExpr, parent)
		return BasicParameterFactory._getRawSource(self, parent)
