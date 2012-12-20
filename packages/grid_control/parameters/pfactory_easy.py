from pfactory_modular import ModularParameterFactory
from grid_control import utils

class EasyParameterFactory(ModularParameterFactory):
	def _getUserSource(self, pExpr):
		if pExpr:
			modExpr = self.rewritePluginSetup(pExpr)
			utils.vprint('Using parameter expression:\n\t%s\n\t%s' % (pExpr, modExpr), 0)
			pExpr = modExpr
		return ModularParameterFactory._getUserSource(self, pExpr)


	def rewritePluginSetup(self, value):
		if isinstance(value, tuple):
			return value[0]
		result = []
		# Separate into tokens (type:tuple), operators (type:list), processed strings (type:str)
		pos = 0
		global tmp
		tmp = ''
		def dumpString():
			global tmp
			if tmp.strip():
				result.append(tmp.strip())
				tmp = ''
		while pos < len(value):
			if value[pos] == '(':
				dumpString()
				rpos = pos
				counter = 0
				while rpos < len(value):
					if value[rpos] == '(':
						counter += 1
					elif value[rpos] == ')':
						counter -= 1
					if counter == 0:
						result.append((value[pos:rpos + 1],))
						break
					rpos += 1
				pos = rpos
			elif value[pos] in ['+', '*', ',', '|', '=']:
				dumpString()
				result.append([value[pos]])
			elif value[pos] == ' ':
				dumpString()
			else:
				tmp += value[pos]
			pos += 1
		dumpString()

		def isInt(value):
			try:
				int(value)
				return True
			except:
				return False
		# Resolve nested expressions
		pos = 0
		while pos < len(result):
			if isinstance(result[pos], tuple) and result[pos][0].startswith('('):
				result[pos] = (self.rewritePluginSetup(result[pos][0][1:-1]),)
			pos += 1
		# Implicit * between two tokens
		pos = 1
		while pos < len(result):
			if (not isinstance(result[pos - 1], list)) and (not isinstance(result[pos], list)):
				result.insert(pos, ['*'])
			pos += 1
		# Special rewrite for integer arguments
		pos = 1
		while pos < len(result):
			if (pos + 1 < len(result)) and isInt(result[pos - 1]) and (result[pos] == ['*']):
				result[pos] = ['x']
				tmp = result[pos + 1]
				result[pos + 1] = result[pos - 1]
				result[pos - 1] = tmp
			if (pos + 1 < len(result)) and isInt(result[pos + 1]) and (result[pos] == ['*']):
				result[pos] = ['x']
			pos += 1
		# Apply binary operators
		def applyOp(tokens, op, opname, parser = self.rewritePluginSetup):
			pos = 0
			while pos + 1 < len(tokens):
				if tokens[pos + 1] == [op]:
					args = [parser(tokens[pos])]
					while (pos + 2 < len(tokens)) and (tokens[pos + 1] == [op]):
						args.append(parser(tokens[pos + 2]))
						del tokens[pos + 2]
						del tokens[pos + 1]
					tokens[pos] = ('%s(%s)' % (opname, str.join(', ', args)),)
				pos += 1

		applyOp(result, '=', 'collect', lambda x: "'%s'" % x)
		lookupArgs = lambda x: self.rewritePluginSetup(x).replace('var', 'key')
		applyOp(result, '?', 'switch', lookupArgs)
		applyOp(result, '|', 'lookup', lookupArgs)
		applyOp(result, '*', 'cross')
		applyOp(result, 'x', 'repeat')
		applyOp(result, '+', 'chain')
		applyOp(result, ',', 'zip')

		if len(result) == 1 and not isinstance(result[0], tuple):
			if isInt(result[0]):
				return str(result[0])
			if result[0] == '<dataset>':
				return 'data()'
			return "var('%s')" % result[0]
		return result[0][0]
