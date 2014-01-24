from grid_control import noDefault, utils, ConfigError, QM, changeImpossible

import shlex

def parseTuple(t, delimeter):
	t = t.strip()
	if t.startswith('('):
		return tuple(map(str.strip, utils.split_advanced(t[1:-1], lambda tok: tok == delimeter, lambda tok: False)))
	return (t,)


def frange(start, end = None, num = None, steps = None, format = '%g'):
	if (end == None) and (num == None):
		raise exceptions.ConfigError('frange: No exit condition!')
	if (end != None) and (num != None) and (steps != None):
		raise exceptions.ConfigError('frange: Overdetermined parameters!')
	if (end != None) and (num != None) and (steps == None):
		steps = (end - start) / (num - 1)
		num -= 1
	if (end != None) and (num == None):
		steps = QM(steps, steps, 1)
		num = int(1 + (end - start) / steps)
	result = map(lambda i: start + QM(steps, steps, 1) * i, range(num)) + QM(end, [end], [])
	return map(lambda x: format % x, result)


def parseParameterOption(option):
	result = map(str.strip, utils.split_advanced(option.lower(), lambda t: t in ')]}', lambda t: True))
	if len(result) and '(' in result[0]:
		validChar = lambda c: c.isalnum() or (c in ['_'])
		result[0] = tuple(utils.accumulate(result[0], '', lambda i, b: not validChar(i), lambda i, b: validChar(i)))
	elif len(result) == 1:
		result = map(str.strip, result[0].split(' ', 1))
	if len(result) == 1:
		result.append(None)
	return tuple(result)


def parseParameterOptions(options):
	(varDict, optDict) = ({}, {})
	for rawOpt in options:
		var, opt = parseParameterOption(rawOpt)
		optDict[(var, opt)] = rawOpt
		if opt == None:
			if isinstance(var, tuple):
				for sk in var:
					varDict[sk] = var
			else:
				varDict[var] = var
	return (varDict, optDict)


class ParameterConfig:
	def __init__(self, config, sections, static):
		(self.config, self.sections, self.static) = (config, sections, static)
		(self.varDict, self.optDict) = ({}, {})
		for section in sections:
			(varDict, optDict) = parseParameterOptions(config.getOptions(section))
			self.varDict.update(varDict)
			self.optDict.update(optDict)
		self.configLog = {}


	def parseParameter(self, varName, value, ptype):
		if ptype == 'verbatim':
			return [value]
		elif ptype == 'split':
			delimeter = self.get(self.getParameterOption(varName), 'delimeter', ',')
			return map(str.strip, value.split(delimeter))
		elif ptype == 'lines':
			return value.splitlines()
		elif ptype == 'expr' or ptype == 'eval':
			result = eval(value)
			if isinstance(result, list):
				return result
			return [result]
		elif ptype == 'default':
			return shlex.split(value)
		raise ConfigError('[Variable: %s] Invalid parameter type: %s' % (varName, ptype))


	def parseParameterTuple(self, varName, tupleValue, tupleType, varType, varIndex):
		if tupleType == 'tuple':
			tupleDelimeter = self.get(self.getParameterOption(varName), 'delimeter', ',')
			tupleStrings = map(str.strip, utils.split_advanced(tupleValue, lambda tok: tok in ')]}', lambda tok: True))
			tupleList = map(lambda t: parseTuple(t, tupleDelimeter), tupleStrings)
		elif tupleType == 'binning':
			tupleList = zip(tupleValue.split(), tupleValue.split()[1:])

		def yieldEntries():
			for tupleEntry in tupleList:
				tmp = self.parseParameter(varName, tupleEntry[varIndex], varType)
				if isinstance(tmp, list):
					if len(tmp) != 1:
						raise ConfigError('[Variable: %s] Tuple entry (%s) expands to multiple variable entries (%s)!' % (varName, tupleEntry[varIndex], tmp))
					yield tmp[0]
				else:
					yield tmp
		return list(yieldEntries())


	def onChange(self, config, old_obj, cur_obj, cur_entry, obj2str):
		if self.static:
			changeImpossible(old_obj, cur_obj, cur_entry, obj2str)


	def getOpt(self, var, opt = None):
		return self.optDict.get((var, opt), ('%s %s' % (var, QM(opt, opt, ''))).replace('\'', ''))


	def get(self, var, opt = None, default = noDefault):
		result = self.config.get(self.sections, self.getOpt(var, opt), default, onChange = self.onChange)
		self.configLog[(var, opt)] = result
		return result


	def getBool(self, var, opt = None, default = noDefault):
		result = self.config.getBool(self.sections, self.getOpt(var, opt), default, onChange = self.onChange)
		self.configLog[(var, opt)] = result
		return result


	def getParameterOption(self, varName):
		try:
			return self.varDict[varName.lower()]
		except:
			raise ConfigError('Variable %s is undefined' % varName)


	def parseDict(self, varName, value, valueParser):
		keyTupleDelimeter = self.get(self.getParameterOption(varName), 'key delimeter', ',')
		return utils.parseDict(value, valueParser, lambda k: parseTuple(k, keyTupleDelimeter))


	def getParameter(self, varName):
		optKey = self.getParameterOption(varName)

		if isinstance(optKey, tuple):
			varIndex = list(optKey).index(varName.lower())
			tupleValue = self.get(optKey, None, '')
			tupleType = self.get(optKey, 'type', 'tuple')
			varType = self.get(varName, 'type', 'verbatim')

			if '=>' in tupleValue:
				return self.parseDict(varName, tupleValue,
					lambda v: self.parseParameterTuple(varName, v, tupleType, varType, varIndex))
			return self.parseParameterTuple(varName, tupleValue, tupleType, varType, varIndex)

		else:
			varValue = self.get(optKey, None, '')
			varType = self.get(varName, 'type', 'default')

			if '=>' in varValue:
				enableDict = self.getBool(optKey, 'parse dict', True)
				if enableDict:
					return self.parseDict(varName, varValue,
						lambda v: self.parseParameter(varName, v, varType))
			return self.parseParameter(varName, varValue, varType)
