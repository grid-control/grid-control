from grid_control import noDefault, utils, ConfigError, QM
import shlex

def parseParameterOption(option):
	result = map(str.strip, utils.findTuples(option.lower()))
	if len(result) and '(' in result[0]:
		result[0] = tuple(utils.accumulate(result[0], '', lambda i, b: not i.isalnum(), lambda i, b: i.isalnum()))
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
	def __init__(self, config, sections):
		(self.config, self.sections) = (config, sections)
		(self.varDict, self.optDict) = ({}, {})
		for section in sections:
			(varDict, optDict) = parseParameterOptions(config.getOptions(section))
			self.varDict.update(varDict)
			self.optDict.update(optDict)

	def parseParameter(self, varName, value, ptype):
		if ptype == 'verbatim':
			return [value]
		elif ptype == 'split':
			delimeter = self.get(self.varDict[varName], 'delimeter', ',')
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
			tupleDelimeter = self.get(self.varDict[varName], 'delimeter', ',')
			tupleList = map(lambda t: utils.parseTuple(t, tupleDelimeter), map(str.strip, utils.findTuples(tupleValue)))
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


	def getOpt(self, var, opt = None):
		return self.optDict.get((var, opt), ('%s %s' % (var, QM(opt, opt, ''))).replace('\'', ''))


	def get(self, var, opt = None, default = noDefault, mutable = False):
		return self.config.get(self.sections, self.getOpt(var, opt), default, mutable)


	def getBool(self, var, opt = None, default = noDefault, mutable = False):
		return self.config.getBool(self.sections, self.getOpt(var, opt), default, mutable)


	def getParameterOption(self, varName):
		try:
			return self.varDict[varName.lower()]
		except:
			raise ConfigError('Variable %s is undefined' % varName)


	def parseDict(self, varName, value, valueParser):
		keyTupleDelimeter = self.get(self.varDict[varName.lower()], 'key delimeter', ',')
		return utils.parseDict(value, valueParser, lambda k: utils.parseTuple(k, keyTupleDelimeter))


	def getParameter(self, varName):
		optKey = self.getParameterOption(varName)

		if isinstance(optKey, tuple):
			varIndex = list(optKey).index(varName)
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
