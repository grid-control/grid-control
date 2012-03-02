import re, shlex
from para_base import ParaMod
from grid_control import exceptions, utils, QM

def frange(start, end = None, num = None, steps = None, format = "%g"):
	if (end == None) and (num == None):
		raise exceptions.ConfigError("frange: No exit condition!")
	if (end != None) and (num != None) and (steps != None):
		raise exceptions.ConfigError("frange: Overdetermined parameters!")
	if (end != None) and (num != None) and (steps == None):
		steps = (end - start) / (num - 1)
		num -= 1
	if (end != None) and (num == None):
		steps = QM(steps, steps, 1)
		num = int(1 + (end - start) / steps)
	result = map(lambda i: start + QM(steps, steps, 1) * i, range(num)) + QM(end, [end], [])
	return map(lambda x: format % x, result)


def parseTuples(s):
	def funCycle():
		while True:
			yield lambda entry: shlex.split(entry.replace(',', ' '))
			yield lambda entry: [tuple(parseTuples(entry))]
	for (fun, entry) in zip(funCycle(), re.split('\(([^\)]*)\)*', s)):
		for result in fun(entry):
			yield result


class MultiParaMod(ParaMod):
	"""This module builds all possible combinations of parameters
	and/or tuples of parameters.  For example,

		parameters  = spam (ham, eggs)
		spam        = A B
		(ham, eggs) = (1, 2) (3, 4)

	gives the following parameter combinations of (spam, ham, egg):

		(A, 1, 2), (B, 1, 2), (A, 3, 4), (B, 3, 4)

	Values of single parameters (e.g. 'spam' in the example above) can
	either be 'words', span 'lines' or be an 'expr'ession that gets
	evaluated.  To let 'foo' go from 0 to 9, the following can be used:

		paramters = foo
		foo       = range(10)
		foo type  = expr

	The default type is 'words' - the parameter value gets split on
	whitespace."""
	def __init__(self, config):
		ParaMod.__init__(self, config)

		def expandParam(param, mode = 'words'):
			""" Expand param depending on its type.
			>>> expandParam('Where the wild things roam')
			['Where', 'the', 'wild', 'things', 'roam']
			>>> expandParam('range(3)', 'expr')
			[0, 1, 2]
			"""
			actions = {
				'expr': eval,
				'lines': lambda l: map(str.strip, l.splitlines()),
				'words': lambda l: list(parseTuples(l)),
				'binning': lambda l: zip(l.split(), l.split()[1:])
			}
			try:
				return actions[mode.lower()](param)
			except:
				raise exceptions.ConfigError("Illegal mode '%s' in parameter expansion" % mode)

		# Map between parsed keys and config keys
		keymap = dict(map(lambda x: (repr(list(parseTuples(x))).lower(), x), config.getOptions('ParaMod')))
		self.pset = {}
		for p in parseTuples(config.get('ParaMod', 'parameters')):
			km_type_key, km_var_key = (repr([p, 'type']).lower(), keymap[repr([p]).lower()])
			expConfig = config.get('ParaMod', keymap.get(km_type_key, 'parameters type'), 'words')
			self.pset[p] = expandParam(config.get('ParaMod', km_var_key), expConfig)


	def getParams(self):
		result = [[]]
		for (p_var, p_value) in self.pset.items():
			if isinstance(p_var, tuple):
				protoDict = map(lambda t: zip(p_var, t), p_value)
			else:
				protoDict = map(lambda v: [[p_var, v]], p_value)
			tmp = []
			for entry in protoDict:
				tmp += [entry] * len(result)
			result = map(lambda (x, y): x + y, zip(tmp, result * len(p_value)))
		return map(dict, result)


class UberParaMod(MultiParaMod):
	pass
