from para_base import ParaMod
from grid_control import exceptions, utils

class MultiParaMod(ParaMod):
	"""This module builds all possible combinations of parameters
	and/or tuples of parameters.  For example,

		parameters  = spam (ham, eggs)
		spam        = A B
		(ham, eggs) = (1, 2) (3, 4)

	gives the following parameter combinations of (spam, ham, egg):

		(A, 1, 2), (B, 1, 2), (A, 3, 4), (B, 3, 4)

	Values of single parameters (e.g. "spam" in the example above) can
	either be "words", span "lines" or be an "expr"ession that gets
	evaluated.  To let "foo" go from 0 to 9, the following can be used:

		paramters = foo
		foo       = range(10)
		foo type  = expr

	The default type is "words" - the parameter value gets split on
	whitespace."""
	def __init__(self, config):
		ParaMod.__init__(self, config)

		def expandParam(param, mode = 'words'):
			""" Expand param depending on its type.
			>>> expandParam("Where the wild things roam")
			['Where', 'the', 'wild', 'things', 'roam']
			>>> expandParam("range(3)", "expr")
			[0, 1, 2]
			Possible improvements: enum values to indicate mode;  doctest
			might be wrong (unable to test)"""
			actions = {
				'expr': eval,
				'lines': lambda l: map(str.strip, l.splitlines()),
				'words': lambda l: utils.parseTuples(l),
				'binning': lambda l: zip(l.split(), l.split()[1:])
			}
			try:
				return actions[mode.lower()](param)
			except:
				raise exceptions.ConfigError("Illegal mode '%s' in parameter expansion" % mode)

		# Map between parsed keys and config keys
		parse_key = lambda x: (str.join(" ", map(repr, utils.parseTuples(x))), x)
		keymap = dict(map(parse_key, config.parser.options('ParaMod')))

		self.pset = {}
		for p in utils.parseTuples(config.get('ParaMod', 'parameters')):
			p_key = repr(p).lower()
			if isinstance(p, tuple):
				p_key = repr(tuple(map(str.lower, p)))
			expConfigSource = keymap.get("%s 'type'" % p_key, "parameters type")
			expConfig = config.get('ParaMod', expConfigSource, default = 'words')
			self.pset[repr(p)] = expandParam(config.get('ParaMod', keymap[p_key]), expConfig)


	def getParams(self):
		res = [[]]

		for p in self.pset.keys():
			m = len(res)
			n = len(self.pset[p])

			tmp = []
			p_ = eval(p)
			if isinstance(p_, tuple):
				tmp = map(lambda t: zip(p_, t), self.pset[p])
			else:
				tmp = [[[p_, v]] for v in self.pset[p]]

			tmp_ = []
			for e in tmp:
				tmp_ += [e] * m
				
			res = map(lambda (x, y): x + y, zip(tmp_, res * n))

		return map(dict, res)


class UberParaMod(MultiParaMod):
	pass
