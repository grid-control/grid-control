from para_base import ParaMod
from grid_control import exceptions, utils

class UberParaMod(ParaMod):
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

		lowercase_tuple = lambda t: tuple(map(str.lower, t))
		option_to_tuple = lambda o: utils.parseTuples(o)[0]

		names = utils.parseTuples(config.get('ParaMod', 'parameters'))
		options = dict(map(lambda (o, v): (repr(option_to_tuple(o)),
			utils.parseTuples(v)), config.parser.items('ParaMod')))

		def expandParam(param, type='words'):
			"""Expand param depending on its type.

			>>> expandParam("Where the wild things grow")
			['Where', 'the', 'wild', 'things', 'grow']
			
			>>> expandParam("range(3)", "expr")
			[0, 1, 2]
			
			Possible improvements: enum values to indicate type;  doctest
			might be wrong (unable to test)"""
			actions = {
				'expr': eval,
				'lines': lambda l: map(str.strip, l.splitlines()),
				'words': lambda l: map(str.strip, l.split())
			}

			try:
				return actions[type](param)
			except:
				raise exceptions.ConfigError("Illegal type '%s' in " +
						"parameter expansion" % type)

		self.pars = {}
		for p in names:
			if isinstance(p, tuple):
				p_key = repr(lowercase_tuple(p))
				if p_key in options:
					self.pars[repr(p)] = options[p_key]
				else:
					self.pars[repr(p)] = []
			else:
				self.pars[repr(p)] = expandParam(config.get('ParaMod', p),
						config.get('ParaMod', '%s type' % p, default='words'))

	def getParams(self):
		res = [[]]

		for p in self.pars.keys():
			m = len(res)
			n = len(self.pars[p])

			tmp = []
			p_ = eval(p)
			if isinstance(p_, tuple):
				tmp = map(lambda t: zip(p_, t),
					  self.pars[p])
			else:
				tmp = [[[p_, v]] for v in self.pars[p]]

			tmp_ = []
			for e in tmp:
				tmp_ += [e] * m
				
			res = map(lambda (x, y): x + y, zip(tmp_, res * n))

		return map(dict, res)


class MultiParaMod(UberParaMod):
	pass
