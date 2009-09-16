from para_base import ParaMod
from grid_control import utils

class UberParaMod(ParaMod):
	"""This module builds all possible combinations of parameters
	and/or tuples of parameters.  For example,

		parameters  = spam (ham, eggs)
		spam        = A B
		(ham, eggs) = (1, 2) (3, 4)

	gives the following parameter combinations of (spam, ham, egg):

		(A, 1, 2), (B, 1, 2), (A, 3, 4), (B, 3, 4)
	"""
	def __init__(self, config):
		ParaMod.__init__(self, config)

		lowercase_tuple = lambda t: tuple(map(str.lower, t))
		option_to_tuple = lambda o: utils.parseTuples(o)[0]

		names = utils.parseTuples(config.get('ParaMod', 'parameters'))
		options = dict(map(lambda (o, v): (repr(option_to_tuple(o)),
			utils.parseTuples(v)), config.parser.items('ParaMod')))

		self.pars = {}
		for p in names:
			if isinstance(p, tuple):
				p_key = repr(lowercase_tuple(p))
				if p_key in options:
					self.pars[repr(p)] = options[p_key]
				else:
					self.pars[repr(p)] = []
			else:
				self.pars[repr(p)] = map(str.strip,
					config.get('ParaMod', p).split())

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
