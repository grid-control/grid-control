from __future__ import generators
#from grid_control import 

try:
	enumerate
except:
	def enumerate(iterable):
		i = 0
		for item in iterable:
			yield (i, item)
			i = i + 1


class Job:
	states = ('SUBMITTED', 'RUNNING', 'ABORTED', 'FAILED', 'DONE', 'ERROR')
	_stateDict = {}
	for id, state in enumerate(states):
		_stateDict[state] = id
		locals()[state] = id

	def __init__(self, fp):
		data = {}
		for line in fp.readlines():
			key, value = line.split('=', 1)
			data[key.strip()] = value.strip()

		self.state = self._stateDict[data['status']]
