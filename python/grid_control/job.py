#from grid_control import 

try:
	enumerate
except:
	def enumerate(iterable):
		class EnumerateIter:
			def __init__(self, iterable):
				self.counter = 0
				self.iterable = iterable
			def __iter__(self):
				return self
			def next(self):
				return (self.counter, iterable.next())
		return EnumerateIter(iter(iterable))


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
