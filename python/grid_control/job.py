from __future__ import generators
#from grid_control import 

try:
	enumerate
except:
	# stupid python 2.2 doesn't have a builtin enumerator
	def enumerate(iterable):
		i = 0
		for item in iterable:
			yield (i, item)
			i = i + 1


class Job:
	states = ('INIT', 'SUBMITTED', 'WAITING', 'READY', 'QUEUED', 'RUNNING', 'ABORTED', 'FAILED', 'DONE', 'OK')
	_stateDict = {}
	for id, state in enumerate(states):
		_stateDict[state] = id
		locals()[state] = id


	def __init__(self, state = INIT):
		self.state = state
		self.id = None


	def load(cls, fp):
		data = {}
		for line in fp.readlines():
			key, value = line.split('=', 1)
			data[key.strip()] = value.strip()

		job = Job(cls._stateDict[data['status']])

		if data.has_key('id'):
			job.id = data['id']

		return job
	load = classmethod(load)


	def save(self, fp):
		data = {}
		data['status'] = self.states[self.state]
		if self.id != None:
			data['id'] = self.id

		for key, value in data.items():
			fp.write("%s = %s\n" % (key, value))


	def update(self, state):
		self.state = state
		# FIXME: job history or something


	def assignId(self, id):
		self.id = id
		# FIXME: History or sth.
