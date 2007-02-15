from grid_control import RuntimeError, utils, enumerate

class Job:
	states = ('INIT', 'SUBMITTED', 'WAITING', 'READY', 'QUEUED', 'RUNNING', 'ABORTED', 'CANCELLED', 'FAILED', 'DONE', 'OK')
	_stateDict = {}
	for id, state in enumerate(states):
		_stateDict[state] = id
		locals()[state] = id
	__internals = ('id', 'status')


	def __init__(self, state = INIT):
		self.state = state
		self.id = None
		self.dict = {}


	def load(cls, fp):
		data = utils.parseShellDict(fp)
		job = Job(cls._stateDict[data['status']])

		if data.has_key('id'):
			job.id = data['id']

		for i in cls.__internals:
			try:
				del data[i]
			except:
				pass
		job.dict = data

		return job
	load = classmethod(load)


	def save(self, fp):
		data = self.dict
		data['status'] = self.states[self.state]
		if self.id != None:
			data['id'] = self.id

		for key, value in data.items():
			if value == None:
				continue
			elif type(value) in (int, float):
				value = str(value)
			else:
				value = utils.shellEscape(value)
			fp.write("%s=%s\n" % (key, value))


	def set(self, key, value):
		self.dict[key] = value


	def update(self, state):
		self.state = state
		# FIXME: job history or something


	def assignId(self, id):
		self.id = id
		# FIXME: History or sth.
