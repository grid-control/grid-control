import time
from utils import DictFormat, safeWriteFile

class Job:
	states = ('INIT', 'SUBMITTED', 'DISABLED', 'READY', 'WAITING', 'QUEUED', 'ABORTED',
		'RUNNING', 'CANCELLED', 'DONE', 'FAILED', 'SUCCESS')
	_stateDict = {}
	for id, state in enumerate(states):
		_stateDict[state] = id
		locals()[state] = id
	__internals = ('wmsId', 'status')


	def __init__(self, state = INIT):
		self.state = state
		self.attempt = 0
		self.history = {}
		self.wmsId = None
		self.submitted = 0
		self.dict = {}


	def load(cls, name):
		try:
			data = DictFormat(escapeString = True).parse(open(name))
		except:
			raise ConfigError('Invalid format in %s' % fp.name)
		job = Job(cls._stateDict[data.get('status', 'FAILED')])

		if 'id' in data:
			job.wmsId = data['id']
		if 'attempt' in data:
			job.attempt = data['attempt']
		if 'submitted' in data:
			job.submitted = data['submitted']
		if 'runtime' not in data:
			if 'submitted' in data:
				data['runtime'] = time.time() - float(job.submitted)
			else:
				data['runtime'] = 0

		for key in range(1, job.attempt + 1):
			if ('history_' + str(key)).strip() in data:
				job.history[key] = data['history_' + str(key)]

		for i in cls.__internals:
			try:
				del data[i]
			except:
				pass
		job.dict = data

		return job
	load = classmethod(load)


	def getAll(self):
		data = self.dict
		data['status'] = self.states[self.state]
		data['attempt'] = self.attempt
		data['submitted'] = self.submitted
		for key, value in self.history.items():
			data['history_' + str(key)] = value
		if self.wmsId != None:
			data['id'] = self.wmsId
		return data


	def save(self, name):
		safeWriteFile(name, DictFormat(escapeString = True).format(self.getAll()))


	def set(self, key, value):
		self.dict[key] = value


	def get(self, key):
		return self.dict.get(key, None)


	def update(self, state):
		self.state = state
		self.history[self.attempt] = self.dict.get('dest', 'N/A')


	def assignId(self, wmsId):
		self.wmsId = wmsId
		self.attempt = self.attempt + 1
		self.submitted = time.time()


	def report(self):
		return {
			'Status': self.states[self.state],
			'Destination': self.dict.get('dest', 'N/A'),
			'Id': self.wmsId
		}
