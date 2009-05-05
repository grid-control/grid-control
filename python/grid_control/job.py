import re
from grid_control import RuntimeError, utils, enumerate, UserError
from time import time

class Job:
	states = ('INIT', 'SUBMITTED', 'WAITING', 'READY', 'QUEUED', 'RUNNING', 'ABORTED', 'CANCELLED', 'FAILED', 'DONE', 'SUCCESS')
	_stateDict = {}
	for id, state in enumerate(states):
		_stateDict[state] = id
		locals()[state] = id
	__internals = ('id', 'status')


	def __init__(self, state = INIT):
		self.state = state
		self.attempt = 0
		self.history = {}
		self.id = None
		self.submitted = 0
		self.dict = {}


	def load(cls, fp):
		try:
			data = utils.DictFormat(escapeString = True).parse(fp)
		except:
			raise ConfigError('Invalid format in %s' % fp.name)
		job = Job(cls._stateDict[data['status']])

		if data.has_key('id'):
			job.id = data['id']
		if data.has_key('attempt'):
			job.attempt = data['attempt']
		if data.has_key('submitted'):
			job.submitted = data['submitted']
		if not data.has_key('runtime'):
			if data.has_key('submitted'):
				data['runtime'] = time() - float(job.submitted)
			else:
				data['runtime'] = 0

		for key in range(1, job.attempt + 1):
			if data.has_key(('history_' + str(key)).strip()):
				job.history[key] = data['history_' + str(key)]

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
		data['attempt'] = self.attempt
		data['submitted'] = self.submitted
		for key, value in self.history.items():
			data['history_' + str(key)] = value
		if self.id != None:
			data['id'] = self.id
		fp.writelines(utils.DictFormat(escapeString = True).format(data))


	def set(self, key, value):
		self.dict[key] = value


	def get(self, key):
		return self.dict.get(key, None)


	def update(self, state):
		self.state = state
		self.history[self.attempt] = self.dict.get('dest', 'N/A')
		# FIXME: job history or something


	def assignId(self, id):
		self.id = id
		self.attempt = self.attempt + 1
		self.submitted = time()
		# FIXME: History or sth.


	def report(self):
		return (self.states[self.state], self.dict.get('dest', 'N/A'), self.id)


	def statefilter(self, filter):
		for state in filter.split(','):
			regex = re.compile('^' + state + '.*')
			for key in self._stateDict.keys():
				if regex.match(key) and self.state == self._stateDict[key]:
					return True
		return False
