import re
from utils import DictFormat
from time import time

class Job:
	states = ('INIT', 'SUBMITTED', 'WAITING', 'READY', 'QUEUED', 'RUNNING', 'ABORTED', 'CANCELLED', 'FAILED', 'DONE', 'SUCCESS')
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

		if data.has_key('id'):
			job.wmsId = data['id']
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
		fp = open(name, 'w')
		fp.writelines(DictFormat(escapeString = True).format(self.getAll()))
		fp.truncate()
		fp.close()


	def set(self, key, value):
		self.dict[key] = value


	def get(self, key):
		return self.dict.get(key, None)


	def update(self, state):
		self.state = state
		self.history[self.attempt] = self.dict.get('dest', 'N/A')
		# FIXME: job history or something


	def assignId(self, wmsId):
		self.wmsId = wmsId
		self.attempt = self.attempt + 1
		self.submitted = time()
		# FIXME: History or sth.


	def report(self):
		return {
			'Status': self.states[self.state],
			'Destination': self.dict.get('dest', 'N/A'),
			'Id': self.wmsId
		}


	def statefilter(self, filterExpr):
		for state in filterExpr.split(','):
			regex = re.compile('^' + state + '.*')
			for key in self._stateDict.keys():
				if regex.match(key) and self.state == self._stateDict[key]:
					return True
#	TODO: Site selection
#			regex = re.compile(state + '$')
#			if self.dict.has_key("dest") and regex.match(self.dict.get("dest")):
#				return True
		return False
