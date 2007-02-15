from grid_control import RuntimeError, utils, enumerate

class Job:
	states = ('INIT', 'SUBMITTED', 'WAITING', 'READY', 'QUEUED', 'RUNNING', 'ABORTED', 'FAILED', 'DONE', 'OK')
	_stateDict = {}
	for id, state in enumerate(states):
		_stateDict[state] = id
		locals()[state] = id
	__internals = ('id', 'status')


	def __init__(self, state = INIT):
		self.state = state
		self.id = None
		self.dict = {}


	def _getString(value, lineIter):
		inString = False
		result = []
		pos = 0
		while True:
			if inString:
				back = value.find('\\', pos)
				quote = value.find('"', pos)
				if back >= 0 and quote >= 0:
					if back < quote:
						quote = -1
					else:
						back = -1
				if back >= 0:
					if len(value) < back + 2:
						raise RuntimeError('Invalid job format')
					if back > pos:
						result.append(value[pos:back])
					result.append(value[back + 1])
					pos = back + 2
				elif quote >= 0:
					if quote > pos:
						result.append(value[pos:quote])
					pos = quote + 1
					inString = False
				else:
					if len(value) > pos:
						result.append(value[pos:])
					pos = -1

				if pos < 0 or pos >= len(value):
					if not inString:
						break

					try:
						value = lineIter.next()
						pos = 0
					except StopIteration:
						raise('Invalid job format')
			else:
				value = value[pos:].lstrip()
				if not len(value):
					break
				elif value[0] != '"':
					raise RuntimeError('Invalid job file')
				pos = 1
				inString = True

		return str.join('', result)
	_getString = staticmethod(_getString)


	def load(cls, fp):
		data = {}
		lineIter = iter(fp)
		while True:
			try:
				line = lineIter.next()
			except StopIteration:
				break
			key, value = line.split('=', 1)
			key = key.strip()
			value = value.lstrip()
			if value[0] == '"':
				value = cls._getString(value, lineIter)
			elif value.find('.') >= 0:
				value = float(value)
			else:
				value = int(value)
			data[key] = value

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
