from grid_control import WMS

class Glite(WMS):
	def __init__(self, config, module):
		self.config = config
		self.module = module

		self.sandboxIn = [ 'run.sh' ]
		self.sandboxOut = [ 'stdout.txt', 'stderr.txt' ]

		def memberReq(member):
			return 'Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' \
			       % self._escape(member)

		reqs = map(memberReq, module.getSoftwareMembers())

		self.requirements = str.join(' && ', reqs)


	def _escape(value):
		repl = { '\"': r'\"', '\n': r'\n' }
		def replace(char):
			try:
				return repl[char]
			except:
				return char
		return '"' + str.join('', map(replace, value)) + '"'
	_escape = staticmethod(_escape)


	def makeJDL(self, fp, job):
		contents = {
			'Executable': 'run.sh',
			'Arguments': self.module.getJobArguments(job),
			'InputSandbox': self.sandboxIn,
			'StdOutput': 'stdout.txt',
			'StdError': 'stderr.txt',
			'OutputSandbox': self.sandboxOut,
			'_Requirements': self.requirements,
			'VirtualOrganisation': self.config.get('grid', 'vo'),
			'RetryCount': 2
		}


		def jdlRep(value):
			if type(value) in (int, long):
				return str(value)
			elif type(value) in (tuple, list):
				return '{ ' + str.join(', ', map(jdlRep, value)) + ' }'
			else:
				return self._escape(value)

		for key, value in contents.items():
			if key[0] == '_':
				key = key[1:]
			else:
				value = jdlRep(value)

			fp.write("%s = %s\n" % (key, value))
