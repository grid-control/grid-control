import sys, os, tempfile
from grid_control import WMS, Job, utils

class Glite(WMS):
	def __init__(self, config, module):
		WMS.__init__(self, config, module)

		self.sandboxIn = [ utils.atRoot('share', 'run.sh') ]
		self.sandboxIn.extend(module.getInFiles())
		self.sandboxOut = [ 'stdout.txt', 'stderr.txt' ]

		def memberReq(member):
			return 'Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' \
			       % self._escape(member)

		reqs = map(memberReq, module.getSoftwareMembers())

		self.requirements = str.join(' && ', reqs)


	def _escape(value):
		repl = { '\\': r'\\', '\"': r'\"', '\n': r'\n' }
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

			if value != '':
				fp.write("%s = %s\n" % (key, value))


	def checkJobs(self, ids):
		# FIXME: glite-job-status
		states = []
		return states


	def submitJob(self, id):
		try:
			fd, jdl = tempfile.mkstemp('.jdl')
		except AttributeError:	# Python 2.2 has no tempfile.mkstemp
			while True:
				jdl = tempfile.mktemp('.jdl')
				try:
					fd = os.open(jdl, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
				except OSError:
					continue
				break

		try:
			fp = os.fdopen(fd, 'w')
			self.makeJDL(fp, id)
			# FIXME: error handling
			fp.close()

			# FIXME: glite-job-submit
			return 'foobar_jobid_%d' % id

		finally:
			os.unlink(jdl)
