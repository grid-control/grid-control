import sys, os, tempfile, cStringIO
from grid_control import WMS, Job, utils

class Glite(WMS):
	def __init__(self, config, module):
		WMS.__init__(self, config, module)

		self.sandboxIn = [ utils.atRoot('share', 'run.sh') ]
		self.sandboxIn.extend(module.getInFiles())
		self.sandboxOut = [ 'stdout.txt', 'stderr.txt' ]


	def _escape(value):
		repl = { '\\': r'\\', '\"': r'\"', '\n': r'\n' }
		def replace(char):
			try:
				return repl[char]
			except:
				return char
		return '"' + str.join('', map(replace, value)) + '"'
	_escape = staticmethod(_escape)


	def memberReq(self, member):
		return 'Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' \
		       % self._escape(member)


	def wallTimeReq(self, wallTime):
		return '(other.GlueCEPolicyMaxWallClockTime >= %d)' \
		       % int((wallTime + 59) / 60)


	def storageReq(self, sites):
		print sites
		return '(' + str.join(' || ', map(lambda x: "Member(%s, other.GlueCESEBindGroupSEUniqueID)", sites)) + ')'


	def makeJDL(self, fp, job):
		contents = {
			'Executable': 'run.sh',
			'Arguments': self.module.getJobArguments(job),
			'InputSandbox': self.sandboxIn,
			'StdOutput': 'stdout.txt',
			'StdError': 'stderr.txt',
			'OutputSandbox': self.sandboxOut,
			'_Requirements': self.formatRequirements(self.module.getRequirements()),
			'VirtualOrganisation': self.config.get('grid', 'vo'),
			'RetryCount': 2
		}

		# JDL parameter formatter
		def jdlRep(value):
			if type(value) in (int, long):
				return str(value)
			elif type(value) in (tuple, list):
				return '{ ' + str.join(', ', map(jdlRep, value)) + ' }'
			else:
				return self._escape(value)

		# write key <-> formatted parameter pairs
		for key, value in contents.items():
			if key[0] == '_':
				key = key[1:]
			else:
				value = jdlRep(value)

			if value != '':
				fp.write("%s = %s;\n" % (key, value))


	def checkJobs(self, ids):
		# FIXME: glite-job-status
		states = []
		return states


	def submitJob(self, id, job):
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
			data = cStringIO.StringIO()
			self.makeJDL(data, id)
			data = data.getvalue()

			job.set('jdl', data)

			os.fdopen(fd, 'w').write(data)
			# FIXME: error handling

			# FIXME: glite-job-submit
			return 'foobar_jobid_%d' % id

		finally:
			os.unlink(jdl)
