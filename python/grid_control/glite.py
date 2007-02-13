import sys, os, popen2, tempfile, cStringIO
from grid_control import WMS, Job, utils

class Glite(WMS):
	def __init__(self, config, module, init):
		WMS.__init__(self, config, module, init)

		self._submitExec = utils.searchPathFind('glite-job-submit')
		self._statusExec = utils.searchPathFind('glite-job-status')
		self._outputExec = utils.searchPathFind('glite-job-output')


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
		def makeMember(member):
			return "Member(%s, other.GlueCESEBindGroupSEUniqueID)" % self._escape(member)
		if len(sites) == 0:
			return None
		elif len(sites) == 1:
			return makeMember(sites[0])
		else:
			return '(' + str.join(' || ', map(makeMember, sites)) + ')'


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
