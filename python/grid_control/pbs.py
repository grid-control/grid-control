from __future__ import generators
import sys, os, popen2
from grid_control import ConfigError, WMS, Job, utils

class PBS(WMS):
	def __init__(self, config, module, init):
		WMS.__init__(self, config, module, init)

		self._submitExec = utils.searchPathFind('qsub')
		self._statusExec = utils.searchPathFind('qstat')

		self._queue = config.getPath('pbs', 'queue', '')

		self._tmpPath = os.path.join(self._outputPath, 'tmp')


	def submitJob(self, id, job):
		try:
			if not os.path.exists(self._tmpPath):
				os.mkdir(self._tmpPath)
		except IOError:
			raise RuntimeError("Temporary path '%s' could not be created." % self._tmpPath)

		activity = utils.ActivityLog('submitting jobs')

		params = ''
		if len(self._queue):
			params = ' -q %s' % self._queue

		name = 'GC' + self.module.getTaskID()
		job.set('name', name)

		outPath = os.path.join(self._tmpPath, name + '.stdout.txt')
		errPath = os.path.join(self._tmpPath, name + '.stderr.txt')
		executable = utils.atRoot('share', 'run.sh')

		proc = popen2.Popen3("%s%s -N %s -o %s -e %s %s"
		                     % (self._submitExec, params, name,
		                        utils.shellEscape(outPath),
		                        utils.shellEscape(errPath),
		                        utils.shellEscape(executable)),
		                     True)

		id = None

		lines = proc.child.readlines()

		for line in lines:
			line = line.strip()
			if len(line) and id is None:
				id = line
		retCode = proc.wait()

		del activity

		if retCode != 0:
			print >> sys.stderr, "WARNING: qsub failed:"
		elif id == None:
			print >> sys.stderr, "WARNING: qsub did not yield job id:"

		if id == None:
			for line in open(log, 'r'):
				sys.stderr.write(line)

		return id


	def checkJobs(self, ids):
		if not len(ids):
			return []

		activity = utils.ActivityLog("checking job status")

		proc = popen2.Popen3("%s --noint --logfile %s -i %s"
		                     % (self._statusExec,
		                        utils.shellEscape(log),
		                        utils.shellEscape(jobs)), True)

		try:
			for data in self._parseStatus(proc.fromchild.readlines()):
				id = data['id']
				del data['id']
				status = data['status']
				del data['status']
				result.append((id, status, data))

			retCode = proc.wait()

			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: glite-job-status failed:"
				for line in open(log, 'r'):
					sys.stderr.write(line)

		finally:
			try:
				os.unlink(jobs)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass

		return result


	def getJobsOutput(self, ids):
		if not len(ids):
			return []

		tmpPath = os.path.join(self._outputPath, 'tmp')
		try:
			if not os.path.exists(tmpPath):
				os.mkdir(tmpPath)
		except IOError:
			raise RuntimeError("Temporary path '%s' could not be created." % tmpPath)

		try:
			fd, jobs = tempfile.mkstemp('.jobids')
		except AttributeError:	# Python 2.2 has no tempfile.mkstemp
			while True:
				jobs = tempfile.mktemp('.jobids')
				try:
					fd = os.open(jobs, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
				except OSError:
					continue
				break

		log = tempfile.mktemp('.log')

		result = []

		try:
			fp = os.fdopen(fd, 'w')
			for id in ids:
				fp.write("%s\n" % id)
			fp.close()
			# FIXME: error handling

			activity = utils.ActivityLog("retrieving job outputs")

			proc = popen2.Popen3("%s --noint --logfile %s -i %s --dir %s"
			                     % (self._outputExec,
			                        utils.shellEscape(log),
			                        utils.shellEscape(jobs),
			                        utils.shellEscape(tmpPath)),
			                        True)

			for data in proc.fromchild.readlines():
				# FIXME: moep
				pass

			retCode = proc.wait()

			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: glite-job-output failed:"
				for line in open(log, 'r'):
					sys.stderr.write(line)

			for file in os.listdir(tmpPath):
				path = os.path.join(tmpPath, file)
				if os.path.isdir(path):
					result.append(path)

		finally:
			try:
				os.unlink(jobs)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass
			try:
				os.rmdir(tmpPath)
			except:
				pass

		return result
