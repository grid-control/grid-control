from __future__ import generators
import sys, os, popen2, tempfile, shutil, time
from grid_control import ConfigError, Job, utils
from wms import WMS

class LocalWMS(WMS):

	def __init__(self, config, module, init):
		WMS.__init__(self, config, module, 'local', init)

		self.sandPath = config.getPath('local', 'sandbox path', os.path.join(self.workDir, 'sandbox'))


	def getJobName(self, taskId, jobId):
		return taskId[:10] + "." + str(jobId) #.rjust(4, "0")[:4]


	def getSubmitArguments(self, id, env_vars, sandbox):
		raise RuntimeError('getSubmitArguments is abstract')


	def parseSubmitOutput(self, data):
		raise RuntimeError('parseSubmitOutput is abstract')


	def submitJob(self, id, job):
		# TODO: fancy job name function
		activity = utils.ActivityLog('submitting jobs')

		try:
			sandbox = tempfile.mkdtemp("." + str(id), self.module.taskID + ".", self.sandPath)
			for file in self.sandboxIn:
				shutil.copy(file, sandbox)
			job.set('sandbox', sandbox)
		except IOError:
			raise RuntimeError("Sandbox '%s' could not be prepared." % sandbox)

		env_vars = {
			'ARGS': utils.shellEscape("%d %s" % (id, self.module.getJobArguments(job))),
			'SANDBOX': sandbox
		}

		proc = popen2.Popen3("%s %s %s" % (
			self.submitExec, self.getSubmitArguments(id, env_vars, sandbox),
			utils.shellEscape(utils.atRoot('share', 'local.sh'))), True)

		wmsId = self.parseSubmitOutput(proc.fromchild.read())
		open(os.path.join(sandbox, wmsId), "w")
		retCode = proc.wait()

		del activity

		if retCode != 0:
			print >> sys.stderr, "WARNING: %s failed:" % self.submitExec
		elif wmsId == None:
			print >> sys.stderr, "WARNING: %s did not yield job id:" % self.submitExec

		if wmsId == '':
			sys.stderr.write(proc.childerr)

		return wmsId


	def parseStatus(self, status):
		raise RuntimeError('parseStatus is abstract')


	def getCheckArgument(self, wmsIds):
		raise RuntimeError('getCheckArgument is abstract')


	def checkJobs(self, wmsIds):
		if not len(wmsIds):
			return []

		activity = utils.ActivityLog("checking job status")
		proc = popen2.Popen3("%s %s" % (self.statusExec, self.getCheckArgument(wmsIds)), True)

		tmp = {}
		for data in self.parseStatus(proc.fromchild.readlines()):
			# (job number, status, extra info)
			tmp[data['id']] = (data['id'], self._statusMap[data['status']], data)

		result = []
		for wmsId in wmsIds:
			if not tmp.has_key(wmsId):
				result.append((wmsId, Job.DONE, {}))
			else:
				result.append(tmp[wmsId])

		retCode = proc.wait()
		del activity

		if retCode != 0:
			for line in proc.childerr.readlines():
				if not "Unknown Job Id" in line:
					sys.stderr.write(line)

		return result


	def getSandbox(self, wmsId):
		for jobdir in os.listdir(self.sandPath):
			path = os.path.join(self.sandPath, jobdir)
			if os.path.isdir(path):
				if wmsId in os.listdir(path):
					return path
		return None


	def getJobsOutput(self, wmsIds):
		if not len(wmsIds):
			return []

		result = []
		activity = utils.ActivityLog("retrieving job outputs")

		for wmsId in wmsIds:
			path = self.getSandbox(wmsId)
			if path == None:
				raise RuntimeError("Sandbox for wmsId '%s' could not be found" % wmsId)

			# Cleanup sandbox
			for file in os.listdir(path):
				if not file in self.sandboxOut:
					try:
						os.unlink(os.path.join(path, file))
					except:
						pass
			result.append(path)

		del activity
		return result


	def getCancelArgument(self, wmsIds):
		return str.join(" ", wmsIds)


	def cancelJobs(self, wmsIds):
		if not len(wmsIds):
			return True

		activity = utils.ActivityLog("cancelling jobs")

		proc = popen2.Popen3("%s %s" % (self.cancelExec, self.getCancelArgument(wmsIds)), True)
		retCode = proc.wait()
		if retCode != 0:
			print >> sys.stderr, "WARNING: %s failed:" % self.cancelExec
			return False

		# Wait for jobs to finish
		time.sleep(1)
		for wmsId in wmsIds:
			path = self.getSandbox(wmsId)
			if path == None:
				raise RuntimeError("Sandbox for wmsId '%s' could not be found" % wmsId)
			try:
				os.unlink(path)
			except:
				raise RuntimeError("Sandbox for wmsId '%s' could not be deleted" % wmsId)
			result.append(path)

		del activity
		return True
