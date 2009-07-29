import sys, os, popen2, tempfile, shutil, time, random
from grid_control import AbstractObject, ConfigError, Job, utils
from wms import WMS
from broker import Broker

class LocalWMSApi(AbstractObject):
	def __init__(self, config, localWMS):
		self.config = config
		self.wms = localWMS

	def getQueues(self):
		raise AbstractError

	def getArguments(self, jobNum, sandbox):
		raise AbstractError

	def getSubmitArguments(self, jobNum, sandbox):
		raise AbstractError

	def parseSubmitOutput(self, data):
		raise AbstractError

	def unknownID(self):
		raise AbstracError

	def parseStatus(self, status):
		raise AbstracError

	def getCheckArgument(self, wmsIds):
		raise AbstracError

	def getCancelArgument(self, wmsIds):
		return str.join(" ", wmsIds)


class LocalWMS(WMS):
	def __init__(self, config, opts, module):
		WMS.__init__(self, config, opts, module, 'local')

		wmsapi = config.get('local', 'wms', self._guessWMS())
		self.api = LocalWMSApi.open("grid_control.backends.%s.%s" % (wmsapi.lower(), wmsapi), config, self)

		try:
			queues = self.api.getQueues()
		except:
			queues = None
		self.broker = Broker.open(config.get('local', 'broker', 'DummyBroker'), config, queues)

		self.sandPath = config.getPath('local', 'sandbox path', os.path.join(opts.workDir, 'sandbox'))
		self._nameFile = config.getPath('local', 'name source', '')
		self._source = None
		if self._nameFile != '':
			tmp = map(str.strip, open(self._nameFile, 'r').readlines())
			self._source = filter(lambda x: not (x.startswith('#') or x == ''), tmp)


	def _guessWMS(self):
		wmsCmdList = [ ('PBS', 'pbs-config'), ('SGE', 'qsub'), ('LSF', 'bsub'), ('SLURM', 'job_slurm'), ('PBS', 'sh') ]
		for wms, cmd in wmsCmdList:
			try:
				utils.searchPathFind(cmd)
				print "Default batch system on this host is: %s" % wms
				return wms
			except:
				pass


	# Wait 5 seconds between cycles and 0 seconds between steps
	def getTimings(self):
		return (5, 0)


	def getJobName(self, jobNum):
		if self._source:
			return self._source[jobNum % len(self._source)]
		return self.module.taskID[:10] + "." + str(jobNum) #.rjust(4, "0")[:4]


	def getRequirements(self, jobNum):
		return self.broker.matchQueue(WMS.getRequirements(self, jobNum))


	# Submit job and yield (jobNum, WMS ID, other data)
	def submitJob(self, jobNum):
		# TODO: fancy job name function
		activity = utils.ActivityLog('submitting jobs')

		try:
			if not os.path.exists(self.sandPath):
				os.mkdir(self.sandPath)
			sandbox = tempfile.mkdtemp("", "%s.%04d." % (self.module.taskID, jobNum), self.sandPath)
			for file in self.sandboxIn:
				shutil.copy(file, sandbox)
		except OSError:
			raise RuntimeError("Sandbox path '%s' is not accessible." % self.sandPath)
		except IOError:
			raise RuntimeError("Sandbox '%s' could not be prepared." % sandbox)

		env_vars = {
			'ARGS': utils.shellEscape("%d %s" % (jobNum, self.module.getJobArguments(jobNum))),
			'SANDBOX': sandbox
		}
		env_vars.update(self.module.getJobConfig(jobNum))

		jcfg = open(os.path.join(sandbox, '_jobconfig.sh'), 'w')
		jcfg.writelines(utils.DictFormat().format(env_vars, format = 'export %s%s%s\n'))
		proc = popen2.Popen3("%s %s %s %s" % (self.api.submitExec,
			self.api.getSubmitArguments(jobNum, sandbox),
			utils.shellEscape(utils.atRoot('share', 'local.sh')),
			self.api.getArguments(jobNum, sandbox)), True)

		wmsIdText = proc.fromchild.read().strip().strip("\n")
		try:
			wmsId = self.api.parseSubmitOutput(wmsIdText)
		except:
			wmsId = None
		retCode = proc.wait()

		del activity

		if retCode != 0:
			print >> sys.stderr, "WARNING: %s failed:" % self.api.submitExec
		elif wmsId == None:
			print >> sys.stderr, "WARNING: %s did not yield job id:" % self.api.submitExec
			print >> sys.stderr,  wmsIdText

		if (wmsId == '') or (wmsId == None):
			sys.stderr.write(proc.childerr.read())
		else:
			open(os.path.join(sandbox, wmsId), "w")
		return (jobNum, wmsId, {'sandbox': sandbox})


	# Check status of jobs and yield (wmsID, status, other data)
	def checkJobs(self, ids):
		if not len(ids):
			return []

		shortWMSIds = map(lambda (wmsId, jobNum): wmsId.split(".")[0], ids)
		activity = utils.ActivityLog("checking job status")
		proc = popen2.Popen3("%s %s" % (self.api.statusExec, self.api.getCheckArgument(shortWMSIds)), True)

		tmp = {}
		jobstatusinfo = proc.fromchild.read()
		for data in self.api.parseStatus(jobstatusinfo):
			# (job number, status, extra info)
			tmp[data['id']] = (data['id'], self.api._statusMap[data['status']], data)
		proc.wait()

		result = []
		for wmsId, jobNum in ids:
			if not tmp.has_key(wmsId):
				result.append((jobNum, wmsId, Job.DONE, {}))
			else:
				result.append(tuple([jobNum] + list(tmp[wmsId])))

		retCode = proc.wait()
		del activity

		if retCode != 0:
			for line in proc.childerr.readlines():
				if not self.api.unknownID() in line:
					sys.stderr.write(line)

		return result


	def getSandbox(self, wmsId):
		for jobdir in os.listdir(self.sandPath):
			path = os.path.join(self.sandPath, jobdir)
			if os.path.isdir(path):
				if wmsId in os.listdir(path):
					return path
		return None


	def getJobsOutput(self, ids):
		if not len(ids):
			raise StopIteration

		activity = utils.ActivityLog("retrieving job outputs")
		for wmsId, jobNum in ids:
			path = self.getSandbox(wmsId)
			if path == None:
				yield (jobNum, None)
				continue

			# Cleanup sandbox
			for file in os.listdir(path):
				if file in self.sandboxOut:
					continue
				try:
					os.unlink(os.path.join(path, file))
				except:
					pass
			yield (jobNum, path)
		del activity


	def cancelJobs(self, ids):
		if not len(ids):
			return True

		activity = utils.ActivityLog("cancelling jobs")

		shortWMSIds = map(lambda (wmsId, jobNum): wmsId.split(".")[0], ids)
		proc = popen2.Popen3("%s %s" % (self.api.cancelExec, self.api.getCancelArgument(shortWMSIds)), True)
		retCode = proc.wait()

		if retCode != 0:
			for line in proc.childerr.readlines():
				if not self.api.unknownID() in line:
					sys.stderr.write(line)

		del activity
		activity = utils.ActivityLog("waiting for jobs to finish")
		# Wait for jobs to finish
		time.sleep(5)
		for wmsId, jobNum in ids:
			path = self.getSandbox(wmsId)
			if path == None:
				print "Sandbox for job %d with wmsId '%s' could not be found" % (jobNum, wmsId)
				continue
			try:
				shutil.rmtree(path)
			except:
				raise RuntimeError("Sandbox for job %d with wmsId '%s' could not be deleted" % (jobNum, wmsId))

		del activity
		return True
