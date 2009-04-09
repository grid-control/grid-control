import os, copy, string, re
from fnmatch import fnmatch
from xml.dom import minidom
from grid_control import ConfigError, Module, WMS, DataProvider, utils
from DashboardAPI import DashboardAPI
from time import time, localtime, strftime

class CMSSW(Module):
	def __init__(self, config, init):
		Module.__init__(self, config, init)

		# SCRAM info
		scramProject = config.get('CMSSW', 'scram project', '').split()
		if len(scramProject):
			self.projectArea = config.getPath('CMSSW', 'project area', '')
			if len(self.projectArea):
				raise ConfigError('Cannot specify both SCRAM project and project area')
			if len(scramProject) != 2:
				raise ConfigError('SCRAM project needs exactly 2 arguments: PROJECT VERSION')
		else:
			self.projectArea = config.getPath('CMSSW', 'project area')
		self.scramArch = config.get('CMSSW', 'scram arch')

		self.configFile = config.getPath('CMSSW', 'config file')

		self.dataset = config.get('CMSSW', 'dataset', '').strip()

		if self.dataset == '':
			self.dataset = None
			self.eventsPerJob = config.getInt('CMSSW', 'events per job', 0)
		else:
			self.eventsPerJob = config.getInt('CMSSW', 'events per job')
			configFileContent = open(self.configFile, 'r').read()
			if configFileContent.find("__FILE_NAMES__") == -1 \
			or configFileContent.find("__MAX_EVENTS__") == -1 \
			or configFileContent.find("__SKIP_EVENTS__") == -1:
				print open(utils.atRoot('share', 'fail.txt'), 'r').read()
				print "Config file must use __FILE_NAMES__, __MAX_EVENTS__ and __SKIP_EVENTS__ to work properly with DBS datasets!"


		self.gzipOut = config.getBool('CMSSW', 'gzip output', True)
		self.useReqs = config.getBool('CMSSW', 'use requirements', True)
		self.seRuntime = config.getBool('CMSSW', 'se runtime', False)

		if self.seRuntime and len(self.projectArea):
			self.seInputFiles.append(self.taskID + ".tar.gz"),

		if len(self.projectArea):
			self.pattern = config.get('CMSSW', 'area files').split()

			if os.path.exists(self.projectArea):
				print "Project area found in: %s" % self.projectArea
			else:
				raise ConfigError("Specified config area '%s' does not exist!" % self.projectArea)

			# check, if the specified project area is a CMSSW project area
			envFile = os.path.join(self.projectArea, '.SCRAM', 'Environment')
			if not os.path.exists(envFile):
				raise ConfigError("Project area is not a SCRAM area.")

			# try to open it
			try:
				fp = open(envFile, 'r')
			except IOError, e:
				raise ConfigError("Project area .SCRAM/Environment cannot be opened: %s" + str(e))

			# find entries
			self.scramEnv = {}
			for line in fp:
				key, value = line.split("=")
				if type(key) == unicode:
					key = key.encode('utf-8')
				if type(value) == unicode:
					value = value.encode('utf-8')
				self.scramEnv[key] = value.strip()

		else:
			self.scramEnv = {
				'SCRAM_PROJECTNAME': scramProject[0],
				'SCRAM_PROJECTVERSION': scramProject[1]
			}

		if not self.scramEnv.has_key('SCRAM_PROJECTNAME') or \
		   not self.scramEnv.has_key('SCRAM_PROJECTVERSION') or \
		   self.scramEnv['SCRAM_PROJECTNAME'] != 'CMSSW':
			raise ConfigError("Project area not a valid CMSSW project area.")

		if not os.path.exists(self.configFile):
			raise ConfigError("Config file '%s' not found." % self.configFile)

		self.dataprovider = None
		if init:
			self._initTask(config)
		elif self.dataset != None:
			self.dataprovider = DataProvider.loadState(self.workDir)


	def _initTask(self, config):
		# function to walk directory in project area
		def walk(dir):
			for file in os.listdir(os.path.join(self.projectArea, dir)):
				if len(dir):
					name = os.path.join(dir, file)
				else:
					name = file
				for match in self.pattern:
					neg = match[0] == '-'
					if neg: match = match[1:]
					if fnmatch(name, match):
						break
				else:
					if os.path.isdir(os.path.join(self.projectArea, name)):
						walk(name)
					continue

				if not neg:
					files.append(name)

		if len(self.projectArea):
			# walk project area subdirectories and find files
			files = []
			walk('')
			utils.genTarball(os.path.join(self.workDir, 'runtime.tar.gz'), self.projectArea, files)

			if self.seRuntime:
				source = 'file:///' + os.path.join(self.workDir, 'runtime.tar.gz')
				target = os.path.join(self.sePath, self.taskID + '.tar.gz')
				print 'Copy CMSSW runtime to SE',

				# kill the runtime on se
				if config.getBool('CMSSW', 'se runtime force', True):
					tool = 'se_copy_force.sh'
				else:
					tool = 'se_copy.sh'

				if os.system('%s %s %s' % (utils.atRoot('share', tool), source, target)) == 0:
					print 'finished'
				else:
					print 'failed'
					raise ConfigError("Unable to copy runtime!")

		# find datasets
		if self.dataset != None:
			dbsapi = config.get('CMSSW', 'dbsapi', 'DBSApiv2')
			if "\n" in self.dataset:
				self.dataprovider = DataProvider.open("DataMultiplexer", self.dataset, dbsapi)
			else:
				self.dataprovider = DataProvider.open(dbsapi, self.dataset)
			self.dataprovider.run(self.eventsPerJob)
			self.dataprovider.printDataset()
##			self.dataprovider.printJobInfo()
			self.dataprovider.saveState(self.workDir)


	# Called on job submission
	def onJobSubmit(self, job, id):
		Module.onJobSubmit(self, job, id)

		if self.dashboard:
			dbsinfo = {}
			if self.dataprovider:
				dbsinfo = self.dataprovider.getFilesForJob(id)
			dashboard = DashboardAPI(self.taskID, "%s_%s" % (id, job.id))
			dashboard.publish(
				taskId=self.taskID, jobId="%s_%s" % (id, job.id), sid="%s_%s" % (id, job.id),
				application=self.scramEnv['SCRAM_PROJECTVERSION'], exe="cmsRun",
				nevtJob=self.eventsPerJob, tool="grid-control", GridName=self.username,
				scheduler="gLite", taskType="analysis", vo=self.config.get('grid', 'vo', ''),
				datasetFull=dbsinfo.get('DatasetPath', ''), user=os.environ['LOGNAME']
			)
		return None


	# Called on job status update
	def onJobUpdate(self, job, id, data):
		Module.onJobUpdate(self, job, id, data)

		if self.dashboard:
			dashboard = DashboardAPI(self.taskID, "%s_%s" % (id, job.id))
			dashboard.publish(
				taskId=self.taskID, jobId="%s_%s" % (id, job.id), sid="%s_%s" % (id, job.id),
				StatusValue=data.get('status', 'pending').upper(),
				StatusValueReason=data.get('reason', data.get('status', 'pending')).upper(),
				StatusEnterTime=data.get('timestamp', strftime("%Y-%m-%d_%H:%M:%S", localtime())),
				StatusDestination=data.get('dest', "")
			)
		return None


	def getRequirements(self, job):
		reqs = Module.getRequirements(self, job)
		if self.useReqs:
			reqs.append((WMS.MEMBER, 'VO-cms-%s' % self.scramEnv['SCRAM_PROJECTVERSION']))
			reqs.append((WMS.MEMBER, 'VO-cms-%s' % self.scramArch))
		if self.dataprovider != None:
			reqs.append((WMS.STORAGE, self.dataprovider.getSitesForJob(job)))
		return reqs


	def getCommand(self):
		return './run.cmssw.sh "$@"'


	def getTaskConfig(self):
		data = Module.getTaskConfig(self)
		data['CMSSW_CONFIG'] = os.path.basename(self.configFile)
		data['SCRAM_VERSION'] = 'scramv1'
		data['SCRAM_ARCH'] = self.scramArch
		data['SCRAM_PROJECTVERSION'] = self.scramEnv['SCRAM_PROJECTVERSION']
		data['USER_INFILES'] = str.join(' ', map(lambda x: utils.shellEscape(os.path.basename(x)), Module.getInFiles(self)))
		data['GZIP_OUT'] = ('no', 'yes')[self.gzipOut]
		data['SE_RUNTIME'] = ('no', 'yes')[self.seRuntime]
		data['HAS_RUNTIME'] = ('no', 'yes')[len(self.projectArea) != 0]
		return data


	def getJobConfig(self, job):
		data = Module.getJobConfig(self, job)
		dbsinfo = {}
		if self.dataprovider:
			dbsinfo = self.dataprovider.getFilesForJob(job)
		data['DATASETID'] = dbsinfo.get('DatasetID', None)
		data['DATASETPATH'] = dbsinfo.get('DatasetPath', None)
		data['DATASETNICK'] = dbsinfo.get('DatasetNick', None)
		return data


	def getInFiles(self):
		files = Module.getInFiles(self)
		if len(self.projectArea) and not self.seRuntime:
			files.append('runtime.tar.gz')
		files.append(utils.atRoot('share', 'run.cmssw.sh')),
		files.extend([self.configFile])

		if self.dashboard:
			for file in ('DashboardAPI.py', 'Logger.py', 'ProcInfo.py', 'apmon.py', 'report.py'):
				files.append(utils.atRoot('python/DashboardAPI', file))
		return files


	def getOutFiles(self):
		files = Module.getOutFiles(self)
		if self.gzipOut:
			files.append('cmssw_out.txt.gz')
		return files


	def getJobArguments(self, job):
		if self.dataprovider == None:
			return "%d" % self.eventsPerJob

		print "Job number: ", job
		dbsinfo = self.dataprovider.getFilesForJob(job)
		self.dataprovider.printInfoForJob(dbsinfo)
		return "%d %d %s" % (dbsinfo['events'], dbsinfo['skip'], str.join(' ', dbsinfo['files']))


	def getMaxJobs(self):
		if self.dataprovider == None:
			raise ConfigError('Must specifiy number of jobs or dataset!')
		return self.dataprovider.getNumberOfJobs()
