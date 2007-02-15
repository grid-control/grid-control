import os, copy, gzip, cPickle
from fnmatch import fnmatch
from xml.dom import minidom
from grid_control import ConfigError, Module, WMS, DBSApi, utils

class CMSSW(Module):
	def __init__(self, config, init):
		Module.__init__(self, config, init)

		self.projectArea = config.getPath('CMSSW', 'project area')
		self.configFile = config.getPath('CMSSW', 'config file')
		self.dataset = config.get('CMSSW', 'dataset', '')
		if self.dataset == '':
			self.dataset = None
			try:
				self.eventsPerJob = config.getInt('CMSSW', 'events per job')
			except:
				self.eventsPerJob = 0
		else:
			self.eventsPerJob = config.getInt('CMSSW', 'events per job')
		self.dbs = None

		self.pattern = config.get('CMSSW', 'files').split()

		if os.path.exists(self.projectArea):
			print "Project area found in: %s" % self.projectArea
		else:
			raise ConfigError("Specified config area '%s' does not exist!" % self.projectArea)

		# check, if the specified project area is a CMSSW project area
		envFile = os.path.join(self.projectArea, '.SCRAM', 'Environment.xml')
		if not os.path.exists(envFile):
			raise ConfigError("Project area is not a SCRAM area.")

		# try to open it
		try:
			fp = open(envFile, 'r')
		except IOError, e:
			raise ConfigError("Project area .SCRAM/Environment.xml cannot be opened: %s" + str(e))

		# try to parse it
		try:
			xml = minidom.parse(fp)
		except :
			raise ConfigError("Project area .SCRAM/Environment.xml file invalid.")
		fp.close()

		# find entries
		self.scramEnv = {}
		try:
			for node in xml.childNodes[0].childNodes:
				if node.nodeType != minidom.Node.ELEMENT_NODE:
					continue
				if node.nodeName != 'environment':
					continue

				for key, value in node.attributes.items():
					if type(key) == unicode:
						key = key.encode('utf-8')
					if type(value) == unicode:
						value = value.encode('utf-8')

					self.scramEnv[key] = value

		except:
			raise ConfigError("Project area .SCRAM/Environment.xml has bad XML structure.")

		if not self.scramEnv.has_key('SCRAM_PROJECTNAME') or \
		   not self.scramEnv.has_key('SCRAM_PROJECTVERSION') or \
		   self.scramEnv['SCRAM_PROJECTNAME'] != 'CMSSW':
			raise ConfigError("Project area not a valid CMSSW project area.")

		if not os.path.exists(self.configFile):
			raise ConfigError("Config file '%s' not found." % self.configFile)

		if init:
			self._init()


	def getRequirements(self):
		reqs = copy.copy(self.requirements)
		reqs.append((WMS.MEMBER, 'VO-cms-%s' % self.scramEnv['SCRAM_PROJECTVERSION']))
		reqs.append((WMS.STORAGE, self._getDataSites()))

		return reqs


	def _init(self):
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

		# walk project area subdirectories and find files
		files = []
		walk('')
		utils.genTarball(os.path.join(self.workDir, 'runtime.tar.gz'), 
		                 self.projectArea, files)

		# find datasets
		if self.dataset != None:
			self.dbs = DBSApi(self.dataset)
			self.dbs.run()

			# and dump to cache file
			fp = gzip.GzipFile(os.path.join(self.workDir, 'dbscache.dat'), 'wb')
			cPickle.dump(self.dbs, fp)
			fp.close()


	def _ensureDataCache(self):
		if self.dbs == None:
			fp = gzip.GzipFile(os.path.join(self.workDir, 'dbscache.dat'), 'rb')
			self.dbs = cPickle.load(fp)
			fp.close()


	def _getDataFiles(self, nJobs, firstEvent):
		self._ensureDataCache()
		return self.dbs.query(nJobs, self.eventsPerJob, firstEvent)


	def _getDataSites(self):
		self._ensureDataCache()
		return self.dbs.sites


	def makeConfig(self, fp):
		fp.write('CMSSW_CONFIG="%s"\n'
		         % utils.shellEscape(os.path.basename(self.configFile)));
		fp.write('SCRAM_VERSION="scramv1"\n');
		fp.write('SCRAM_PROJECTVERSION="%s"\n'
		         % utils.shellEscape(self.scramEnv['SCRAM_PROJECTVERSION']))


	def getInFiles(self):
		return ['runtime.tar.gz', utils.atRoot('share', 'cmssw.sh'),
		         self.configFile]


	def getOutFiles(self):
		return []


	def getJobArguments(self, job):
		if self.dataset == None:
			return "%d %d" % (job, num)

		files = self._getDataFiles(1, job * self.eventsPerJob)
		try:
			skip, num, files = iter(files).next()
		except:
			raise ConfigError("Job %d out of range for available dataset" % job)

		return "%d %d %d %s" % (job, num, skip, str.join(' ', files))
