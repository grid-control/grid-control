import os, copy, gzip, cPickle, string, re
from fnmatch import fnmatch
from xml.dom import minidom
from grid_control import ConfigError, Module, WMS, DataDiscovery, utils

class CMSSW(Module):
	def __init__(self, config, init):
		Module.__init__(self, config, init)
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
		self.dbsapi = config.get('CMSSW', 'dbsapi')
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

		self.anySites = config.get('CMSSW', 'sites', 'no') == 'any' or \
				self.dbs == None
		
		self.gzipOut = config.getBool('CMSSW', 'gzip output', True)

		try:
			self.seeds = map(lambda x: int(x), config.get('CMSSW', 'seeds', '').split())
		except:
			raise ConfigError("Invalid CMSSW seeds!")

		try:
			self.seOutputFiles = config.get('CMSSW', 'se output files', '').split()

		except:
			self.seOutputFiles = ""
	
		try:
			self.sePath = config.get('CMSSW', 'se path')
		except:
			self.sePath = ""			

		if len(self.projectArea):
			self.pattern = config.get('CMSSW', 'area files').split()

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

		if init:
			self._init()


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

		if len(self.projectArea):
			# walk project area subdirectories and find files
			files = []
			walk('')
			utils.genTarball(os.path.join(self.workDir, 'runtime.tar.gz'), 
			                 self.projectArea, files)

		# find datasets
		if self.dataset != None:
			self.dbs = DataDiscovery.open(self.dbsapi, self.dataset)
			self.dbs.run(self.eventsPerJob)
			self.dbs.printDataset()
##			self.dbs.printJobInfo()

			# and dump to cache file
			fp = gzip.GzipFile(os.path.join(self.workDir, 'dbscache.dat'), 'wb')
			cPickle.dump(self.dbs, fp)
			fp.close()


	def _ensureDataCache(self):
		if self.dbs == None and self.dataset != None:
			fp = gzip.GzipFile(os.path.join(self.workDir, 'dbscache.dat'), 'rb')
			self.dbs = cPickle.load(fp)
			fp.close()


	def _getDataFiles(self, job):
		self._ensureDataCache()
		return self.dbs.GetFilerangeForJob(job)


	def _getDataSites(self, job):
		self._ensureDataCache()
		return self.dbs.GetSitesForJob(job)


	def getRequirements(self, job):
		reqs = copy.copy(self.requirements)
		reqs.append((WMS.MEMBER, 'VO-cms-%s' % self.scramEnv['SCRAM_PROJECTVERSION']))
		reqs.append((WMS.MEMBER, 'VO-cms-%s' % self.scramArch))

		if self.dataset != None:
	       		reqs.append((WMS.STORAGE, self._getDataSites(job)))

		return reqs


	def getCommand(self):
		return './cmssw.sh "$@"'


	def getConfig(self):
		return {
			'CMSSW_CONFIG': os.path.basename(self.configFile),
			'SCRAM_VERSION': 'scramv1',
			'SCRAM_ARCH': self.scramArch,
			'SCRAM_PROJECTVERSION': self.scramEnv['SCRAM_PROJECTVERSION'],
			'USER_INFILES': str.join(' ', map(lambda x: utils.shellEscape(os.path.basename(x)), Module.getInFiles(self))),
			'GZIP_OUT': ('no', 'yes')[self.gzipOut],
			'HAS_RUNTIME': ('no', 'yes')[len(self.projectArea) != 0],
			'SEEDS': str.join(' ', map(lambda x: "%d" % x, self.seeds)),
			'SE_OUTPUT_FILES': str.join(' ', self.seOutputFiles),
			'SE_PATH': self.sePath,
		}


	def getInFiles(self):
		files = Module.getInFiles(self)
		if len(self.projectArea):
			files.append('runtime.tar.gz')
		files.extend([
			utils.atRoot('share', 'cmssw.sh'),
			self.configFile
		])
		return files


	def getOutFiles(self):
		files = Module.getOutFiles(self)
		if self.gzipOut:
			files.append('cmssw_out.txt.gz')
		return files


	def getJobArguments(self, job):
		if self.dataset == None:
			return "%d" % self.eventsPerJob
		
		print ""
		print "Job number: ",job
		files = self._getDataFiles(job)
		self.dbs.printInfoForJob(files)
		return "%d %d %s" % (files['events'], files['skip'], str.join(' ', files['files']))

		
	def getMaxJobs(self):
		self._ensureDataCache()
		return self.dbs.getNumberOfJobs()
