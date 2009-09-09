import os, string, re, sys, tarfile
from grid_control import ConfigError, Module, WMS, utils, DataMod
from time import time, localtime, strftime

class CMSSW(DataMod):
	def __init__(self, config):
		DataMod.__init__(self, config)

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

		# Get cmssw config files and check their existance
		self.configFiles = config.getPaths('CMSSW', 'config file')
		for cfgFile in self.configFiles:
			if not os.path.exists(cfgFile):
				raise ConfigError("Config file '%s' not found." % cfgFile)

		# Check that for dataset jobs the necessary placeholders are in the config file
		if self.dataSplitter != None:
			for tag in [ "__FILE_NAMES__", "__MAX_EVENTS__", "__SKIP_EVENTS__" ]:
				for cfgName in self.configFiles:
					if open(cfgName, 'r').read().find(tag) == -1:
						print open(utils.atRoot('share', 'fail.txt'), 'r').read()
						raise ConfigError("Config file must use __FILE_NAMES__, __MAX_EVENTS__" \
							" and __SKIP_EVENTS__ to work properly with datasets!")
		else:
			self.eventsPerJob = config.get('CMSSW', 'events per job', 0)

		self.gzipOut = config.getBool('CMSSW', 'gzip output', True)
		self.useReqs = config.getBool('CMSSW', 'use requirements', True, volatile=True)
		self.seRuntime = config.getBool('CMSSW', 'se runtime', False)

		if self.seRuntime and len(self.projectArea):
			self.seInputFiles.append(self.taskID + ".tar.gz"),

		if len(self.projectArea):
			self.pattern = config.get('CMSSW', 'area files', '-.* -config lib module */data *.xml *.sql *.cf[if] *.py').split()

			if os.path.exists(self.projectArea):
				print "Project area found in: %s" % self.projectArea
			else:
				raise ConfigError("Specified config area '%s' does not exist!" % self.projectArea)

			scramPath = os.path.join(self.projectArea, '.SCRAM')
			# try to open it
			try:
				fp = open(os.path.join(scramPath, 'Environment'), 'r')
				self.scramEnv = utils.DictFormat().parse(fp, lowerCaseKey = False)
			except:
				raise ConfigError("Project area file .SCRAM/Environment cannot be parsed!")

			for key in ['SCRAM_PROJECTNAME', 'SCRAM_PROJECTVERSION']:
				if key not in self.scramEnv:
					raise ConfigError("Installed program in project area can't be recognized.")

			archs = filter(lambda x: os.path.isdir(os.path.join(scramPath, x)), os.listdir(scramPath))
			self.scramArch = config.get('CMSSW', 'scram arch', archs[0])
			try:
				fp = open(os.path.join(scramPath, self.scramArch, 'Environment'), 'r')
				self.scramEnv.update(utils.DictFormat().parse(fp, lowerCaseKey = False))
			except:
				print "Project area file .SCRAM/%s/Environment cannot be parsed!" % self.scramArch
		else:
			self.scramEnv = {
				'SCRAM_PROJECTNAME': scramProject[0],
				'SCRAM_PROJECTVERSION': scramProject[1]
			}
			self.scramArch = config.get('CMSSW', 'scram arch')

		self.scramVersion = config.get('CMSSW', 'scram version', 'scramv1')
		if self.scramEnv['SCRAM_PROJECTNAME'] != 'CMSSW':
			raise ConfigError("Project area not a valid CMSSW project area.")

		if config.opts.init and len(self.projectArea):
			# Generate runtime tarball (and move to SE)
			utils.genTarball(os.path.join(config.workDir, 'runtime.tar.gz'), self.projectArea, self.pattern)

			if self.seRuntime:
				print 'Copy CMSSW runtime to SE',
				sys.stdout.flush()
				source = 'file:///' + os.path.join(config.workDir, 'runtime.tar.gz')
				target = os.path.join(self.sePath, self.taskID + '.tar.gz')
				if utils.se_copy(source, target, config.getBool('CMSSW', 'se runtime force', True)):
					print 'finished'
				else:
					print 'failed'
					print utils.se_copy.lastlog
					raise RuntimeError("Unable to copy runtime!")


	# Get default dataset provider
	def getDefaultProvider(self):
		return 'DBSApiv2'


	# Called on job submission
	def getSubmitInfo(self, jobNum):
		result = DataMod.getSubmitInfo(self, jobNum)
		result.update({"application": self.scramEnv['SCRAM_PROJECTVERSION'], "exe": "cmsRun"})
		if self.dataSplitter == None:
			result.update({"nevtJob": nEvents})
		return result


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		data = DataMod.getTaskConfig(self)
		data['CMSSW_CONFIG'] = str.join(' ', map(os.path.basename, self.configFiles))
		data['CMSSW_OLD_RELEASETOP'] = self.scramEnv.get('RELEASETOP', None)
		data['DB_EXEC'] = 'cmsRun'
		data['SCRAM_VERSION'] = self.scramVersion
		data['SCRAM_ARCH'] = self.scramArch
		data['SCRAM_PROJECTVERSION'] = self.scramEnv['SCRAM_PROJECTVERSION']
		data['GZIP_OUT'] = ('no', 'yes')[self.gzipOut]
		data['SE_RUNTIME'] = ('no', 'yes')[self.seRuntime]
		data['HAS_RUNTIME'] = ('no', 'yes')[len(self.projectArea) != 0]
		return data


	# Get job requirements
	def getRequirements(self, jobNum):
		reqs = DataMod.getRequirements(self, jobNum)
		if self.useReqs:
			reqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self.scramEnv['SCRAM_PROJECTVERSION']))
			reqs.append((WMS.SOFTWARE, 'VO-cms-%s' % self.scramArch))
		return reqs


	# Get files for input sandbox
	def getInFiles(self):
		files = DataMod.getInFiles(self)
		if len(self.projectArea) and not self.seRuntime:
			files.append(os.path.join(self.config.workDir, 'runtime.tar.gz'))
		files.append(utils.atRoot('share', 'run.cmssw.sh')),
		files.extend(self.configFiles)
		return files


	# Get files for output sandbox
	def getOutFiles(self):
		files = DataMod.getOutFiles(self)
		# Add framework report file
		renameExt = lambda name: str.join('.', name.split('.')[:-1]) + '.xml.gz'
		files.extend(map(renameExt, map(os.path.basename, self.configFiles)))
		if self.gzipOut:
			files.append('cmssw_out.txt.gz')
		return files


	def getCommand(self):
		return './run.cmssw.sh $@'


	def formatFileList(self, filelist):
		return str.join(', ', map(lambda x: '"%s"' % x, filelist))


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		data = DataMod.getJobConfig(self, jobNum)
		if self.dataSplitter == None:
			data['MAX_EVENTS'] = self.eventsPerJob
		return data


	def getDependencies(self):
		return DataMod.getDependencies(self) + ['cmssw']
