# | Copyright 2007-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, random
from grid_control import utils
from grid_control.backends import WMS
from grid_control.config import ConfigError, changeInitNeeded, validNoVar
from grid_control.gc_plugin import ConfigurablePlugin, NamedPlugin
from grid_control.parameters import ParameterFactory, ParameterInfo
from grid_control.utils.file_objects import SafeFile
from grid_control.utils.gc_itertools import ichain, lchain
from hpfwk import AbstractError
from time import strftime, time
from python_compat import ifilter, imap, izip, lfilter, lmap, lru_cache, md5_hex

class JobNamePlugin(ConfigurablePlugin):
	def __init__(self, config, task):
		ConfigurablePlugin.__init__(self, config)
		self._task = task

	def getName(self, jobNum):
		raise AbstractError


class DefaultJobName(JobNamePlugin):
	alias = ['default']

	def getName(self, jobNum):
		return self._task.taskID[:10] + '.' + str(jobNum)


class ConfigurableJobName(JobNamePlugin):
	alias = ['config']

	def __init__(self, config, task):
		JobNamePlugin.__init__(self, config, task)
		self._name = config.get('job name', '@GC_TASK_ID@.@GC_JOB_ID@', onChange = None)

	def getName(self, jobNum):
		return self._task.substVars('job name', self._name, jobNum)


class TaskModule(NamedPlugin):
	configSections = NamedPlugin.configSections + ['task']
	tagName = 'task'

	# Read configuration options and init vars
	def __init__(self, config, name):
		NamedPlugin.__init__(self, config, name)
		initSandbox = changeInitNeeded('sandbox')
		self._varCheck = validNoVar(config)

		# Task requirements
		jobs_config = config.changeView(viewClass = 'TaggedConfigView', addSections = ['jobs'], addTags = [self]) # Move this into parameter manager?
		self.wallTime = jobs_config.getTime('wall time', onChange = None)
		self.cpuTime = jobs_config.getTime('cpu time', self.wallTime, onChange = None)
		self.cpus = jobs_config.getInt('cpus', 1, onChange = None)
		self.memory = jobs_config.getInt('memory', -1, onChange = None)
		self.nodeTimeout = jobs_config.getTime('node timeout', -1, onChange = initSandbox)

		# Compute / get task ID
		self.taskID = config.get('task id', 'GC' + md5_hex(str(time()))[:12], persistent = True)
		self.taskDate = config.get('task date', strftime('%Y-%m-%d'), persistent = True, onChange = initSandbox)
		self.taskConfigName = config.getConfigName()
		self._job_name_generator = config.getPlugin('job name generator', 'DefaultJobName',
			cls = JobNamePlugin, pargs = (self,))

		# Storage setup
		storage_config = config.changeView(viewClass = 'TaggedConfigView',
			setClasses = None, setNames = None, addSections = ['storage'], addTags = [self])
		self.taskVariables = {
			# Space limits
			'SCRATCH_UL': storage_config.getInt('scratch space used', 5000, onChange = initSandbox),
			'SCRATCH_LL': storage_config.getInt('scratch space left', 1, onChange = initSandbox),
			'LANDINGZONE_UL': storage_config.getInt('landing zone space used', 100, onChange = initSandbox),
			'LANDINGZONE_LL': storage_config.getInt('landing zone space left', 1, onChange = initSandbox),
		}
		storage_config.set('se output pattern', 'job_@GC_JOB_ID@_@X@')
		self.seMinSize = storage_config.getInt('se min size', -1, onChange = initSandbox)

		self.sbInputFiles = config.getPaths('input files', [], onChange = initSandbox)
		self.sbOutputFiles = config.getList('output files', [], onChange = initSandbox)
		self.gzipOut = config.getBool('gzip output', True, onChange = initSandbox)

		self.substFiles = config.getList('subst files', [], onChange = initSandbox)
		self.dependencies = lmap(str.lower, config.getList('depends', [], onChange = initSandbox))

		# Get error messages from gc-run.lib comments
		self.errorDict = {}
		self.updateErrorDict(utils.pathShare('gc-run.lib'))

		# Init parameter source manager
		self._pm = config.getPlugin('parameter factory', 'SimpleParameterFactory',
			cls = ParameterFactory, inherit = True)
		param_config = config.changeView(viewClass = 'TaggedConfigView', addSections = ['parameters'], addTags = [self])
		self.setupJobParameters(param_config, self._pm)
		self.source = self._pm.getSource(param_config)


	def setupJobParameters(self, config, pm):
		pass


	# Read comments with error codes at the beginning of file: # <code> - description
	def updateErrorDict(self, fileName):
		for line in ifilter(lambda x: x.startswith('#'), SafeFile(fileName).readlines()):
			tmp = lmap(str.strip, line.lstrip('#').split(' - ', 1))
			if tmp[0].isdigit() and (len(tmp) == 2):
				self.errorDict[int(tmp[0])] = tmp[1]


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		taskConfig = {
			# Storage element
			'SE_MINFILESIZE': self.seMinSize,
			# Sandbox
			'SB_OUTPUT_FILES': str.join(' ', self.getSBOutFiles()),
			'SB_INPUT_FILES': str.join(' ', imap(lambda x: x.pathRel, self.getSBInFiles())),
			# Runtime
			'GC_JOBTIMEOUT': self.nodeTimeout,
			'GC_RUNTIME': self.getCommand(),
			# Seeds and substitutions
			'SUBST_FILES': str.join(' ', imap(os.path.basename, self.getSubstFiles())),
			'GC_SUBST_OLD_STYLE': str('__' in self._varCheck.markers).lower(),
			# Task infos
			'GC_TASK_CONF': self.taskConfigName,
			'GC_TASK_DATE': self.taskDate,
			'GC_TASK_ID': self.taskID,
			'GC_VERSION': utils.getVersion(),
		}
		return utils.mergeDicts([taskConfig, self.taskVariables])
	getTaskConfig = lru_cache(getTaskConfig)


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		tmp = self.source.getJobInfo(jobNum)
		return dict(imap(lambda key: (str(key), tmp.get(key, '')), self.source.getJobKeys()))


	def getTransientVars(self):
		hx = str.join("", imap(lambda x: "%02x" % x, imap(random.randrange, [256]*16)))
		return {'GC_DATE': strftime("%F"), 'GC_TIMESTAMP': strftime("%s"),
			'GC_GUID': '%s-%s-%s-%s-%s' % (hx[:8], hx[8:12], hx[12:16], hx[16:20], hx[20:]),
			'RANDOM': str(random.randrange(0, 900000000))}


	def getVarNames(self):
		# Take task variables and the variables from the parameter source
		return lchain([self.getTaskConfig().keys(), self.source.getJobKeys()])


	def getVarMapping(self):
		# Transient variables
		transients = ['GC_DATE', 'GC_TIMESTAMP', 'GC_GUID'] # these variables are determined on the WN
		# Alias vars: Eg. __MY_JOB__ will access $GC_JOB_ID - used mostly for compatibility
		alias = {'DATE': 'GC_DATE', 'TIMESTAMP': 'GC_TIMESTAMP', 'GUID': 'GC_GUID',
			'MY_JOBID': 'GC_JOB_ID', 'MY_JOB': 'GC_JOB_ID', 'JOBID': 'GC_JOB_ID', 'GC_JOBID': 'GC_JOB_ID',
			'CONF': 'GC_CONF', 'TASK_ID': 'GC_TASK_ID'}
		varNames = self.getVarNames() + transients
		alias.update(dict(izip(varNames, varNames))) # include reflexive mappings
		return alias


	def substVars(self, name, inp, jobNum = None, addDict = None, check = True):
		addDict = addDict or {}
		allVars = utils.mergeDicts([addDict, self.getTaskConfig()])
		if jobNum is not None:
			allVars.update(self.getJobConfig(jobNum))
		subst = lambda x: utils.replaceDict(x, allVars, ichain([self.getVarMapping().items(), izip(addDict, addDict)]))
		result = subst(subst(str(inp)))
		if check and self._varCheck.check(result):
			raise ConfigError('%s references unknown variables: %s' % (name, result))
		return result


	def validateVariables(self):
		example_vars = dict.fromkeys(self.getVarNames(), '')
		example_vars.update(dict.fromkeys(['X', 'XBASE', 'XEXT', 'GC_DATE', 'GC_TIMESTAMP', 'GC_GUID', 'RANDOM'], ''))
		for name, value in ichain([self.getTaskConfig().items(), example_vars.items()]):
			self.substVars(name, value, None, example_vars)


	# Get job requirements
	def getRequirements(self, jobNum):
		return [
			(WMS.WALLTIME, self.wallTime),
			(WMS.CPUTIME, self.cpuTime),
			(WMS.MEMORY, self.memory),
			(WMS.CPUS, self.cpus)
		] + self.source.getJobInfo(jobNum)[ParameterInfo.REQS]


	def getSEInFiles(self):
		return []


	# Get files for input sandbox
	def getSBInFiles(self):
		return lmap(lambda fn: utils.Result(pathAbs = fn, pathRel = os.path.basename(fn)), self.sbInputFiles)


	# Get files for output sandbox
	def getSBOutFiles(self):
		return list(self.sbOutputFiles)


	# Get files whose content will be subject to variable substitution
	def getSubstFiles(self):
		return list(self.substFiles)


	def getCommand(self):
		raise AbstractError


	def getJobArguments(self, jobNum):
		return ''


	def getMaxJobs(self):
		return self.source.getMaxJobs()


	def getDependencies(self):
		return list(self.dependencies)


	def getDescription(self, jobNum): # (task name, job name, job type)
		return utils.Result(taskName = self.taskID, jobType = None,
			jobName = self._job_name_generator.getName(jobNum))


	def report(self, jobNum):
		keys = lfilter(lambda k: not k.untracked, self.source.getJobKeys())
		return utils.filterDict(self.source.getJobInfo(jobNum), kF = lambda k: k in keys)


	def canFinish(self):
		return True


	def canSubmit(self, jobNum):
		return self.source.canSubmit(jobNum)


	# Called on job submission
	def getSubmitInfo(self, jobNum):
		return {}


	# Intervene in job management - return None or (redoJobs, disableJobs)
	def getIntervention(self):
		return self.source.resync()
