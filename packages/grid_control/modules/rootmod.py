import os
from grid_control import QM, utils, ConfigError
from usermod import UserMod

class ROOTMod(UserMod):
	def __init__(self, config):
		# Determine ROOT path from previous settings / environment / config file
		taskInfo = utils.PersistentDict(os.path.join(config.workDir, 'task.dat'), ' = ')
		self._rootpath = taskInfo.get('root path', os.environ.get('ROOTSYS', None))
		self._rootpath = config.get(self.__class__.__name__, 'root path', self._rootpath)
		if not self._rootpath:
			raise ConfigError('Either set environment variable "ROOTSYS" or set option "root path"!')
		utils.vprint('Using the following ROOT path: %s' % self._rootpath, -1)
		taskInfo.write({'root path': self._rootpath})

		# Special handling for executables bundled with ROOT
		exe = config.get(self.__class__.__name__, 'executable')
		exeFull = os.path.join(self._rootpath, 'bin', exe)
		self.builtIn = os.path.exists(exeFull)
		if self.builtIn:
			config.set(self.__class__.__name__, 'send executable', 'False')
			config.set(self.__class__.__name__, 'executable', exeFull)

		# Apply default handling from UserMod
		UserMod.__init__(self, config)
		self.updateErrorDict(utils.pathShare('gc-run.root.sh'))

		# Collect lib files needed by executable
		self.libFiles = []


	def getTaskConfig(self):
		return utils.mergeDicts([UserMod.getTaskConfig(self), {'MY_ROOTSYS': self._rootpath}])


	def getCommand(self):
		cmd = './gc-run.root.sh %s $@ > job.stdout 2> job.stderr' % self._executable
		return QM(self.builtIn, '', 'chmod u+x %s; ' % self._executable) + cmd


	def getSBInFiles(self):
		return UserMod.getSBInFiles(self) + self.libFiles + [utils.pathShare('gc-run.root.sh')]
