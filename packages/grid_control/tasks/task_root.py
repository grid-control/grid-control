import os
from grid_control import QM, utils, ConfigError
from task_user import UserTask

class ROOTTask(UserTask):
	def __init__(self, config, name):
		# Determine ROOT path from previous settings / environment / config file
		taskInfo = utils.PersistentDict(os.path.join(config.workDir, 'task.dat'), ' = ')
		self._rootpath = config.get(self.__class__.__name__, 'root path',
			os.environ.get('ROOTSYS', ''), persistent = True)
		if not self._rootpath:
			raise ConfigError('Either set environment variable "ROOTSYS" or set option "root path"!')
		utils.vprint('Using the following ROOT path: %s' % self._rootpath, -1)
		taskInfo.write({'root path': self._rootpath})

		# Special handling for executables bundled with ROOT
		self._executable = config.get(self.__class__.__name__, 'executable')
		exeFull = os.path.join(self._rootpath, 'bin', self._executable)
		self.builtIn = os.path.exists(exeFull)
		if self.builtIn:
			config.set(self.__class__.__name__, 'send executable', 'False')
			# store resolved built-in executable path?

		# Apply default handling from UserTask
		UserTask.__init__(self, config, name)
		self.updateErrorDict(utils.pathShare('gc-run.root.sh'))

		# Collect lib files needed by executable
		self.libFiles = []


	def getTaskConfig(self):
		return utils.mergeDicts([UserTask.getTaskConfig(self), {'MY_ROOTSYS': self._rootpath}])


	def getCommand(self):
		cmd = './gc-run.root.sh %s $@ > job.stdout 2> job.stderr' % self._executable
		return QM(self.builtIn, '', 'chmod u+x %s; ' % self._executable) + cmd


	def getSBInFiles(self):
		return UserTask.getSBInFiles(self) + self.libFiles + [utils.pathShare('gc-run.root.sh')]


class ROOTMod(ROOTTask):
	pass
