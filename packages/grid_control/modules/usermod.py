import os.path
from grid_control import QM, datasets
from datamod import DataMod

class UserMod(DataMod):
	def __init__(self, config):
		DataMod.__init__(self, config)
		self._sendexec = config.getBool(self.__class__.__name__, 'send executable', True)
		if self._sendexec:
			self._executable = config.getPath(self.__class__.__name__, 'executable')
		else:
			self._executable = config.get(self.__class__.__name__, 'executable', noVar = False)
		self._arguments = config.get(self.__class__.__name__, 'arguments', '', noVar = False)


	def getCommand(self):
		if self._sendexec:
			cmd = os.path.basename(self._executable)
			return 'chmod u+x %s; (./%s $@) > job.stdout 2> job.stderr' % (cmd, cmd)
		return '(%s $@) > job.stdout 2> job.stderr' % str.join('; ', self._executable.splitlines())


	def getJobArguments(self, jobNum):
		return DataMod.getJobArguments(self, jobNum) + ' ' + self._arguments


	def getSBInFiles(self):
		return DataMod.getSBInFiles(self) + QM(self._sendexec, [self._executable], [])


	def getSBOutFiles(self):
		tmp = map(lambda s: s + QM(self.gzipOut, '.gz', ''), ['job.stdout', 'job.stderr'])
		return DataMod.getSBOutFiles(self) + tmp
