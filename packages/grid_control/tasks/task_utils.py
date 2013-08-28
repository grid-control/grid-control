import os
from grid_control import QM, utils, noDefault

class TaskExecutableWrapper:
	def __init__(self, config, section, prefix = '', exeDefault = noDefault):
		self._executableSend = config.getBool(section, '%s send executable' % prefix, True)
		if self._executableSend:
			self._executable = config.getPath(section, '%s executable' % prefix, exeDefault)
		else:
			self._executable = config.get(section, '%s executable' % prefix, exeDefault, noVar = False)
		self._arguments = config.get(section, '%s arguments' % prefix, '', noVar = False)


	def isActive(self):
		return self._executable != ''


	def getCommand(self):
		if self._executableSend:
			cmd = os.path.basename(self._executable)
			return 'chmod u+x %s; ./%s $@' % (cmd, cmd)
		return '%s $@' % str.join('; ', self._executable.splitlines())


	def getArguments(self):
		return self._arguments


	def getSBInFiles(self):
		return QM(self._executableSend and self._executable, [self._executable], [])
