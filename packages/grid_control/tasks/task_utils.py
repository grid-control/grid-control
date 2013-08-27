from grid_control import utils, noDefault

class TaskExecutableWrapper:
	def __init__(self, config, section, prefix = '', exeDefault = noDefault):
		self.prefix = prefix
		self.sendexec = config.getBool(section, '%s send executable' % prefix, True)
		if self.sendexec:
			self.executable = config.getPath(section, '%s executable' % prefix, exeDefault)
		else:
			self.executable = config.get(section, '%s executable' % prefix, exeDefault, noVar = False)
		self.arguments = config.get(section, '%s arguments' % prefix, '', noVar = False)

	def isActive(self):
		return self.executable

	def getCommand(self):
		if self.sendexec:
			cmd = os.path.basename(self._executable)
			return 'chmod u+x %s; ./%s $@' % (cmd, cmd)
		return '%s $@' % str.join('; ', self._executable.splitlines())

	def getArguments(self):
		return self.arguments

	def getSBInFiles(self):
		return QM(self.sendexec and self.executable, [self.executable], [])
