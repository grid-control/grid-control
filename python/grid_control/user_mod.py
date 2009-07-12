import os.path, random, time
from grid_control import Module

class UserMod(Module):
	def __init__(self, config, opts, proxy):
		Module.__init__(self, config, opts, proxy)
		self._executable = config.getPath('UserMod', 'executable')
		self._arguments = config.get('UserMod', 'arguments', '')


	def getCommand(self):
		cmd = os.path.basename(self._executable)
		return 'chmod u+x %s; ./%s "$@"' % (cmd, cmd)


	def getJobArguments(self, jobNum):
		args = self._arguments
		args.replace("__DATE__", time.strftime("%F"))
		args.replace("__TIMESTAMP__", time.strftime("%s"))
		args.replace("__RANDOM__", str(random.randrange(0, 900000000)))
		for key, value in self.getTaskConfig().items() + self.getJobConfig(jobNum).items():
			args.replace("__%s__" % key, str(value))
		return args


	def getInFiles(self):
		return Module.getInFiles(self) + [ self._executable ]


	def onJobSubmit(self, job, id, dbmessage = [{}]):
		Module.onJobSubmit(self, job, id, dbmessage + [dict.fromkeys(["application", "exe"], "shellscript")])
