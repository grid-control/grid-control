from grid_control import AbstractObject, AbstractError

class GUI(AbstractObject):
	def __init__(self, jobCycle, jobMgr, module):
		(self.jobCycle, self.jobMgr, self.module) = (jobCycle, jobMgr, module)

	def run(self):
		raise AbstractError()
GUI.dynamicLoaderPath()


class SimpleConsole(GUI):
	def run(self):
		self.jobCycle()
