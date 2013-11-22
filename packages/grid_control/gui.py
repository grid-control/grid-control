from grid_control import AbstractObject, AbstractError

class GUI(AbstractObject):
	def __init__(self, jobCycle, jobMgr, task):
		(self.jobCycle, self.jobMgr, self.task) = (jobCycle, jobMgr, task)

	def run(self):
		raise AbstractError()
GUI.registerObject()


class SimpleConsole(GUI):
	def run(self):
		self.jobCycle()
