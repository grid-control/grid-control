from grid_control import LoadableObject, AbstractError

class GUI(LoadableObject):
	def __init__(self, jobCycle, jobMgr, task):
		(self.jobCycle, self.jobMgr, self.task) = (jobCycle, jobMgr, task)

	def run(self):
		raise AbstractError()
GUI.registerObject()


class SimpleConsole(GUI):
	def run(self):
		self.jobCycle()
