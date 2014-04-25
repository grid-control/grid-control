from grid_control import LoadableObject, AbstractError, Report, utils

class GUI(LoadableObject):
	def __init__(self, config, workflow):
		self._workflow = workflow
		self._reportClass = config.getClass('report', 'BasicReport', cls = Report, onChange = None)
		self._reportOpts = config.get('report options', '', onChange = None)

	def displayWorkflow(self):
		raise AbstractError()
GUI.registerObject()


class SimpleConsole(GUI):
	def __init__(self, config, workflow):
		GUI.__init__(self, config, workflow)
		self._report = self._reportClass.getInstance(self._workflow.jobManager.jobDB, configString = self._reportOpts)

	def displayWorkflow(self):
		if self._workflow.runContinuous:
			utils.vprint(level = -1)
			self._report.display()
			utils.vprint('Running in continuous mode. Press ^C to exit.', -1)
		self._workflow.jobCycle()
