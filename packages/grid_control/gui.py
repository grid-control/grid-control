# | Copyright 2009-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

from grid_control import utils
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.report import Report
from grid_control.utils.parsing import strTimeShort
from hpfwk import AbstractError

class GUI(ConfigurablePlugin):
	def __init__(self, config, workflow):
		ConfigurablePlugin.__init__(self, config)
		self._workflow = workflow
		self._reportOpts = config.get('report options', '', onChange = None)
		self._report = config.getCompositePlugin('report', 'BasicReport', 'MultiReport',
			cls = Report, onChange = None, pargs = (workflow.jobManager.jobDB,
			workflow.task), pkwargs = {'configString': self._reportOpts})

	def displayWorkflow(self):
		raise AbstractError()


class SimpleConsole(GUI):
	def displayWorkflow(self):
		utils.vprint(level = -1)
		self._report.display()
		utils.vprint(level = -1)
		if self._workflow.duration < 0:
			utils.vprint('Running in continuous mode. Press ^C to exit.', -1)
		elif self._workflow.duration > 0:
			utils.vprint('Running for %s' % strTimeShort(self._workflow.duration), -1)
		self._workflow.jobCycle()
