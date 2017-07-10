# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import logging
from grid_control.gui import GUI
from grid_control.report import Report


class BasicConsoleGUI(GUI):
	alias_list = ['console']

	def __init__(self, config, workflow):
		GUI.__init__(self, config, workflow)
		self._log = logging.getLogger('workflow')
		self._workflow = workflow
		self._report = config.get_composited_plugin('report', 'BasicTheme', 'MultiReport',
			cls=Report, on_change=None, pargs=(workflow.job_manager.job_db, workflow.task))

	def start_interface(self):
		self._log.info('')
		self._report.show_report(self._workflow.job_manager.job_db,
			self._workflow.job_manager.job_db.get_job_list())
		self._log.info('')
