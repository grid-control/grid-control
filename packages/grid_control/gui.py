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
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.report import Report
from grid_control.utils.parsing import str_time_short
from hpfwk import AbstractError


class GUI(ConfigurablePlugin):
	config_section_list = ['gui']

	def __init__(self, config, workflow):
		ConfigurablePlugin.__init__(self, config)
		self._workflow = workflow

	def start_display(self):
		raise AbstractError


class NullGUI(GUI):
	def start_display(self):
		return self._workflow.process()


class SimpleConsole(GUI):
	def __init__(self, config, workflow):
		GUI.__init__(self, config, workflow)
		self._log = logging.getLogger('workflow')
		report_config_str = config.get('report options', '', on_change=None)
		self._report = config.get_composited_plugin('report', 'BasicHeaderReport BasicReport',
			'MultiReport', cls=Report, on_change=None, pargs=(workflow.job_manager.job_db,
			workflow.task), pkwargs={'config_str': report_config_str})

	def start_display(self):
		if self._report.get_height():
			self._log.info('')
		self._report.show_report(self._workflow.job_manager.job_db)
		if self._report.get_height():
			self._log.info('')
		if self._workflow.duration < 0:
			self._log.info('Running in continuous mode. Press ^C to exit.')
		elif self._workflow.duration > 0:
			self._log.info('Running for %s', str_time_short(self._workflow.duration))
		return self._workflow.process()
