# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

from grid_control.job_db import Job, JobClass
from grid_control.report import ConsoleReport, HeaderReport, MultiReport
from python_compat import izip, lfilter


class BasicReport(ConsoleReport):
	alias_list = ['basicreport']

	def __init__(self, config, name, job_db, task=None):
		ConsoleReport.__init__(self, config, name, job_db, task)
		self._total_succ_str = 'Total number of jobs:%9d     Successful jobs:%8d  %3d%%'
		self._atwms_fail_str = 'Jobs being processed:%9d        Failing jobs:%8d  %3d%%'
		self._detail_str = 'Detailed Status Information:    '
		self._js_str_dict = {}
		for (js_name, js_enum) in izip(Job.enum_name_list, Job.enum_value_list):
			self._js_str_dict[js_enum] = 'Jobs  %9s:%%8d  %%3d%%%%' % js_name
		self._height = 4 + int((len(Job.enum_name_list) + 1) / 2)

	def show_report(self, job_db, jobnum_list):
		js_dict = self._get_job_state_dict(job_db, jobnum_list)

		def _stat(state):
			return (js_dict[state], round(float(js_dict[state]) / max(1, js_dict[None]) * 100.0))

		# Print report summary
		self._show_line(self._total_succ_str, js_dict[None], *_stat(Job.SUCCESS))
		self._show_line(self._atwms_fail_str, js_dict[JobClass.PROCESSING], *_stat(JobClass.FAILING))
		ignored_str = ''
		if js_dict[Job.IGNORED]:
			ignored_str = '  (' + self._js_str_dict[Job.IGNORED] % _stat(Job.IGNORED) + ')'
		js_list = self._get_js_list(js_dict)
		if ignored_str or js_list:
			self._show_line(self._detail_str + ignored_str)
		output_str_list = []
		for js_enum in js_list:
			output_str = self._js_str_dict[js_enum] % _stat(js_enum)
			output_str_list.append(output_str)
			if len(output_str_list) == 2:
				self._show_line(str.join(' ' * 5, output_str_list))
				output_str_list = []
		if output_str_list:
			self._show_line(output_str_list[0])
		self._show_line('-' * 65)

	def _get_js_list(self, js_dict):
		return lfilter(lambda js_enum: js_enum != Job.IGNORED, Job.enum_value_list)


class BasicHeaderReport(HeaderReport):
	alias_list = ['basicheader']

	def show_report(self, job_db, jobnum_list):
		msg = 'REPORT SUMMARY:'
		self._show_line('-' * 65 + '\n%s%s\n' % (msg, self._header.rjust(65 - len(msg))) + '-' * 15)


class BasicTheme(MultiReport):
	alias_list = ['basic']

	def __init__(self, config, name, job_db, task=None):
		MultiReport.__init__(self, config, name, [
			BasicHeaderReport(config, name, job_db, task),
			BasicReport(config, name, job_db, task),
		], job_db, task)
