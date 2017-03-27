# | Copyright 2017 Karlsruhe Institute of Technology
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

import time
from grid_control.job_db import Job, JobClass
from grid_control.report import ConsoleReport, HeaderReport, MultiReport
from grid_control_gui.ansi import ANSI
from grid_control_gui.report_bar import BasicProgressBar
from python_compat import irange, izip, lmap


class LeanReport(ConsoleReport):
	alias_list = ['leanreport']

	def __init__(self, config, name, job_db, task=None):
		ConsoleReport.__init__(self, config, name, job_db, task)
		(self._start_time, self._start_js_dict) = (None, None)

	def show_report(self, job_db, jobnum_list):
		js_dict = self._get_job_state_dict(job_db, jobnum_list)
		self._start_time = self._start_time or time.time()
		self._start_js_dict = self._start_js_dict or js_dict

		def _add_column(output_list, js_value, name, color, spacing=1):
			output_list.append((
				color + ANSI.bold + '%6d' % js_dict[js_value] + ANSI.reset,
				color + name + ANSI.reset,
				color + ANSI.bold + '%5.1f%%' % _make_per(Job.INIT) + ANSI.reset))
			if spacing > 0:
				output_list += [(' ' * spacing, '>', ' ' * spacing)]

		def _make_per(value):
			return js_dict[value] / float(js_dict[None]) * 100

		(dt_start, dt_div, dt_delim) = (time.time() - self._start_time, 60, ':')  # mmm:ss format
		if dt_start > 24 * 60 * 60:  # > 24 h
			(dt_start, dt_div, dt_delim) = (dt_start / (60 * 60), 24., 'd')  # DDDdhh format
		elif dt_start > 60 * 60:  # > 60 min
			(dt_start, dt_div, dt_delim) = (dt_start / 60, 60, 'h')  # HHHhmm format
		output_list = [(' ' * 6, '%3d%s%02d' % (dt_start / dt_div, dt_delim, dt_start % dt_div), ' ' * 6)]
		_add_column(output_list, Job.INIT, '  INIT', '')
		_add_column(output_list, JobClass.ATWMS, 'QUEUED', ANSI.color_yellow, spacing=2)
		_add_column(output_list, JobClass.ATWMS, 'RUNNING', ANSI.color_cyan)
		_add_column(output_list, JobClass.ATWMS, '  DONE', ANSI.color_blue, spacing=0)
		output_list += [('/', '|', '\\')]

		def _get_status_str(js_value, name, color):
			return color + name + ANSI.reset + ANSI.bold + color + ' %6d %5.1f%%' % (
				js_dict[js_value], _make_per(js_value)) + ANSI.reset
		output_list += [(_get_status_str(Job.SUCCESS, 'SUCCESS', ANSI.color_green),
			ANSI.back_white + ANSI.color_black +
			str(BasicProgressBar(0, js_dict[None], 21, js_dict[Job.SUCCESS])) + ANSI.reset,
			_get_status_str(JobClass.FAILING, 'FAILING', ANSI.color_red))]
		for entry in izip(*output_list):
			self._show_line(str.join(' ', entry))


class LeanHeaderReport(HeaderReport):  # pylint:disable=too-many-ancestors
	alias_list = ['leanheader']

	def __init__(self, config, name, job_db, task=None):
		HeaderReport.__init__(self, config, name, job_db, task)
		self._max_x = 65

	def show_report(self, job_db, jobnum_list):
		max_x = self._max_x - len(self._header) - 3
		self._show_line('> %s ' % self._header + str.join('', lmap(
			lambda x: ANSI.color_grayscale(1 - float(x) / max_x) + '-',
			irange(max_x))) + ANSI.reset)


class LeanTheme(MultiReport):
	alias_list = ['lean']

	def __init__(self, config, name, job_db, task=None):
		MultiReport.__init__(self, config, name, [
			LeanHeaderReport(config, name, job_db, task),
			LeanReport(config, name, job_db, task),
		], job_db, task)
