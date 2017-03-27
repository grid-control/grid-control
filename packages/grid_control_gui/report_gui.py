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
from grid_control_gui.ansi import ANSI, Console
from grid_control_gui.cat_manager import AdaptiveJobCategoryManager
from grid_control_gui.report_basic import BasicReport
from grid_control_gui.report_colorbar import ColorBarReport, JobProgressBar
from python_compat import imap, irange, lfilter, lmap


class ANSIReport(BasicReport):
	alias_list = ['ansireport']

	def __init__(self, config, name, job_db=None, task=None):
		BasicReport.__init__(self, config, name, job_db, task)
		self._total_succ_str = 'Total number of jobs:' + ANSI.bold + '%9d     ' + ANSI.reset + \
			'Successful jobs:' + ANSI.bold + ANSI.color_green + '%8d  %3d%%' + ANSI.reset
		self._atwms_fail_str = 'Jobs being processed:%9d        Failing jobs:' + \
			ANSI.bold + ANSI.color_red + '%8d  %3d%%' + ANSI.reset
		self._detail_str = ' ' * len(self._detail_str)
		js_color_dict = {Job.SUCCESS: ANSI.color_green, Job.DONE: ANSI.color_cyan,
			Job.FAILED: ANSI.color_red, Job.RUNNING: ANSI.color_blue}
		for (js_enum, js_color) in js_color_dict.items():
			self._js_str_dict[js_enum] = self._js_str_dict[js_enum].replace(
				':', ':' + ANSI.bold + js_color) + ANSI.reset
		self._last_js_list = []

	def _get_js_list(self, js_dict):
		old_js_list = self._last_js_list
		self._last_js_list = []
		js_blacklist = (Job.IGNORED, Job.SUCCESS, Job.FAILED)

		def _check_js_enum(js_enum):
			return js_dict[js_enum] and not ((js_enum in js_blacklist) or (js_enum in self._last_js_list))
		js_list = lfilter(lambda js_enum: _check_js_enum(js_enum) and (js_enum not in old_js_list),
			Job.enum_value_list)
		for js_enum in old_js_list:
			if _check_js_enum(js_enum):
				self._last_js_list.append(js_enum)
			elif js_list:
				self._last_js_list.append(js_list.pop())
		self._last_js_list.extend(js_list)
		return self._last_js_list


class ModernReport(ConsoleReport):
	alias_list = ['modern']

	def __init__(self, config, name, job_db, task=None):
		ConsoleReport.__init__(self, config, name, job_db, task)
		(max_y, self._max_x) = Console.getmaxyx()
		cat_max = config.get_int('report categories max', int(max_y / 5), on_change=None)
		self._cat_manager = AdaptiveJobCategoryManager(config, job_db, task, cat_max)
		self._cat_cur = cat_max

	def show_report(self, job_db, jobnum_list):
		self._max_x = Console.getmaxyx()[1]
		(cat_state_dict, map_cat2desc, cat_subcat_dict) = self._cat_manager.get_category_infos(
			job_db, jobnum_list)
		self._cat_cur = len(cat_state_dict)

		def _sum_cat(cat_key, states):
			return sum(imap(lambda z: cat_state_dict[cat_key].get(z, 0), states))

		for cat_key in cat_state_dict:  # Sorted(cat_state_dict, key=lambda x: -self._categories[x][0]):
			desc = self._cat_manager.format_desc(map_cat2desc[cat_key], cat_subcat_dict.get(cat_key, 0))
			completed = _sum_cat(cat_key, [Job.SUCCESS])
			total = sum(cat_state_dict[cat_key].values())
			job_info_str = ''
			if self._max_x > 65 + 23:
				job_info_str = ' (%5d jobs, %6.2f%%  )' % (total, 100 * completed / float(total))
			width = min(65 + len(job_info_str), self._max_x)
			if len(desc) + len(job_info_str) > width:
				desc = desc[:width - 3 - len(job_info_str)] + '...'
			self._show_line(ANSI.bold + desc + ANSI.reset +
				' ' * (width - (len(desc) + len(job_info_str))) + job_info_str)
			progressbar = JobProgressBar(sum(cat_state_dict[cat_key].values()),
				width=width, display_text=(job_info_str != ''))
			progressbar.update(completed, _sum_cat(cat_key, JobClass.ATWMS.state_list),
				_sum_cat(cat_key, [Job.RUNNING]), _sum_cat(cat_key, [Job.DONE]),
				_sum_cat(cat_key, [Job.ABORTED, Job.CANCELLED, Job.FAILED]))
			self._show_line(str(progressbar))


class ANSIHeaderReport(HeaderReport):
	alias_list = ['ansiheader']

	def __init__(self, config, name, job_db, task=None):
		HeaderReport.__init__(self, config, name, job_db, task)
		self._max_x = 65

	def show_report(self, job_db, jobnum_list):
		self._print_gui_header('Status report for task:')

	def _print_gui_header(self, message):
		border = lmap(lambda x: ANSI.color_grayscale(1 - float(x) / self._max_x) + '-',
			irange(self._max_x))
		self._show_line(str.join('', border) + ANSI.reset)
		self._show_line(message + self._header.rjust(self._max_x - len(message)))
		self._show_line(str.join('', border) + ANSI.reset)


class ANSITheme(MultiReport):
	alias_list = ['ansi']

	def __init__(self, config, name, job_db, task=None):
		MultiReport.__init__(self, config, name, [
			ANSIHeaderReport(config, name, job_db, task),
			ANSIReport(config, name, job_db, task),
			ColorBarReport(config, name, job_db, task),
		], job_db, task)
