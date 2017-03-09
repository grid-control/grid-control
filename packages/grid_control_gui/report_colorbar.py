# | Copyright 2016-2017 Karlsruhe Institute of Technology
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

from grid_control.job_db import JobClass
from grid_control.job_selector import ClassSelector
from grid_control.report import ConsoleReport
from grid_control_gui.ansi import Console


class JobProgressBar(object):
	def __init__(self, total=100, width=16, display_text=True, jobs_on_finish=False):
		(self._total, self._width) = (total, width)
		(self._display_text, self._jobs_on_finish) = (display_text, jobs_on_finish)
		self.update()

	def __len__(self):
		return self._width

	def __str__(self):
		return str(self._bar)

	def update(self, success=0, queued=0, running=0, failed=0):
		# Compute variables
		complete = self._width - 2  # outer markers
		if self._display_text:
			complete -= 24
		blocks_ok = int(round(success / float(self._total) * complete))
		blocks_proc = int(round(queued / float(self._total) * complete))
		blocks_run = int(round(running / float(self._total) * complete))
		blocks_fail = min(complete - blocks_ok - blocks_proc - blocks_run,
			int(round(failed / float(self._total) * complete)))
		self._bar = Console.COLOR_GREEN + '=' * blocks_ok
		self._bar += Console.COLOR_BLUE + '=' * blocks_run
		self._bar += Console.COLOR_WHITE + '=' * blocks_proc
		self._bar += ' ' * (complete - blocks_ok - blocks_proc - blocks_run - blocks_fail)
		self._bar += Console.COLOR_RED + '=' * blocks_fail
		self._bar = '[' + Console.RESET + self._bar + Console.RESET + ']'
		if self._display_text:
			if success == self._total and self._jobs_on_finish:
				self._bar += ' (%s |%s)' % (Console.fmt('%5d' % self._total, [Console.COLOR_GREEN]),
					Console.fmt('finished'.center(14), [Console.COLOR_GREEN]))
			elif success == self._total:
				self._bar += ' (%s)' % (Console.fmt('finished'.center(21), [Console.COLOR_GREEN]))
			else:
				def _fmt(value):
					return str(value).rjust(4)  # int(math.log(self._total) / math.log(10)) + 1)
				self._bar += ' ( %s|%s|%s|%s )' % (
					Console.fmt(_fmt(success), [Console.COLOR_GREEN]),
					Console.fmt(_fmt(running), [Console.COLOR_BLUE]),
					Console.fmt(_fmt(queued), [Console.COLOR_WHITE]),
					Console.fmt(_fmt(failed), [Console.COLOR_RED]))


class ColorBarReport(ConsoleReport):
	alias_list = ['cbar']

	def __init__(self, job_db, task, jobs=None, config_str=''):
		ConsoleReport.__init__(self, job_db, task, jobs, config_str)
		self._bar = JobProgressBar(len(job_db), 65, jobs_on_finish=True)

	def get_height(self):
		return 1

	def show_report(self, job_db):
		self._bar.update(
			len(job_db.get_job_list(ClassSelector(JobClass.SUCCESS))),
			len(job_db.get_job_list(ClassSelector(JobClass.ATWMS))),
			len(job_db.get_job_list(ClassSelector(JobClass.RUNNING_DONE))),
			len(job_db.get_job_list(ClassSelector(JobClass.FAILING))))
		self._output(str(self._bar))
