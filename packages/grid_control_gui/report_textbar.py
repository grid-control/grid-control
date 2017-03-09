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

from grid_control.job_db import JobClass
from grid_control.job_selector import ClassSelector
from grid_control.report import ConsoleReport


class BasicProgressBar(object):
	def __init__(self, value_min=0, value_max=100, total_width=16):
		(self._min, self._max, self._width) = (value_min, value_max, total_width)
		self.update(0)

	def __str__(self):
		return str(self._bar)

	def update(self, progress_new=0):
		# Compute variables
		complete = self._width - 2
		progress = max(self._min, min(self._max, progress_new))
		done = int(round(((progress - self._min) / max(1.0, float(self._max - self._min))) * 100.0))
		blocks = int(round((done / 100.0) * complete))

		# Build progress bar
		if blocks == 0:
			self._bar = '[>%s]' % (' ' * (complete - 1))
		elif blocks == complete:
			self._bar = '[%s]' % ('=' * complete)
		else:
			self._bar = '[%s>%s]' % ('=' * (blocks - 1), ' ' * (complete - blocks))

		# Print percentage
		text = str(done) + '%'
		text_pos = int((self._width - len(text) + 1) / 2)
		self._bar = self._bar[0:text_pos] + text + self._bar[text_pos + len(text):]


class BarReport(ConsoleReport):
	alias_list = ['bar']

	def __init__(self, job_db, task, jobs=None, config_str=''):
		ConsoleReport.__init__(self, job_db, task, jobs, config_str)
		self._bar = BasicProgressBar(0, len(job_db), 65)

	def get_height(self):
		return 1

	def show_report(self, job_db):
		self._bar.update(len(job_db.get_job_list(ClassSelector(JobClass.SUCCESS))))
		self._output.info(str(self._bar))
