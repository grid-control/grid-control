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

import logging
from grid_control.job_db import Job
from grid_control.report import ConsoleReport
from grid_control.utils.activity import ProgressActivity


class BasicProgressBar(object):
	def __init__(self, value_min=0, value_max=100, total_width=16, value=0):
		(self._min, self._max, self._width) = (value_min, value_max, total_width)
		self.update(value)

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

	def __init__(self, config, name, job_db, task=None):
		ConsoleReport.__init__(self, config, name, job_db, task)
		self._bar = BasicProgressBar(0, len(job_db), 65)

	def show_report(self, job_db, jobnum_list):
		self._bar.update(self._get_job_state_dict(job_db, jobnum_list)[Job.SUCCESS])
		self._show_line(str(self._bar))


class ProgressBarActivity(ProgressActivity):
	def __init__(self, msg=None, progress_max=None, progress=None,
			level=logging.INFO, name=None, parent=None, width=16, fmt='%(progress_bar)s %(msg)s...'):
		self._width = width
		ProgressActivity.__init__(self, msg, progress_max, progress, level, name, parent, fmt)

	def update(self, msg):
		pbar = BasicProgressBar(0, self._progress_max or 1, self._width, self._progress or 0)
		self._set_msg(msg=msg, progress_str=self._get_progress_str() or '', progress_bar=str(pbar))
