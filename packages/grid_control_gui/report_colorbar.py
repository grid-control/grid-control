# | Copyright 2016 Karlsruhe Institute of Technology
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

import sys
from grid_control.job_db import JobClass
from grid_control.job_selector import ClassSelector
from grid_control.report import Report
from grid_control_gui.ansi import Console

class JobProgressBar(object):
	def __init__(self, total = 100, width = 16, displayText = True, jobsOnFinish = False):
		(self._total, self._width) = (total, width)
		(self._displayText, self._jobsOnFinish) = (displayText, jobsOnFinish)
		self.update()

	def update(self, success = 0, queued = 0, running = 0, failed = 0):
		# Compute variables
		complete = self._width - 2 # outer markers
		if self._displayText:
			complete -= 24
		blocks_ok   = int(round(success / float(self._total) * complete))
		blocks_proc = int(round(queued / float(self._total) * complete))
		blocks_run  = int(round(running / float(self._total) * complete))
		blocks_fail = min(complete - blocks_ok - blocks_proc - blocks_run,
			int(round(failed / float(self._total) * complete)))
		self._bar  = str(Console.fmt('=' * blocks_ok, [Console.COLOR_GREEN]))
		self._bar += str(Console.fmt('=' * blocks_run, [Console.COLOR_BLUE]))
		self._bar += str(Console.fmt('=' * blocks_proc, [Console.COLOR_WHITE]))
		self._bar += ' ' * (complete - blocks_ok - blocks_proc - blocks_run - blocks_fail)
		self._bar += str(Console.fmt('=' * blocks_fail, [Console.COLOR_RED]))
		self._bar = '[%s]' % self._bar
		if self._displayText:
			if success == self._total and self._jobsOnFinish:
				self._bar += ' (%s |%s)' % (Console.fmt('%5d' % self._total, [Console.COLOR_GREEN]),
					Console.fmt('finished'.center(14), [Console.COLOR_GREEN]))
			elif success == self._total:
				self._bar += ' (%s)' % (Console.fmt('finished'.center(21), [Console.COLOR_GREEN]))
			else:
				fmt = lambda x: str(x).rjust(4)#int(math.log(self._total) / math.log(10)) + 1)
				self._bar += ' ( %s|%s|%s|%s )' % (
					Console.fmt(fmt(success), [Console.COLOR_GREEN]),
					Console.fmt(fmt(running), [Console.COLOR_BLUE]),
					Console.fmt(fmt(queued), [Console.COLOR_WHITE]),
					Console.fmt(fmt(failed), [Console.COLOR_RED]))

	def __len__(self):
		return self._width

	def __str__(self):
		return str(self._bar)


class ColorBarReport(Report):
	alias = ['cbar']

	def __init__(self, jobDB, task, jobs = None, configString = ''):
		Report.__init__(self, jobDB, task, jobs, configString)
		self._bar = JobProgressBar(len(jobDB), 65, jobsOnFinish = True)

	def getHeight(self):
		return 1

	def display(self):
		self._bar.update(
			len(self._jobDB.getJobs(ClassSelector(JobClass.SUCCESS))),
			len(self._jobDB.getJobs(ClassSelector(JobClass.ATWMS))),
			len(self._jobDB.getJobs(ClassSelector(JobClass.RUNNING_DONE))),
			len(self._jobDB.getJobs(ClassSelector(JobClass.FAILING))))
		sys.stdout.write(str(self._bar) + '\n')
