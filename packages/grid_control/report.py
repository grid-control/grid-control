# | Copyright 2007-2016 Karlsruhe Institute of Technology
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

import sys, logging
from grid_control import utils
from grid_control.job_db import Job
from hpfwk import AbstractError, Plugin
from python_compat import imap, irange, izip, lzip

class Report(Plugin):
	def __init__(self, jobDB = None, task = None, jobs = None, configString = ''):
		if jobs is None:
			jobs = jobDB.getJobs()
		(self._jobDB, self._task, self._jobs) = (jobDB, task, jobs)
		# FIXME: really store task for later access? maybe just use task during init run?
		self._header = self._getHeader(45)
		self._log = logging.getLogger('report')

	def _getHeader(self, maxLen = 45):
		if not self._task or not self._task.taskConfigName:
			return ''
		tmp = self._task.taskConfigName + ' / ' + self._task.taskID
		if len(tmp) < maxLen:
			return tmp
		tmp = self._task.taskConfigName
		if len(tmp) < maxLen:
			return tmp
		return self._task.taskID

	def getHeight(self):
		return 0

	def display(self):
		raise AbstractError


class NullReport(Report):
	alias = ['null']

	def display(self):
		pass


class MultiReport(Report):
	def __init__(self, reportList, *args, **kwargs):
		self._reportList = reportList

	def getHeight(self):
		return sum(imap(lambda r: r.getHeight(), self._reportList))

	def display(self):
		for report in self._reportList:
			report.display()


class BasicReport(Report):
	alias = ['basic']

	def _write_line(self, content, width = 65, newline = True):
		content = content.ljust(width)
		if newline:
			content += '\n'
		sys.stdout.write(content)
		sys.stdout.flush()

	def _printHeader(self, message, level = -1, width = 65):
		self._write_line('-' * width, width)
		self._write_line(message + self._header.rjust(width - len(message)), width)
		self._write_line('-' * 15, width)

	def getHeight(self):
		return 8 + int((len(Job.enumNames) + 1) / 2)

	def display(self):
		njobs_total = len(self._jobDB)
		summary = dict(imap(lambda x: (x, 0.0), Job.enumValues))
		for jobNum in self._jobs:
			summary[self._jobDB.getJobTransient(jobNum).state] += 1
		makeSum = lambda *states: sum(imap(lambda z: summary[z], states))
		makePer = lambda *states: [makeSum(*states), round(makeSum(*states) / max(1, njobs_total) * 100.0)]

		# Print report summary
		self._printHeader('REPORT SUMMARY:')
		jobov_succ = makePer(Job.SUCCESS)
		self._write_line('Total number of jobs:%9d     Successful jobs:%8d  %3d%%' % tuple([njobs_total] + jobov_succ))
		njobs_assigned = makeSum(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING)
		jobov_fail = makePer(Job.ABORTED, Job.CANCELLED, Job.FAILED)
		self._write_line('Jobs assigned to WMS:%9d        Failing jobs:%8d  %3d%%' % tuple([njobs_assigned] + jobov_fail))
		self._write_line('')
		ignored = njobs_total - sum(summary.values())
		ignored_str = ''
		if ignored:
			ignored_str = '(Jobs    IGNORED:%8d  %3d%%)' % (ignored, ignored / max(1, njobs_total) * 100.0)
		self._write_line('Detailed Status Information:      ' + ignored_str)
		tmp = []
		for (sid, sname) in izip(Job.enumValues, Job.enumNames):
			tmp.append('Jobs  %9s:%8d  %3d%%' % tuple([sname] + makePer(sid)))
			if len(tmp) == 2:
				self._write_line(str.join('     ', tmp))
				tmp = []
		if tmp:
			self._write_line(tmp[0])
		self._write_line('-' * 65)


class LocationReport(Report):
	alias = ['location']

	def _add_details(self, reports, jobObj):
		if jobObj.get('dest', 'N/A') != 'N/A':
			reports.append({2: ' -> ' + jobObj.get('dest')})

	def display(self):
		reports = []
		for jobNum in self._jobs:
			jobObj = self._jobDB.getJob(jobNum)
			if not jobObj or (jobObj.state == Job.INIT):
				continue
			reports.append({0: jobNum, 1: Job.enum2str(jobObj.state), 2: jobObj.gcID})
			self._add_details(reports, jobObj)
		utils.printTabular(lzip(irange(3), ['Job', 'Status / Attempt', 'Id / Destination']), reports, 'rcl')
