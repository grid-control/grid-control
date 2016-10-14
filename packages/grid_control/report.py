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
	def __init__(self, job_db=None, task=None, jobs=None, config_str=''):
		if jobs is None:
			jobs = job_db.get_job_list()
		(self._jobs, self._task_id, self._task_name) = (jobs, '', '')
		if task is not None:
			(self._task_id, self._task_name) = (task.task_id, task.task_config_name)
		# FIXME: really store task for later access? maybe just use task during init run?
		self._header = self._get_header(45)
		self._log = logging.getLogger('report')

	def get_height(self):
		return 0

	def show_report(self, job_db):
		raise AbstractError

	def _get_header(self, max_len=45):
		tmp = self._task_name + ' / ' + self._task_id
		if self._task_id and self._task_name and (len(tmp) < max_len):
			return tmp
		elif self._task_name and (len(self._task_name) < max_len):
			return self._task_name
		elif self._task_id:
			return self._task_id
		return ''


class BasicReport(Report):
	alias_list = ['basic']

	def get_height(self):
		return 8 + int((len(Job.enum_name_list) + 1) / 2)

	def show_report(self, job_db):
		njobs_total = len(job_db)
		summary = dict(imap(lambda x: (x, 0.0), Job.enum_value_list))
		for jobnum in self._jobs:
			summary[job_db.get_job_transient(jobnum).state] += 1

		def _make_per(*states):
			return [_make_sum(*states), round(_make_sum(*states) / max(1, njobs_total) * 100.0)]

		def _make_sum(*states):
			return sum(imap(lambda z: summary[z], states))

		# Print report summary
		self._print_header('REPORT SUMMARY:')
		jobov_succ = _make_per(Job.SUCCESS)
		self._write_line(('Total number of jobs:%9d     ' +
			'Successful jobs:%8d  %3d%%') % tuple([njobs_total] + jobov_succ))
		njobs_assigned = _make_sum(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING)
		jobov_fail = _make_per(Job.ABORTED, Job.CANCELLED, Job.FAILED)
		self._write_line(('Jobs assigned to WMS:%9d        ' +
			'Failing jobs:%8d  %3d%%') % tuple([njobs_assigned] + jobov_fail))
		self._write_line('')
		ignored = njobs_total - sum(summary.values())
		ignored_str = ''
		if ignored:
			ignored_str = '(Jobs    IGNORED:%8d  %3d%%)' % (ignored, ignored / max(1, njobs_total) * 100.0)
		self._write_line('Detailed Status Information:      ' + ignored_str)
		tmp = []
		for (sid, sname) in izip(Job.enum_value_list, Job.enum_name_list):
			tmp.append('Jobs  %9s:%8d  %3d%%' % tuple([sname] + _make_per(sid)))
			if len(tmp) == 2:
				self._write_line(str.join('     ', tmp))
				tmp = []
		if tmp:
			self._write_line(tmp[0])
		self._write_line('-' * 65)

	def _print_header(self, message, level=-1, width=65):
		self._write_line('-' * width, width)
		self._write_line(message + self._header.rjust(width - len(message)), width)
		self._write_line('-' * 15, width)

	def _write_line(self, content, width=65, newline=True):
		content = content.ljust(width)
		if newline:
			content += '\n'
		sys.stdout.write(content)
		sys.stdout.flush()


class LocationReport(Report):
	alias_list = ['location']

	def show_report(self, job_db):
		reports = []
		for jobnum in self._jobs:
			job_obj = job_db.get_job(jobnum)
			if not job_obj or (job_obj.state == Job.INIT):
				continue
			reports.append({0: jobnum, 1: Job.enum2str(job_obj.state), 2: job_obj.gc_id})
			self._add_details(reports, job_obj)
		header_list = ['Job', 'Status / Attempt', 'Id / Destination']
		utils.display_table(lzip(irange(3), header_list), reports, 'rcl')

	def _add_details(self, reports, job_obj):
		if job_obj.get('dest', 'N/A') != 'N/A':
			reports.append({2: ' -> ' + job_obj.get('dest')})


class MultiReport(Report):
	def __init__(self, report_list, *args, **kwargs):  # pylint:disable=super-init-not-called
		self._report_list = report_list

	def get_height(self):
		return sum(imap(lambda r: r.get_height(), self._report_list))

	def show_report(self, job_db):
		for report in self._report_list:
			report.show_report(job_db)


class NullReport(Report):
	alias_list = ['null']

	def show_report(self, job_db):
		pass
