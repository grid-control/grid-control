# | Copyright 2007-2017 Karlsruhe Institute of Technology
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
from grid_control.utils.table import ConsoleTable
from hpfwk import AbstractError, Plugin
from python_compat import imap, irange, izip, lzip


class Report(Plugin):
	def __init__(self, job_db=None, task=None, jobs=None, config_str=''):
		if jobs is None:
			jobs = job_db.get_job_list()
		(self._log, self._jobs) = (logging.getLogger('report'), jobs)

	def get_height(self):
		return 0

	def show_report(self, job_db):
		raise AbstractError


class ConsoleReport(Report):
	def __init__(self, job_db=None, task=None, jobs=None, config_str=''):
		Report.__init__(self, job_db, task, jobs, config_str)
		self._output = logging.getLogger('console.report')


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
		ConsoleTable.create(lzip(irange(3), header_list), reports, 'rcl')

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


class BasicReport(ConsoleReport):
	alias_list = ['basic']

	def get_height(self):
		return 4 + int((len(Job.enum_name_list) + 1) / 2)

	def show_report(self, job_db):
		njobs_total = len(job_db)
		summary = dict(imap(lambda x: (x, 0.0), Job.enum_value_list))
		for jobnum in self._jobs:
			summary[job_db.get_job_transient(jobnum).state] += 1

		def _make_per(*state_list):
			return [_make_sum(*state_list), round(_make_sum(*state_list) / max(1, njobs_total) * 100.0)]

		def _make_sum(*state_list):
			return sum(imap(lambda z: summary[z], state_list))

		# Print report summary
		self._output.info('Total number of jobs:%9d     Successful jobs:%8d  %3d%%',
			*([njobs_total] + _make_per(Job.SUCCESS)))
		njobs_assigned = _make_sum(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING)
		self._output.info('Jobs assigned to WMS:%9d        Failing jobs:%8d  %3d%%',
			*([njobs_assigned] + _make_per(Job.ABORTED, Job.CANCELLED, Job.FAILED)))
		ignored = njobs_total - sum(summary.values())
		ignored_str = ''
		if ignored:
			ignored_str = '(Jobs    IGNORED:%8d  %3d%%)' % (ignored, ignored / max(1, njobs_total) * 100.0)
		self._output.info('Detailed Status Information:      ' + ignored_str)
		output_str_list = []
		for (sid, sname) in izip(Job.enum_value_list, Job.enum_name_list):
			output_str_list.append('Jobs  %9s:%8d  %3d%%' % tuple([sname] + _make_per(sid)))
			if len(output_str_list) == 2:
				self._output.info(str.join('     ', output_str_list))
				output_str_list = []
		if output_str_list:
			self._output.info(output_str_list)
		self._output.info('-' * 65)


class BasicHeaderReport(ConsoleReport):
	alias_list = ['basicheader']

	def __init__(self, job_db=None, task=None, jobs=None, config_str=''):
		ConsoleReport.__init__(self, job_db, task, jobs, config_str)
		(self._task_id, self._task_name) = ('', '')
		if task is not None:
			(self._task_id, self._task_name) = (task.task_id, task.task_config_name)
		self._header = self._get_header(width=45)

	def get_height(self):
		return 3

	def show_report(self, job_db):
		msg = 'REPORT SUMMARY:'
		self._output.info('-' * 65 + '\n%s%s\n' % (msg, self._header.rjust(65 - len(msg))) + '-' * 15)

	def _get_header(self, width=45):
		tmp = self._task_name + ' / ' + self._task_id
		if self._task_id and self._task_name and (len(tmp) < width):
			return tmp
		elif self._task_name and (len(self._task_name) < width):
			return self._task_name
		elif self._task_id:
			return self._task_id
		return ''
