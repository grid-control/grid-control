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
from grid_control.gc_plugin import NamedPlugin
from grid_control.job_db import Job, JobClass
from grid_control.utils.file_tools import SafeFile, with_file
from grid_control.utils.table import ConsoleTable
from hpfwk import AbstractError
from python_compat import imap, irange, lzip


class Report(NamedPlugin):
	config_tag_name = 'report'
	config_section_list = NamedPlugin.config_section_list + ['report']

	def __init__(self, config, name, job_db, task=None):
		NamedPlugin.__init__(self, config, name)
		self._job_class_list = []
		for jc_attr in dir(JobClass):
			if hasattr(getattr(JobClass, jc_attr), 'state_list'):
				self._job_class_list.append(getattr(JobClass, jc_attr))

	def show_report(self, job_db, jobnum_list):
		raise AbstractError

	def _get_job_state_dict(self, job_db, jobnum_list):
		result = dict.fromkeys(Job.enum_value_list, 0)
		result[Job.IGNORED] = len(job_db) - len(jobnum_list)
		for jobnum in jobnum_list:
			job_obj = job_db.get_job_transient(jobnum)
			result[job_obj.state] += 1
		result[None] = sum(result.values())  # get total
		for job_class in self._job_class_list:  # sum job class states
			result[job_class] = sum(imap(lambda job_state: result[job_state], job_class.state_list))
		return result


class ConsoleReport(Report):
	def __init__(self, config, name, job_db, task=None):
		Report.__init__(self, config, name, job_db, task)
		self._show_line = logging.getLogger('console.report').info


class ImageReport(Report):
	def __init__(self, config, name, job_db, task=None):
		Report.__init__(self, config, name, job_db, task)

	def _show_image(self, name, buffer):
		with_file(SafeFile(name, 'wb'), lambda fp: fp.write(buffer.getvalue()))


class MultiReport(Report):
	alias_list = ['multi']

	def __init__(self, config, name, report_list, job_db, task=None):
		Report.__init__(self, config, name, job_db, task)
		self._report_list = report_list

	def show_report(self, job_db, jobnum_list):
		for report in self._report_list:
			report.show_report(job_db, jobnum_list)


class NullReport(Report):
	alias_list = ['null']

	def show_report(self, job_db, jobnum_list):
		pass


class TableReport(Report):
	def __init__(self, config, name, job_db, task=None):
		Report.__init__(self, config, name, job_db, task)
		self._show_table = ConsoleTable.create


class HeaderReport(ConsoleReport):
	def __init__(self, config, name, job_db, task=None):
		ConsoleReport.__init__(self, config, name, job_db, task)
		(self._task_id, self._task_name) = ('', '')
		if task is not None:
			desc = task.get_description()
			(self._task_id, self._task_name) = (desc.task_id, desc.task_name)
		self._header = self._get_header(width=65 - 20)

	def _get_header(self, width):
		tmp = self._task_name + ' / ' + self._task_id
		if self._task_id and self._task_name and (len(tmp) < width):
			return tmp
		elif self._task_name and (len(self._task_name) < width):
			return self._task_name
		elif self._task_id:
			return self._task_id
		return ''


class LocationReport(TableReport):
	alias_list = ['location']

	def show_report(self, job_db, jobnum_list):
		report_dict_list = []
		for jobnum in jobnum_list:
			job_obj = job_db.get_job_transient(jobnum)
			if job_obj.state != Job.INIT:
				report_dict_list.append({0: jobnum, 1: Job.enum2str(job_obj.state), 2: job_obj.gc_id})
				self._fill_report_dict_list(report_dict_list, job_obj)
		header_list = ['Job', 'Status / Attempt', 'Id / Destination']
		self._show_table(lzip(irange(3), header_list), report_dict_list, 'rcl')

	def _fill_report_dict_list(self, report_dict_list, job_obj):
		if job_obj.get_job_location() != 'N/A':
			report_dict_list.append({2: ' -> ' + job_obj.get_job_location()})


class TrivialReport(TableReport):
	alias_list = ['trivial']

	def show_report(self, job_db, jobnum_list):
		self._show_table(lzip(Job.enum_value_list, Job.enum_name_list),
			[self._get_job_state_dict(job_db, jobnum_list)], pivot=True)
