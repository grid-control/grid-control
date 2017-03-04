# | Copyright 2012-2017 Karlsruhe Institute of Technology
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

from grid_control.job_selector import JobSelector
from grid_control.report import Report
from grid_control.utils import display_table
from python_compat import imap, lzip, set, sorted


class VariablesReport(Report):
	alias_list = ['variables', 'vars']

	def __init__(self, job_db, task, jobs=None, config_str=''):
		Report.__init__(self, job_db, task, jobs, config_str)
		self._task = task
		self._selector = JobSelector.create(config_str, task=task)

	def show_report(self, job_db):
		task_config = {}
		if self._task:
			task_config = self._task.get_task_dict()
		header = lzip(task_config, task_config)
		if self._task:
			header.extend(imap(lambda key: (key, '<%s>' % key), self._task.get_transient_variables()))
		variables = set()
		entries = []
		for jobnum in job_db.get_job_list(self._selector):
			job_config = {}
			if self._task:
				job_config = self._task.get_job_dict(jobnum)
			variables.update(job_config)
			entry = dict(task_config)
			if self._task:
				entry.update(self._task.get_transient_variables())
			entry.update(job_config)
			entries.append(entry)
		display_table(sorted(header + lzip(variables, variables)), entries)
