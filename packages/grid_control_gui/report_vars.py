# | Copyright 2012-2016 Karlsruhe Institute of Technology
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
from grid_control.utils import printTabular
from python_compat import imap, lzip, set, sorted

class VariablesReport(Report):
	alias = ['variables', 'vars']

	def __init__(self, jobDB, task, jobs = None, configString = ''):
		Report.__init__(self, jobDB, task, jobs, configString)
		self._selector = JobSelector.create(configString, task = task)

	def display(self):
		taskConfig = self._task.getTaskConfig()
		header = lzip(taskConfig, taskConfig)
		header.extend(imap(lambda key: (key, '<%s>' % key), self._task.getTransientVars()))
		variables = set()
		entries = []
		for jobNum in self._jobDB.getJobs(self._selector):
			jobConfig = self._task.getJobConfig(jobNum)
			variables.update(jobConfig)
			entry = dict(taskConfig)
			entry.update(self._task.getTransientVars())
			entry.update(jobConfig)
			entries.append(entry)
		printTabular(sorted(header + lzip(variables, variables)), entries)
