# | Copyright 2017 Karlsruhe Institute of Technology
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

import os
from grid_control.job_db import Job
from grid_control.report import Report
from grid_control.utils import clean_path
from python_compat import json


class FileReport(Report):
	alias_list = ['file']

	def __init__(self, config, name, job_db, task=None):
		# needed in destructor:
		self._task_info = {}
		self._output_fn = None

		Report.__init__(self, config, name, job_db, task)

		task_id = 'Unknown'
		name = 'Unknown'
		if task:
			desc = task.get_description()
			task_id = desc.task_id
			name = desc.task_name
		self._task_info = {
			'task id': task_id,
			'name': name,
		}

		output_dn = clean_path(config.get('report file directory', on_change=None))
		self._output_fn = os.path.join(output_dn, 'states' + task_id + '.json')

	def __del__(self):
		if not self._output_fn:
			# no path known to write to
			return
		self._task_info['gc state'] = 'terminated'
		self._write_task_info()

	def get_height(self):
		return 0

	def show_report(self, job_db, jobnum_list):
		self._update_job_states(job_db, jobnum_list)
		self._task_info['gc state'] = 'running'
		self._write_task_info()

	def _update_job_states(self, job_db, jobnum_list):
		states = {}
		for state_name in Job.enum_name_list:
			states[state_name] = 0

		for jobnum in jobnum_list:
			states[Job.enum2str(job_db.get_job_transient(jobnum).state)] += 1

		self._task_info['states'] = states

	def _write_task_info(self):
		outfile = open(self._output_fn, 'w')
		json.dump(self._task_info, outfile)
		outfile.close()
