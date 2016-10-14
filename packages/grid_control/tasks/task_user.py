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

from grid_control.tasks.task_data import DataTask
from grid_control.tasks.task_utils import TaskExecutableWrapper
from python_compat import lmap


class UserTask(DataTask):
	alias_list = ['UserMod', 'user', 'script']
	config_section_list = DataTask.config_section_list + ['UserMod', 'UserTask']

	def __init__(self, config, name):
		DataTask.__init__(self, config, name)
		self._exe = TaskExecutableWrapper(config)

	def get_command(self):
		return '(%s) > job.stdout 2> job.stderr' % self._exe.get_command()

	def get_job_arguments(self, jobnum):
		return DataTask.get_job_arguments(self, jobnum) + ' ' + self._exe.get_arguments()

	def get_sb_in_fpi_list(self):
		return DataTask.get_sb_in_fpi_list(self) + self._exe.get_sb_in_fpi_list()

	def get_sb_out_fn_list(self):
		job_out_fn_list = ['job.stdout', 'job.stderr']
		if self._do_gzip_std_output:
			job_out_fn_list = lmap(lambda fn: fn + '.gz', job_out_fn_list)
		return DataTask.get_sb_out_fn_list(self) + job_out_fn_list
