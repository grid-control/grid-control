#!/usr/bin/env python
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

import sys
from gc_scripts import Result, ScriptOptions, display_plugin_list_for, gc_create_config
from grid_control.backends import WMS


class DummyTask(object):
	def get_dependency_list(self):
		return []

	def get_description(self, jobnum=None):
		return Result(task_id='GCmanual', task_name='GCmanuel', job_id=jobnum, job_name=jobnum)

	def get_job_arguments(self, jobnum):
		return 'arg1 "arg2 arg3"'

	def get_job_dict(self, jobnum):
		return {'GC_RUNTIME': 'ls'}

	def get_requirement_list(self, jobnum):
		return []

	def get_sb_in_fn_list(self):
		return []

	def get_sb_out_fn_list(self):
		return []

	def get_schedule(self, jobnum):
		return (';', [])

	def get_se_in_fn_list(self):
		return []

	def get_var_alias_map(self):
		return {}


def _main():
	parser = ScriptOptions(usage='%s [OPTIONS] <Backend>')
	parser.add_bool(None, 'L', 'list_wms', default=False, help='Show available backends')
	parser.add_text(None, 'W', 'workdir', default='work', help='Set work directory')
	parser.section('reqs', 'Job requirements')
	parser.add_list('reqs', 'c', 'cores', default=[], help='Add core requirement')
	parser.add_list('reqs', 'd', 'diskspace', default=[], help='Add diskspace requirement')
	parser.add_list('reqs', 'e', 'endpoint', default=[], help='Add endpoint requirement')
	parser.add_list('reqs', 'l', 'locality', default=[], help='Add storage locality requirement')
	parser.add_list('reqs', 'm', 'memory', default=[], help='Add memory requirement')
	parser.add_list('reqs', 'n', 'node', default=[], help='Add workernode requirement')
	parser.add_list('reqs', 'q', 'queue', default=[], help='Add queue requirement')
	parser.add_list('reqs', 's', 'site', default=[], help='Add site requirement')
	parser.add_list('reqs', 'S', 'software', default=[], help='Add software requirement')
	parser.add_list('reqs', 't', 'cputime', default=[], help='Add cputime requirement')
	parser.add_list('reqs', 'w', 'walltime', default=[], help='Add walltime requirement')
	options = parser.script_parse()
	if options.opts.list_wms:
		display_plugin_list_for('WMS', show_all=True, title='Available job submission backends')
	if len(options.args) != 1:
		parser.exit_with_usage()
	config = gc_create_config(config_dict={'global': {'workdir': options.opts.workdir}})
	backend = WMS.create_instance(options.args[0], config, 'wms')
	print backend
	dummy_task = DummyTask()
	for result in backend.submit_jobs([0], dummy_task):
		print result


if __name__ == '__main__':
	sys.exit(_main())
