# | Copyright 2010-2017 Karlsruhe Institute of Technology
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

import shlex
from grid_control.backends.aspect_submit import LocalSubmitWithProcess
from grid_control.backends.wms import WMS
from python_compat import identity


class PBSGESubmit(LocalSubmitWithProcess):
	def __init__(self, config, submit_exec, submit_req_map):
		submit_req_map[WMS.ARRAY_SIZE] = ('-t', identity, lambda req_value: req_value > 1)  # Array jobs
		LocalSubmitWithProcess.__init__(self, config, submit_exec, submit_req_map)
		self._shell = config.get('shell', '', on_change=None)
		self._account = config.get('account', '', on_change=None)
		self._software_req_lookup = config.get_lookup('software requirement map', {},
			single=False, on_change=None)

	def _get_submit_arguments(self, job_desc, exec_fn, req_list, stdout_fn, stderr_fn):
		# Job name
		arg_list = ['-N', job_desc.job_name, '-o', stdout_fn, '-e', stderr_fn]
		# Job accounting
		if self._account:
			arg_list.extend(['-P', self._account])
		# Job shell
		if self._shell:
			arg_list.extend(['-S', self._shell])
		# Process job requirements
		software_matched = False
		for (req, req_value) in req_list:
			if req == WMS.SOFTWARE:
				for entry in self._software_req_lookup.lookup(req_value, is_selector=False):
					software_matched = True
					arg_list.extend(shlex.split(entry))
		if not software_matched:
			for entry in self._software_req_lookup.lookup(None, is_selector=False):
				arg_list.extend(shlex.split(entry))
		return arg_list + self._get_req_arg_list(req_list) + [exec_fn]
