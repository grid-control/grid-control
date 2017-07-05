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

import shlex
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils import resolve_install_path
from grid_control.utils.process_base import LocalProcess
from hpfwk import AbstractError, ignore_exception


class LocalSubmitWithProcess(ConfigurablePlugin):
	def __init__(self, config, submit_exec, submit_req_map=None):
		ConfigurablePlugin.__init__(self, config)
		self._submit_exec = resolve_install_path(submit_exec)
		self._submit_opt_list = shlex.split(config.get('submit options', '', on_change=None))
		self._submit_req_map = submit_req_map or {}
		self._timeout = config.get_time('submit timeout', 20, on_change=None)

	def get_array_key_list(self):  # return possible variable names with job array identifiers
		return None  # FIXME: offset information (start at 0 or 1)?

	def submit(self, log, job_desc, exec_fn, req_list, stdout_fn, stderr_fn):
		submit_args = self._submit_opt_list + self._get_submit_arguments(
			job_desc, exec_fn, req_list, stdout_fn, stderr_fn)
		proc = LocalProcess(self._submit_exec, *submit_args)
		exit_code = proc.status(timeout=self._timeout, terminate=True)
		wms_id_str = proc.stdout.read(timeout=0).strip().strip('\n')
		wms_id = ignore_exception(Exception, None, self._parse_submit_output, wms_id_str)

		if exit_code != 0:
			log.log_process(proc)
		elif wms_id is None:
			log.log_process(proc, msg='Unable to parse wms id: %s' % repr(wms_id_str))
		return wms_id

	def _get_req_arg_list(self, req_list):
		result = []
		for (req, req_value) in req_list:
			req_info = self._submit_req_map.get(req)
			if req_info:
				(req_opt, req_fmt, req_filter) = req_info
				if req_filter(req_value):
					result.extend([req_opt, req_fmt(req_value)])
		return result

	def _get_submit_arguments(self, job_desc, exec_fn, req_list, stdout_fn, stderr_fn):
		raise AbstractError

	def _parse_submit_output(self, wms_id_str):
		raise AbstractError
