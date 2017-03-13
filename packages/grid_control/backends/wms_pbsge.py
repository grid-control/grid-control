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

from grid_control.backends.wms import WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.utils import resolve_install_path


class PBSGECommon(LocalWMS):
	def __init__(self, config, name, check_executor, cancel_executor, nodes_finder, queues_finder):
		LocalWMS.__init__(self, config, name,
			submit_exec=resolve_install_path('qsub'),
			check_executor=check_executor, cancel_executor=cancel_executor,
			nodes_finder=nodes_finder, queues_finder=queues_finder)
		self._shell = config.get('shell', '', on_change=None)
		self._account = config.get('account', '', on_change=None)
		self._delay = config.get_bool('delay output', False, on_change=None)
		self._software_req_lookup = config.get_lookup('software requirement map', {},
			single=False, on_change=None)

	def _get_common_submit_arguments(self, jobnum, job_name, reqs, sandbox, stdout, stderr, req_map):
		# Job name
		params = ' -N "%s"' % job_name
		# Job accounting
		if self._account:
			params += ' -P %s' % self._account
		# Job shell
		if self._shell:
			params += ' -S %s' % self._shell
		# Process job requirements
		for entry in self._software_req_lookup.lookup(reqs.get(WMS.SOFTWARE), is_selector=False):
			params += ' ' + entry
		for req in req_map:
			if self._check_req(reqs, req):
				params += ' -l %s=%s' % (req_map[req][0], req_map[req][1](reqs[req]))
		# Sandbox, IO paths
		params += ' -v GC_SANDBOX="%s"' % sandbox
		if self._delay:
			params += ' -v GC_DELAY_OUTPUT="%s" -v GC_DELAY_ERROR="%s"' % (stdout, stderr)
			params += ' -o /dev/null -e /dev/null'
		else:
			params += ' -o "%s" -e "%s"' % (stdout, stderr)
		return params

	def _get_job_arguments(self, jobnum, sandbox):
		return ''
