# | Copyright 2010-2016 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.backends.wms import WMS
from grid_control.backends.wms_local import LocalWMS


class PBSGECommon(LocalWMS):
	def __init__(self, config, name, check_executor, cancel_executor, nodesFinder, queuesFinder):
		LocalWMS.__init__(self, config, name,
			submitExec = utils.resolve_install_path('qsub'),
			check_executor = check_executor, cancel_executor = cancel_executor,
			nodesFinder = nodesFinder, queuesFinder = queuesFinder)
		self._shell = config.get('shell', '', on_change = None)
		self._account = config.get('account', '', on_change = None)
		self._delay = config.get_bool('delay output', False, on_change = None)
		self._softwareReqs = config.get_lookup('software requirement map', {}, single = False, on_change = None)


	def get_job_arguments(self, jobnum, sandbox):
		return ''


	def getCommonSubmitArguments(self, jobnum, job_name, reqs, sandbox, stdout, stderr, reqMap):
		# Job name
		params = ' -N "%s"' % job_name
		# Job accounting
		if self._account:
			params += ' -P %s' % self._account
		# Job shell
		if self._shell:
			params += ' -S %s' % self._shell
		# Process job requirements
		for entry in self._softwareReqs.lookup(reqs.get(WMS.SOFTWARE), is_selector = False):
			params += ' ' + entry
		for req in reqMap:
			if self.checkReq(reqs, req):
				params += ' -l %s=%s' % (reqMap[req][0], reqMap[req][1](reqs[req]))
		# Sandbox, IO paths
		params += ' -v GC_SANDBOX="%s"' % sandbox
		if self._delay:
			params += ' -v GC_DELAY_OUTPUT="%s" -v GC_DELAY_ERROR="%s" -o /dev/null -e /dev/null' % (stdout, stderr)
		else:
			params += ' -o "%s" -e "%s"' % (stdout, stderr)
		return params
