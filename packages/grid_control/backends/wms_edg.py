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

from grid_control import utils
from grid_control.backends.jdl_writer import JDLWriter
from grid_control.backends.wms_grid import GridWMS, Grid_CancelJobs, Grid_CheckJobs
from python_compat import imap

class EDGJDL(JDLWriter):
	def _format_reqs_storage(self, locations):
		if locations:
			location_iter = imap(lambda x: '(target.GlueSEUniqueID == %s)' % self._escape(x), locations)
			return 'anyMatch(other.storage.CloseSEs, %s)' % str.join(' || ', location_iter)


class EuropeanDataGrid(GridWMS):
	alias = ['EDG', 'LCG']

	def __init__(self, config, name):
		GridWMS.__init__(self, config, name,
			checkExecutor = Grid_CheckJobs(config, 'edg-job-status'),
			cancelExecutor = Grid_CancelJobs(config, 'edg-job-cancel'),
			jdlWriter = EDGJDL())

		self._submitExec = utils.resolveInstallPath('edg-job-submit')
		self._outputExec = utils.resolveInstallPath('edg-job-get-output')
		self._submitParams.update({'-r': self._ce, '--config-vo': self._configVO })
