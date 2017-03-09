# | Copyright 2016-2017 Karlsruhe Institute of Technology
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
from grid_control.backends.backend_tools import BackendExecutor
from hpfwk import AbstractError, clear_current_exception
from python_compat import tarfile


class RetrieveJobs(BackendExecutor):
	def execute(self, wms_id_list):  # yields list of (wms_id, local_output_dir)
		raise AbstractError


class RetrieveAndPurgeJobs(RetrieveJobs):
	pass


class RetrieveJobsEmulateWildcard(RetrieveJobs):
	def __init__(self, config, executor):
		RetrieveJobs.__init__(self, config)
		(self._executor, self._wildcard_file) = (executor, 'GC_WC.tar.gz')

	def execute(self, wms_id_list):  # yields list of (wms_id, local_output_dir)
		for (wms_id, local_output_dir) in self._executor.execute(self, wms_id_list):
			if local_output_dir and os.path.exists(local_output_dir):
				fn_wildcard_tar = os.path.join(local_output_dir, self._wildcard_file)
				if os.path.exists(fn_wildcard_tar):
					try:
						tarfile.TarFile.open(fn_wildcard_tar, 'r:gz').extractall(local_output_dir)
					except Exception:
						self._log.error('Unable to unpack output files contained in %s', fn_wildcard_tar)
						clear_current_exception()
						continue
					try:
						os.unlink(fn_wildcard_tar)
					except Exception:
						self._log.error('Unable to remove wildcard emulation file %s', fn_wildcard_tar)
						clear_current_exception()
			yield (wms_id, local_output_dir)

	def setup(self, log):
		RetrieveJobs.setup(self, log)
		self._executor.setup(log)
