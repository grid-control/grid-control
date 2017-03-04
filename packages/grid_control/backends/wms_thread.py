# | Copyright 2012-2017 Karlsruhe Institute of Technology
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

from grid_control.backends.wms_multi import MultiWMS
from grid_control.utils.thread_tools import tchain
from python_compat import ifilter, imap


class ThreadedMultiWMS(MultiWMS):
	def _forward_call(self, args, assign_fun, call_fun):
		backend_name2args = self._get_map_backend_name2args(args, assign_fun)

		def _make_generator(backend_name):
			return call_fun(self._map_backend_name2backend[backend_name], backend_name2args[backend_name])

		backend_name_iter = ifilter(backend_name2args.__contains__, self._map_backend_name2backend)
		for result in tchain(imap(_make_generator, backend_name_iter)):
			yield result
