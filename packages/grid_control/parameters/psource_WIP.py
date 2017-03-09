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

from grid_control.parameters.psource_meta import ZipLongParameterSource
from hpfwk import AbstractError
from python_compat import irange


class CombineParameterSource(ZipLongParameterSource):
	alias_list = ['combine']

	def __init__(self, psource1, psource2, var1, var2=None):
		ZipLongParameterSource.__init__(self)
		psource1_values = {}
		for (pnum1, value) in self._iter_parameter_items(psource1, var1):
			psource1_values.setdefault(value, []).append(pnum1)
		self._combine_idx = []
		for (pnum2, value) in self._iter_parameter_items(psource2, var2 or var1):
			for pnum1 in psource1_values.get(value, []):
				self._combine_idx.append((pnum1, pnum2))
		self._combine_idx.sort()
		raise AbstractError

	def _iter_parameter_items(self, psource, var):
		def _get_value(psource, pnum, var):
			result = {}
			psource.fill_parameter_content(pnum, result)
			return result.get(var)
		if psource.get_parameter_len() is None:
			yield (-1, _get_value(psource, None, var))
		else:
			for pnum in irange(psource.get_parameter_len()):
				yield (pnum, _get_value(psource, pnum, var))
