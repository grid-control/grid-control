# | Copyright 2016 Karlsruhe Institute of Technology
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
from python_compat import irange, sort_inplace

class CombineParameterSource(ZipLongParameterSource):
	alias = ['combine']
	def __init__(self, psource1, psource2, var1, var2 = None):
		psource1_values = {}
		for (pNum1, value) in self._iterParamItems(psource1, var1):
			psource1_values.setdefault(value, []).append(pNum1)
		self._combine_idx = []
		for (pNum2, value) in self._iterParamItems(psource2, var2 or var1):
			for pNum1 in psource1_values.get(value, []):
				self._combine_idx.append((pNum1, pNum2))
		sort_inplace(self._combine_idx)
		raise AbstractError

	def _iterParamItems(self, psource, var):
		def getValue(psource, pNum, var):
			result = {}
			psource.fillParameterInfo(pNum, result)
			return result.get(var)
		if psource.getMaxParameters() is None:
			yield (-1, getValue(psource, None, var))
		else:
			for pNum in irange(psource.getMaxParameters()):
				yield (pNum, getValue(psource, pNum, var))
