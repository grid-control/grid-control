# | Copyright 2013-2017 Karlsruhe Institute of Technology
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

from grid_control.utils.data_structures import UniqueList


class JobDef(object):
	def __init__(self, jobnum):
		self.variables = {}
		self.active = True
		self.jobnum = jobnum
		(self.files, self.software, self.storage) = ([], None, None)
		(self.memory, self.time_wall, self.time_cpu, self.cores) = (None, None, None, None)

	def apply_to(self, other):
		other.variables.update(self.variables)
		other.active = other.active and self.active
		other.files.extend(self.files)
		# FIX requirements!

	def get_hash(self):
		return None

	def require_cores(self, value):
		self.cores = self._combine_req(max, self.cores, value)

	def require_cpu_time(self, value):
		self.time_cpu = self._combine_req(max, self.time_cpu, value)

	def require_memory(self, value):
		self.memory = self._combine_req(max, self.memory, value)

	def require_software(self, value):
		if value is not None:
			self.software = UniqueList(self.software + [value])

	def require_storage(self, se_list):
		self.storage = UniqueList(self.storage + (se_list or []))

	def require_wall_time(self, value):
		self.time_wall = self._combine_req(max, self.time_wall, value)

	def _combine_req(self, fun, value_old, value_new):
		if value_old is not None:
			return fun(value_old, value_new)
		return value_new
