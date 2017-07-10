# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import time, logging
from grid_control.utils.data_structures import make_enum
from hpfwk import AbstractError, NestedException, Plugin
from python_compat import set


ParameterInfo = make_enum(['ACTIVE', 'HASH', 'REQS', 'FILES'])  # pylint:disable=invalid-name


class ParameterError(NestedException):
	pass


class ParameterMetadata(object):
	def __init__(self, value, untracked=False):
		(self.value, self.untracked) = (value, untracked)

	def __repr__(self):
		return self.get_value()

	def get_value(self):
		if self.untracked:
			return '!' + self.value
		return self.value


class ParameterSource(Plugin):
	def __init__(self):
		self._log = logging.getLogger('parameters.source')
		Plugin.__init__(self)

	def can_finish(self):
		return True

	def create_psrc(cls, pconfig, repository, *args, **kwargs):
		return cls(*args, **kwargs)
	create_psrc = classmethod(create_psrc)

	def create_psrc_safe(cls, cls_name, pconfig, repository, *args, **kwargs):
		try:
			psrc_type = ParameterSource.get_class(cls_name)
			cls_name = psrc_type.__name__  # update class name for error message
			return psrc_type.create_psrc(pconfig, repository, *args, **kwargs)
		except Exception:
			error_msg = 'Error while creating %r with arguments %r and keywords %r'
			raise ParameterError(error_msg % (cls_name, args, kwargs))
	create_psrc_safe = classmethod(create_psrc_safe)

	def fill_parameter_content(self, pnum, result):
		raise AbstractError

	def fill_parameter_metadata(self, result):
		raise AbstractError

	def get_empty_resync_result(cls):
		return (set(), set(), False)  # (<redo>, <disable>, <size changes>)
	get_empty_resync_result = classmethod(get_empty_resync_result)

	def get_parameter_deps(self):
		return []

	def get_parameter_len(self):
		return None

	def get_psrc_hash(self):
		raise AbstractError

	def get_resync_request(self):
		return []

	def get_used_psrc_list(self):
		return [self]

	def resync_psrc(self):
		# only needed if the parameters are opaque references (like partition idx)
		return ParameterSource.get_empty_resync_result()

	def show_psrc(self):
		return [self.__class__.__name__ + ':']


class LimitedResyncParameterSource(ParameterSource):
	def __init__(self):
		ParameterSource.__init__(self)
		(self._resync_interval, self._resync_force, self._resync_last) = (-1, False, time.time())

	def get_resync_request(self):
		t_last_resync = abs(time.time() - self._resync_last)
		resync_needed = self._resync_interval >= 0 and (t_last_resync >= self._resync_interval)
		if self._resync_force or resync_needed:
			return [self]
		return []

	def resync_psrc(self):
		result = None
		if self.get_resync_request():
			result = self._resync_psrc()
			self._resync_force = False
			self._resync_last = time.time()
		return result or ParameterSource.get_empty_resync_result()

	def setup_resync(self, interval=None, force=None):
		if interval is not None:
			self._resync_interval = interval
		if force is not None:
			self._resync_force = force

	def _resync_psrc(self):
		pass


class NullParameterSource(ParameterSource):
	alias_list = ['null']

	def __repr__(self):
		return 'null()'

	def create_psrc(cls, pconfig, repository):
		return cls()
	create_psrc = classmethod(create_psrc)

	def fill_parameter_content(self, pnum, result):
		pass

	def fill_parameter_metadata(self, result):
		pass

	def get_psrc_hash(self):
		return ''
