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

from grid_control.parameters.psource_base import ParameterSource
from grid_control.parameters.psource_basic import ImmutableParameterSource
from grid_control.utils.parsing import str_dict_linear
from hpfwk import APIError, AbstractError
from python_compat import lmap


class InternalNestedParameterSource(ImmutableParameterSource):
	def fill_parameter_content(self, pnum, result):
		pass

	def fill_parameter_metadata(self, result):
		pass

	def get_nested(self, psrc):
		raise AbstractError


class InternalParameterSource(ImmutableParameterSource):
	def __init__(self, value_list, meta_list):
		(self._value_list, self._meta_list) = (value_list, meta_list)
		self._keys = lmap(lambda pm: pm.get_value(), meta_list)
		ImmutableParameterSource.__init__(self, (lmap(str_dict_linear, value_list), self._keys))

	def __repr__(self):
		return '<internal:%s=%s>' % (str.join('|', self._keys), self.get_psrc_hash())

	def fill_parameter_content(self, pnum, result):
		result.update(self._value_list[pnum])

	def fill_parameter_metadata(self, result):
		result.extend(self._meta_list)

	def get_parameter_len(self):
		return len(self._value_list)


class InternalReferenceParameterSource(ParameterSource):  # Redirector ParameterSource
	def create_psrc(cls, pconfig, repository, ref_name, *args):  # pylint:disable=arguments-differ
		ref_type_default = 'data'
		if 'dataset:' + ref_name not in repository:
			ref_type_default = 'csv'
		ref_type = pconfig.get(ref_name, 'type', ref_type_default)
		return ParameterSource.create_psrc_safe(ref_type, pconfig, repository, ref_name, *args)
	create_psrc = classmethod(create_psrc)


class RedirectorParameterSource(ParameterSource):
	def __init__(self, *args, **kwargs):
		ParameterSource.__init__(self)
		raise APIError('Redirector class initialized')


class InternalAutoParameterSource(RedirectorParameterSource):
	def create_psrc(cls, pconfig, repository, output_vn, lookup_vn_list=None):  # pylint:disable=arguments-differ
		parameter_value = pconfig.get_parameter(output_vn.lstrip('!'))
		if isinstance(parameter_value, list):
			if len(parameter_value) != 1:  # Simplify single value parameters to const parameters
				return ParameterSource.create_instance('SimpleParameterSource', output_vn, parameter_value)
			return ParameterSource.create_instance('ConstParameterSource', output_vn, parameter_value[0])
		elif isinstance(parameter_value, tuple) and not isinstance(parameter_value[0], dict):
			return ParameterSource.create_instance(*parameter_value)
		return ParameterSource.create_instance('InternalAutoLookupParameterSource',
			pconfig, output_vn, lookup_vn_list)
	create_psrc = classmethod(create_psrc)


class InternalResolveParameterSource(RedirectorParameterSource):
	def __new__(cls, root_psrc):
		if isinstance(root_psrc, ParameterSource):
			for psrc in root_psrc.get_used_psrc_list():
				if isinstance(psrc, InternalNestedParameterSource):
					root_psrc = psrc.get_nested(root_psrc)
		return root_psrc
