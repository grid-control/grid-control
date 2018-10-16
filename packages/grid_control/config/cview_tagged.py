# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

from grid_control.config.config_entry import ConfigError, norm_config_locations
from grid_control.config.cview_base import SimpleConfigView
from grid_control.utils.algos import safe_index
from hpfwk import APIError
from python_compat import identity, imap, itemgetter, lfilter, lmap, unspecified


class TaggedConfigView(SimpleConfigView):
	def __init__(self, name, container_old, container_cur, parent=None,
			set_sections=unspecified, add_sections=None,
			set_names=unspecified, add_names=None,
			set_tags=unspecified, add_tags=None,
			set_classes=unspecified, add_classes=None, inherit_sections=False):
		parent = parent or self
		if inherit_sections and isinstance(parent, TaggedConfigView):
			add_sections = (parent.get_class_section_list() or []) + (add_sections or [])
		SimpleConfigView.__init__(self, name, container_old, container_cur, parent,
			set_sections=set_sections, add_sections=add_sections)

		self._class_section_list = self._init_variable(parent, '_class_section_list', None,
			set_classes, add_classes, norm_config_locations, lambda x: x.config_section_list)
		self._section_name_list = self._init_variable(parent, '_section_name_list', [],
			set_names, add_names, norm_config_locations)

		def _get_tag_tuple(tag_obj):
			try:
				config_tag_name = tag_obj.config_tag_name.lower()
			except Exception:
				raise APIError('Class %r does not define a valid tag name!' % tag_obj.__class__.__name__)
			return [(config_tag_name, tag_obj.get_object_name().lower())]
		self._section_tag_list = self._init_variable(parent, '_section_tag_list', [],
			set_tags, add_tags, identity, _get_tag_tuple)
		self._section_tag_order = lmap(itemgetter(0), self._section_tag_list)

	def __str__(self):
		return '<%s(class = %r, sections = %r, names = %r, tags = %r)>' % (
			self.__class__.__name__, self._class_section_list, self._section_list,
			self._section_name_list, self._section_tag_list)

	def get_class_section_list(self):
		return self._class_section_list

	def _get_section(self, specific):
		if specific:
			if self._class_section_list:
				section = self._class_section_list[-1]
			else:
				section = SimpleConfigView._get_section(self, specific)
			if self._section_name_list:
				section += ' %s' % str.join(' ', self._section_name_list)
			if self._section_tag_list:
				section += ' %s' % str.join(' ', imap(lambda t: '%s:%s' % t, self._section_tag_list))
			return section
		elif self._class_section_list:
			return self._class_section_list[0]
		return SimpleConfigView._get_section(self, specific)

	def _get_section_key(self, section):
		tmp = section.split()
		if not tmp:
			raise ConfigError('Invalid config section %r' % section)
		(cur_section, cur_name_list, cur_tag_map) = (tmp[0], [], {})
		for token in tmp[1:]:
			if ':' in token:
				tag_entry = token.split(':')
				if len(tag_entry) != 2:
					raise ConfigError('Invalid config tag in section %r' % section)
				cur_tag_map[tag_entry[0]] = tag_entry[1]
			elif token:
				cur_name_list.append(token)

		class_section_idx = safe_index(self._class_section_list, cur_section)
		section_idx = safe_index(self._section_list, cur_section)
		if (not self._class_section_list) and (not self._section_list):
			section_idx = 0
		if (class_section_idx is not None) or (section_idx is not None):
			# Section is selected by class or manually
			name_idx_tuple = tuple(imap(lambda n: safe_index(self._section_name_list, n), cur_name_list))
			if None not in name_idx_tuple:  # All names in current section are selected
				cur_tag_name_list = lfilter(cur_tag_map.__contains__, self._section_tag_order)
				left_tag_name_list = lfilter(lambda tn: tn not in self._section_tag_order, cur_tag_map)
				tag_tuple_list = imap(lambda tn: (tn, cur_tag_map[tn]), cur_tag_name_list)
				tag_idx_tuple = tuple(imap(lambda tt: safe_index(self._section_tag_list, tt), tag_tuple_list))
				if (None not in tag_idx_tuple) and not left_tag_name_list:
					return (class_section_idx, section_idx, name_idx_tuple, tag_idx_tuple)
