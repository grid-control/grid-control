# | Copyright 2014-2016 Karlsruhe Institute of Technology
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

from grid_control.config.config_entry import norm_config_locations
from grid_control.config.cview_base import SimpleConfigView
from grid_control.utils import safe_index
from hpfwk import APIError
from python_compat import identity, imap, lfilter, lmap, unspecified


class TaggedConfigView(SimpleConfigView):
	def __init__(self, name, oldContainer, curContainer, parent=None,
			setSections=unspecified, addSections=None,
			setNames=unspecified, addNames=None,
			setTags=unspecified, addTags=None,
			setClasses=unspecified, addClasses=None, inheritSections=False):
		parent = parent or self
		if inheritSections and isinstance(parent, TaggedConfigView):
			addSections = (parent.get_class_section_list() or []) + (addSections or [])
		SimpleConfigView.__init__(self, name, oldContainer, curContainer, parent,
			setSections=setSections, addSections=addSections)

		self._class_section_list = self._init_variable(parent, '_class_section_list', None,
			setClasses, addClasses, norm_config_locations, lambda x: x.config_section_list)
		self._section_name_list = self._init_variable(parent, '_section_name_list', [],
			setNames, addNames, norm_config_locations)

		def makeTagTuple(t):
			try:
				config_tag_name = t.config_tag_name.lower()
			except Exception:
				raise APIError('Class %r does not define a valid tag name!' % t.__class__.__name__)
			return [(config_tag_name, t.getObjectName().lower())]
		self._section_tag_list = self._init_variable(parent, '_section_tag_list', [],
			setTags, addTags, identity, makeTagTuple)
		self._section_tag_order = lmap(lambda config_tag_name_tagValue: config_tag_name_tagValue[0], self._section_tag_list)

	def get_class_section_list(self):
		return self._class_section_list

	def __str__(self):
		return '<%s(class = %r, sections = %r, names = %r, tags = %r)>' %\
			(self.__class__.__name__, self._class_section_list, self._section_list, self._section_name_list, self._section_tag_list)

	def _get_section_key(self, section):
		tmp = section.split()
		assert(len(tmp) > 0)
		(curSection, curNames, curTags) = (tmp[0], [], {})
		for token in tmp[1:]:
			if ':' in token:
				tag_entry = token.split(':')
				assert(len(tag_entry) == 2)
				curTags[tag_entry[0]] = tag_entry[1]
			elif token:
				curNames.append(token)

		idxClass = safe_index(self._class_section_list, curSection)
		idxSection = safe_index(self._section_list, curSection)
		if (not self._class_section_list) and (not self._section_list):
			idxSection = 0
		if (idxClass is not None) or (idxSection is not None):  # Section is selected by class or manually
			idxNames = tuple(imap(lambda n: safe_index(self._section_name_list, n), curNames))
			if None not in idxNames:  # All names in current section are selected
				curTagNames = lfilter(lambda tn: tn in curTags, self._section_tag_order)
				curTagNamesLeft = lfilter(lambda tn: tn not in self._section_tag_order, curTags)
				idxTags = lmap(lambda tn: safe_index(self._section_tag_list, (tn, curTags[tn])), curTagNames)
				if (None not in idxTags) and not curTagNamesLeft:
					return (idxClass, idxSection, idxNames, idxTags)

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
