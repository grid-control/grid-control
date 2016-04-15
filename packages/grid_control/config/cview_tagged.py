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

from grid_control.config.config_entry import standardConfigForm
from grid_control.config.cview_base import SimpleConfigView, selectorUnchanged
from hpfwk import APIError
from python_compat import identity, imap, lfilter, lmap

# Simple ConfigView implementation
class TaggedConfigView(SimpleConfigView):
	def __init__(self, name, oldContainer, curContainer, parent = None,
			setSections = selectorUnchanged, addSections = None,
			setNames = selectorUnchanged, addNames = None,
			setTags = selectorUnchanged, addTags = None,
			setClasses = selectorUnchanged, addClasses = None, inheritSections = False):
		if inheritSections and parent:
			addSections = parent.getClassSections() + (addSections or [])
		SimpleConfigView.__init__(self, name, oldContainer, curContainer, parent,
			setSections = setSections, addSections = addSections)

		self._initVariable('_cfgClassSections', None, setClasses, addClasses, standardConfigForm, lambda x: x.configSections)
		self._initVariable('_cfgNames', [], setNames, addNames, standardConfigForm)
		def makeTagTuple(t):
			try:
				tagName = t.tagName.lower()
			except Exception:
				raise APIError('Class %r does not define a valid tag name!' % t.__class__.__name__)
			return [(tagName, t.getObjectName().lower())]
		self._initVariable('_cfgTags', [], setTags, addTags, identity, makeTagTuple)
		self._cfgTagsOrder = lmap(lambda tagName_tagValue: tagName_tagValue[0], self._cfgTags)

	def getClassSections(self):
		return self._cfgClassSections

	def __str__(self):
		return '<%s(class = %r, sections = %r, names = %r, tags = %r)>' %\
			(self.__class__.__name__, self._cfgClassSections, self._cfgSections, self._cfgNames, self._cfgTags)

	def _getSectionKey(self, section):
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

		def myIndex(src, value):
			try:
				return src.index(value)
			except Exception:
				return None
		idxClass = myIndex(self._cfgClassSections, curSection)
		idxSection = myIndex(self._cfgSections, curSection)
		if (not self._cfgClassSections) and (not self._cfgSections):
			idxSection = 0
		if (idxClass is not None) or (idxSection is not None): # Section is selected by class or manually
			idxNames = tuple(imap(lambda n: myIndex(self._cfgNames, n), curNames))
			if None not in idxNames: # All names in current section are selected
				curTagNames = lfilter(lambda tn: tn in curTags, self._cfgTagsOrder)
				curTagNamesLeft = lfilter(lambda tn: tn not in self._cfgTagsOrder, curTags)
				idxTags = lmap(lambda tn: myIndex(self._cfgTags, (tn, curTags[tn])), curTagNames)
				if (None not in idxTags) and not curTagNamesLeft:
					return (idxClass, idxSection, idxNames, idxTags)

	def _getSection(self, specific):
		if specific:
			if self._cfgClassSections:
				section = self._cfgClassSections[-1]
			else:
				section = SimpleConfigView._getSection(self, specific)
			if self._cfgNames:
				section += ' %s' % str.join(' ', self._cfgNames)
			if self._cfgTags:
				section += ' %s' % str.join(' ', imap(lambda t: '%s:%s' % t, self._cfgTags))
			return section
		elif self._cfgClassSections:
			return self._cfgClassSections[0]
		return SimpleConfigView._getSection(self, specific)
