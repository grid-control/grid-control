#-#  Copyright 2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from grid_control.config.cview_tagged import TaggedConfigView
from hpfwk import InstanceFactory, Plugin

# NamedPlugin provides functionality to name plugin instances
class NamedPlugin(Plugin):
	tagName = None

	def __init__(self, config, name):
		self._name = name

	def getObjectName(self):
		return self._name

	def bind(cls, value, modulePaths = None, config = None, tags = None, inherit = False, **kwargs):
		while (': ' in value) or (' :' in value):
			value = value.replace(' :', ':').replace(': ', ':')
		for entry in value.split():
			(clsName, instanceName) = (None, None)
			tmp = entry.split(':', 1)
			if len(tmp) == 2:
				(clsName, instanceName) = tmp
			elif len(tmp) == 1:
				clsName = tmp[0]
			clsNew = cls.getClass(clsName, modulePaths)
			if not instanceName:
				instanceName = clsNew.__name__.split('.')[-1]
			config = config.changeView(viewClass = TaggedConfigView,
				setClasses = [clsNew], setSections = None, setNames = [instanceName],
				addTags = tags or [], inheritSections = inherit)
			yield InstanceFactory(entry, clsNew, config, instanceName)
	bind = classmethod(bind)
