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

import logging
from hpfwk import InstanceFactory, Plugin

# ConfigurablePlugin is the base class for plugins that need config as constructor parameter
class ConfigurablePlugin(Plugin):
	def __init__(self, config):
		pass

	def bind(cls, value, **kwargs):
		config = kwargs.pop('config')
		for entry in value.split():
			yield InstanceFactory(entry, cls.getClass(entry), config)
	bind = classmethod(bind)


# NamedPlugin provides functionality to name plugin instances
class NamedPlugin(ConfigurablePlugin):
	tagName = None

	def __init__(self, config, name):
		self._name = name
		self._log = logging.getLogger('%s.%s' % (self.tagName.lower(), name.lower()))
		ConfigurablePlugin.__init__(self, config)

	def getObjectName(self):
		return self._name

	def bind(cls, value, **kwargs):
		while (': ' in value) or (' :' in value):
			value = value.replace(' :', ':').replace(': ', ':')
		config = kwargs.pop('config')
		tags = kwargs.pop('tags', None)
		inheritSections = kwargs.pop('inherit', False)
		for entry in value.split():
			(clsName, instanceName) = (None, None)
			tmp = entry.split(':', 1)
			if len(tmp) == 2:
				(clsName, instanceName) = tmp
			elif len(tmp) == 1:
				clsName = tmp[0]
			clsNew = cls.getClass(clsName)
			if not instanceName:
				instanceName = clsNew.__name__.split('.')[-1]
			cls_config = config.changeView(viewClass = 'TaggedConfigView',
				setClasses = [clsNew], setSections = None, setNames = [instanceName],
				addTags = tags or [], inheritSections = inheritSections)
			yield InstanceFactory(entry, clsNew, cls_config, instanceName)
	bind = classmethod(bind)
