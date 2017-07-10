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

import logging
from hpfwk import InstanceFactory, Plugin


class ConfigurablePlugin(Plugin):
	# ConfigurablePlugin is the base class for plugins that need "config" as constructor parameter
	def __init__(self, config):
		pass

	def bind(cls, value, **kwargs):
		config = kwargs.pop('config')
		cls_config = config.change_view(add_sections=cls.config_section_list)
		for entry in value.split():
			cls_new = cls.get_class(entry)
			yield InstanceFactory(cls_new.get_bind_class_name(entry), cls_new, cls_config)
	bind = classmethod(bind)


class NamedPlugin(ConfigurablePlugin):
	# NamedPlugin provides functionality to name plugin instances
	config_tag_name = None

	def __init__(self, config, name):
		self._name = name
		self._log = logging.getLogger('%s.%s' % (self.config_tag_name.lower(), name.lower()))
		ConfigurablePlugin.__init__(self, config)

	def bind(cls, value, **kwargs):
		while (': ' in value) or (' :' in value):
			value = value.replace(' :', ':').replace(': ', ':')
		config = kwargs.pop('config')
		tags = kwargs.pop('tags', None)
		inherit_sections = kwargs.pop('inherit', False)
		for entry in value.split():
			(cls_name, instance_name) = (None, None)
			tmp = entry.split(':', 1)
			if len(tmp) == 2:
				(cls_name, instance_name) = tmp
			elif len(tmp) == 1:
				cls_name = tmp[0]
			cls_new = cls.get_class(cls_name.strip())
			bind_value = '%s:%s' % (cls_new.get_bind_class_name(cls_name), instance_name)
			if not instance_name:
				instance_name = cls_new.__name__.split('.')[-1]
				bind_value = cls_new.get_bind_class_name(cls_name)
			cls_config = config.change_view(view_class='TaggedConfigView',
				set_classes=[cls_new], set_sections=None, set_names=[instance_name],
				add_tags=tags or [], inherit_sections=inherit_sections)
			yield InstanceFactory(bind_value, cls_new, cls_config, instance_name, **kwargs)
	bind = classmethod(bind)

	def get_object_name(self):
		return self._name
