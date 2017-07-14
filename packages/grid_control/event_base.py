# | Copyright 2017 Karlsruhe Institute of Technology
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

import os
from grid_control.gc_plugin import ConfigurablePlugin, NamedPlugin
from grid_control.utils.algos import dict_union
from python_compat import imap, lchain, lmap


class EventHandlerManager(ConfigurablePlugin):
	alias_list = ['NullEventHandlerManager', 'null']


class LocalEventHandler(NamedPlugin):  # Local event handler base class
	alias_list = ['NullLocalEventHandler', 'null']
	config_section_list = NamedPlugin.config_section_list + ['events']
	config_tag_name = 'event'

	def __init__(self, config, name, task):
		NamedPlugin.__init__(self, config, name)
		self._task = task

	def on_job_output(self, wms, job_obj, jobnum, exit_code):
		pass

	def on_job_state_change(self, job_db_len, jobnum, job_obj, old_state, new_state, reason=None):
		pass

	def on_job_submit(self, wms, job_obj, jobnum):
		pass

	def on_job_update(self, wms, job_obj, jobnum, data):
		pass

	def on_task_finish(self, job_len):
		pass

	def on_workflow_finish(self):
		pass


class RemoteEventHandler(NamedPlugin):  # Remote monitoring base class
	alias_list = ['NullRemoteEventHandler', 'null']
	config_section_list = NamedPlugin.config_section_list + ['events']
	config_tag_name = 'monitor'

	def get_file_list(self):
		return []

	def get_mon_env_dict(self):
		return {'GC_MONITORING': str.join(' ', imap(os.path.basename, self.get_script()))}

	def get_script(self):  # Script to call later on
		return []


class MultiLocalEventHandler(LocalEventHandler):
	alias_list = ['multi']

	def __init__(self, config, name, handler_list, task):
		LocalEventHandler.__init__(self, config, name, task)
		self._handlers = handler_list

	def on_job_output(self, wms, job_obj, jobnum, exit_code):
		for handler in self._handlers:
			handler.on_job_output(wms, job_obj, jobnum, exit_code)

	def on_job_state_change(self, job_db_len, jobnum, job_obj, old_state, new_state, reason=None):
		for handler in self._handlers:
			handler.on_job_state_change(job_db_len, jobnum, job_obj, old_state, new_state, reason)

	def on_job_submit(self, wms, job_obj, jobnum):
		for handler in self._handlers:
			handler.on_job_submit(wms, job_obj, jobnum)

	def on_job_update(self, wms, job_obj, jobnum, data):
		for handler in self._handlers:
			handler.on_job_update(wms, job_obj, jobnum, data)

	def on_task_finish(self, job_len):
		for handler in self._handlers:
			handler.on_task_finish(job_len)

	def on_workflow_finish(self):
		for handler in self._handlers:
			handler.on_workflow_finish()


class MultiRemoteEventHandler(RemoteEventHandler):
	alias_list = ['multi']

	def __init__(self, config, name, handler_list):
		RemoteEventHandler.__init__(self, config, name)
		self._handlers = handler_list

	def get_file_list(self):
		return lchain(lmap(lambda h: h.get_file_list(), self._handlers) + [self.get_script()])

	def get_mon_env_dict(self):
		tmp = RemoteEventHandler.get_mon_env_dict(self)
		return dict_union(*(lmap(lambda m: m.get_mon_env_dict(), self._handlers) + [tmp]))

	def get_script(self):
		return lchain(imap(lambda h: h.get_script(), self._handlers))
