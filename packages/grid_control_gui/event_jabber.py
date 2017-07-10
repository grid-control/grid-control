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

import os, stat, time
from grid_control.event_base import LocalEventHandler
from grid_control.utils.file_tools import SafeFile
from hpfwk import clear_current_exception, ignore_exception


class JabberAlarm(LocalEventHandler):
	alias_list = ['jabber']
	config_section_list = LocalEventHandler.config_section_list + ['jabber']

	def __init__(self, config, name, task):
		LocalEventHandler.__init__(self, config, name, task)
		self._source_jid = config.get('source jid', on_change=None)
		self._target_jid = config.get('target jid', on_change=None)
		password_fn = config.get_fn('source password file')
		os.chmod(password_fn, stat.S_IRUSR)
		# password in variable name removes it from debug log!
		self._source_password = SafeFile(password_fn).read_close().strip()
		try:  # xmpp contains many deprecated constructs
			import warnings
			warnings.simplefilter('ignore', DeprecationWarning)
		except Exception:
			clear_current_exception()
		self._xmpp = ignore_exception(Exception, None, __import__, 'xmpp')
		if self._xmpp is None:
			try:
				import grid_control_gui.xmpp
				self._xmpp = grid_control_gui.xmpp
			except Exception:
				raise Exception('Unable to load jabber library!')

	def on_task_finish(self, job_len):
		try:
			jid = self._xmpp.protocol.JID(self._source_jid)
			xmpp_client = self._xmpp.Client(jid.getDomain(), debug=[])
			con = ignore_exception(Exception, None, xmpp_client.connect)
			if not con:
				return self._log.warning('Could not connect to jabber server!')
			auth = xmpp_client.auth(jid.getNode(), self._source_password, resource=jid.getResource())
			if not auth:
				return self._log.warning('Could not authenticate to jabber server!')
			text = 'Task %s finished!' % self._task.get_description().task_name
			xmpp_client.send(self._xmpp.protocol.Message(self._target_jid, text))
			time.sleep(1)  # Stay connected until delivered
		except Exception:
			self._log.exception('Error while sending message to jabber server')
