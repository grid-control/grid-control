# | Copyright 2013-2016 Karlsruhe Institute of Technology
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
from grid_control.monitoring import Monitoring
from hpfwk import clear_current_exception


class JabberAlarm(Monitoring):
	alias_list = ['jabber']
	config_section_list = Monitoring.config_section_list + ['jabber']

	def __init__(self, config, name, task):
		Monitoring.__init__(self, config, name, task)
		self._source_jid = config.get('source jid', on_change=None)
		self._target_jid = config.get('target jid', on_change=None)
		password_fn = config.get_path('source password file')
		os.chmod(password_fn, stat.S_IRUSR)
		# password in variable name removes it from debug log!
		self._source_password = open(password_fn).read().strip()
		try:  # xmpp contains many deprecated constructs
			import warnings
			warnings.simplefilter('ignore', DeprecationWarning)
		except Exception:
			clear_current_exception()
		self._xmpp = None
		try:
			import xmpp
			self._xmpp = xmpp
		except Exception:
			try:
				import grid_control_gui.xmpp
				self._xmpp = grid_control_gui.xmpp
			except Exception:
				raise Exception('Unable to load jabber library!')

	def on_task_finish(self, job_len):
		jid = self._xmpp.protocol.JID(self._source_jid)
		xmpp_client = self._xmpp.Client(jid.getDomain(), debug=[])
		con = xmpp_client.connect()
		if not con:
			self._log.warning('Could not connect to jabber server!')
			return
		auth = xmpp_client.auth(jid.getNode(), self._source_password, resource=jid.getResource())
		if not auth:
			self._log.warning('Could not authenticate to jabber server!')
			return
		text = 'Task %s finished!' % self._task.task_id
		xmpp_client.send(self._xmpp.protocol.Message(self._target_jid, text))
		time.sleep(1)  # Stay connected until delivered
