#-#  Copyright 2013-2014 Karlsruhe Institute of Technology
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

import time, xmpp, stat, os
from grid_control import Monitoring

class JabberAlarm(Monitoring):
	getConfigSections = Monitoring.createFunction_getConfigSections(['jabber'])

	def __init__(self, config, name, task, submodules = []):
		Monitoring.__init__(self, config, name, task)
		self.source_jid = config.get('source jid')
		self.target_jid = config.get('target jid')
		pwPath = config.getPath('source password file')
		os.chmod(pwPath, stat.S_IRUSR)
		# password in variable name removes it from debug log!
		self.source_password = open(pwPath).read().strip()

	def onTaskFinish(self, nJobs):
		jid = xmpp.protocol.JID(self.source_jid)
		cl = xmpp.Client(jid.getDomain(), debug=[])
		con = cl.connect()
		if not con:
			print 'could not connect!'
			return
		auth = cl.auth(jid.getNode(), self.source_password, resource = jid.getResource())
		if not auth:
			print 'could not authenticate!'
			return
		text = 'Task %s finished!' % self.task.taskID
		mid = cl.send(xmpp.protocol.Message(self.target_jid, text))
		time.sleep(1) # Stay connected until delivered
