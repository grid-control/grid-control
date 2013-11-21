import time, xmpp, stat, os
from grid_control import Monitoring

class JabberAlarm(Monitoring):
	def __init__(self, config, task, submodules = []):
		Monitoring.__init__(self, config, task)
		self.source_jid = config.get('jabber', 'source jid')
		self.target_jid = config.get('jabber', 'target jid')
		pwPath = config.getPath('jabber', 'source password file')
		os.chmod(pwPath, stat.S_IRUSR)
		self.source_pw = open(pwPath).read().strip()

	def onTaskFinish(self, nJobs):
		jid = xmpp.protocol.JID(self.source_jid)
		cl = xmpp.Client(jid.getDomain(), debug=[])
		con = cl.connect()
		if not con:
			print 'could not connect!'
			return
		auth = cl.auth(jid.getNode(), self.source_pw, resource = jid.getResource())
		if not auth:
			print 'could not authenticate!'
			return
		text = 'Task %s finished!' % self.task.taskID
		mid = cl.send(xmpp.protocol.Message(self.target_jid, text))
		time.sleep(1) # Stay connected until delivered
