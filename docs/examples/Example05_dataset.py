from hpfwk import Plugin

class MyNick(Plugin.getClass('NickNameProducer')):
	def getName(self, oldnick, dataset, block):
		if oldnick:
			return oldnick + '_changed'
		return 'newnick'
