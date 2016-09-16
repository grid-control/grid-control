from hpfwk import Plugin

class MyNick(Plugin.get_class('NickNameProducer')):
	def getName(self, oldnick, dataset, block):
		if oldnick:
			return oldnick + '_changed'
		return 'newnick'
