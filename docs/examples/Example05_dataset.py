from hpfwk import Plugin

class MyNick(Plugin.getClass('NickNameProducer')):
	def getName(self, oldnick, dataset, block):
		return oldnick + "_changed"
