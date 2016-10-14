from hpfwk import Plugin


class MyNick(Plugin.get_class('NickNameProducer')):
	def get_name(self, oldnick, dataset, block):
		if oldnick:
			return oldnick + '_changed'
		return 'newnick'
