from grid_control.abstract import LoadableObject

class MyNick(LoadableObject.getClass('NickNameProducer')):
	def getName(self, oldnick, dataset, block):
		return oldnick + "_changed"
