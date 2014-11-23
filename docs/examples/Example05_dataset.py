from grid_control.datasets.nickname_base import NickNameProducer

class MyNick(NickNameProducer):
	def getName(self, oldnick, dataset, block):
		return oldnick + "_changed"
