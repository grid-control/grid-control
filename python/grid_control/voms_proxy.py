import os, popen2

from . import Proxy

class Voms(Proxy):
	def __init__(self):
		self.proc = popen2.Popen4('voms-proxy-init')
