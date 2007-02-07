# Generic base class for grid proxies
# instantiates named class instead (default is VomsProxy)

import sys

class Proxy:
	def __init__(cls):
		pass

	def open(name = 'VomsProxy'):
		cls = getattr(sys.modules['grid_control'], name)
		return cls()
	open = staticmethod(open)
