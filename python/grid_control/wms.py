# Generic base class for grid proxies
# instantiates named class instead (default is VomsProxy)

import sys

class WMS:
	def __init__(self):
		pass

	def open(name = 'Glite', *args):
		cls = getattr(sys.modules['grid_control'], name)
		return cls(*args)
	open = staticmethod(open)
