# Generic base class for grid proxies
# instantiates named class instead (default VomsProxy)

import sys

class Proxy(object):
	def __new__(cls, name = 'VomsProxy', **kwargs):
		if cls == Proxy:
			cls = getattr(sys.modules['grid_control'], name)
			return cls.__new__(cls, **kwargs)
		else:
			return object.__new__(cls)

