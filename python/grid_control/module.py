# Generic base class for job modules
# instantiates named class instead (default is usermod)

import sys
from grid_control import ConfigError

class Module:
	def __init__(self):
		pass

	def open(name = 'usermod', *args):
		try:
			cls = getattr(sys.modules['grid_control'], name)
		except:
			raise ConfigError("Module '%s' does not exist!" % name)

		return cls(*args)

	open = staticmethod(open)
