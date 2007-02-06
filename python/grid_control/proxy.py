# Generic base class for grid proxies
# instantiates named class instead (default is VomsProxy)

import sys

class Proxy(object):
	def __new__(cls, name = 'VomsProxy', **kwargs):
		# __new__ is called with the class type to insantiated
		# if we are instructed to instantiate a Proxy object
		# we instead instantiate the subclass from `name'
		if cls == Proxy:
			cls = getattr(sys.modules['grid_control'], name)
			return cls.__new__(cls, **kwargs)
		else:
			# we were called from our child class,
			# so call the constructor from the superclass
			# `object' here
			return object.__new__(cls)

