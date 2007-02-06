# Generic base class for grid proxies
# instantiates named class instead (default VomsProxy)

class Proxy:
	def __new__(cls, type = 'VomsProxy', **kwargs):
		cls = __module__.__dict__[type]
		return cls.__new__(cls, **kwargs)
