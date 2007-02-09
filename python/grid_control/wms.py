# Generic base class for grid proxies
# instantiates named class instead (default is VomsProxy)

import os
from grid_control import AbstractObject

class WMS(AbstractObject):
	def __init__(self, config, module):
		self.config = config
		self.module = module
		self.workDir = config.getPath('global', 'workdir')
