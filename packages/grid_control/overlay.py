from grid_control import AbstractObject

# Class to translate domain specific option values
class ConfigOverlay(AbstractObject):
	def __init__(self, config):
		self.config = config

	def rewrite(self, selector, rewriteFun):
		pass

	def rewriteList(self, selector, rewriteFun):
		pass

ConfigOverlay.dynamicLoaderPath()
ConfigOverlay.moduleMap["verbatim"] = "ConfigOverlay"
