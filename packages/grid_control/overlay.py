from grid_control import AbstractObject

# Class to translate domain specific option values
class ConfigOverlay(AbstractObject):
	def __init__(self, config):
		pass

ConfigOverlay.dynamicLoaderPath()
ConfigOverlay.moduleMap["verbatim"] = "ConfigOverlay"
