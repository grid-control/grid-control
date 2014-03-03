from grid_control import utils, LoadableObject, AbstractError

class InfoScanner(LoadableObject):
	def __init__(self, config):
		pass

	def getGuards(self):
		return ([], [])

	def getEntriesVerbose(self, level, *args):
		utils.vprint('    ' * level + 'Collecting information with %s...' % self.__class__.__name__, 1)
		for c, n, l in zip(args, ['Path', 'Metadata', 'Events', 'SE list', 'Objects'], [1, 2, 1, 2, 2]):
			utils.vprint('    ' * level + '  %s: %s' % (n, c), l)
		return self.getEntries(*args)

	def getEntries(self, path, metadata, events, seList, objStore):
		raise AbstractError
InfoScanner.registerObject()
