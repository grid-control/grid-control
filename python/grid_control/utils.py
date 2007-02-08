import os
from grid_control import InstallationError

def searchPathFind(program):
	try:
		path = os.environ['PATH'].split(':')
	except:
		# Hmm, something really wrong
		path = ['/bin', '/usr/bin', '/usr/local/bin']

	for dir in path:
		fname = os.path.join(dir, program)
		if os.path.exists(fname):
			return fname
	raise InstallationError("%s not found" % program)
