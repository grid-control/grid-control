def initGC():
	import os, sys, grid_control.utils
	sys.path.insert(1, grid_control.utils.cleanPath(os.path.dirname(__file__)))
	sys.path.append(grid_control.utils.cleanPath(os.getcwd()))

	# Package discovery
	for pkgName in os.listdir(os.path.dirname(__file__)):
		if os.path.isdir(os.path.join(os.path.dirname(__file__), pkgName)) and not pkgName.startswith('.'):
			grid_control.utils.AbstractObject.pkgPaths.append(pkgName)

	for pkgName in grid_control.utils.AbstractObject.pkgPaths:
		sys.path.insert(2, os.path.join(os.path.dirname(__file__), pkgName))
		yield pkgName

for pkgName in initGC():
	exec('from %s import *' % pkgName)
