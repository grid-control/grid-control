def initGC():
	import os, sys
	sys.path.insert(1, os.path.dirname(__file__))
	import grid_control.utils
	sys.path.append(grid_control.utils.cleanPath(os.getcwd()))

	# Package discovery
	for pkgName in filter(lambda x: x != 'grid_control', os.listdir(os.path.dirname(__file__))):
		if os.path.exists(os.path.join(os.path.dirname(__file__), pkgName, '__init__.py')):
			grid_control.abstract.AbstractObject.pkgPaths.append(pkgName)

	for pkgName in ['grid_control'] + grid_control.abstract.AbstractObject.pkgPaths:
		sys.path.insert(2, os.path.join(os.path.dirname(__file__), pkgName))
		yield pkgName

for pkgName in initGC():
	exec('from %s import *' % pkgName)
