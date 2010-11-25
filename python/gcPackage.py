import os, sys
cleanPath = lambda *args: os.path.normpath(os.path.expanduser(os.path.join(*args)))
sys.path.insert(1, cleanPath(os.path.dirname(__file__)))
sys.path.append(cleanPath(os.getcwd()))

# Package discovery
pkgList = []
for pkgName in os.listdir(os.path.dirname(__file__)):
	if os.path.isdir(os.path.join(os.path.dirname(__file__), pkgName)) and not pkgName.startswith('.'):
		pkgList.append(pkgName)

for pkgName in pkgList:
	sys.path.insert(2, os.path.join(os.path.dirname(__file__), pkgName))
	exec 'from %s import *' % pkgName

utils.AbstractObject.pkgPaths.extend(pkgList)
del pkgName, pkgList
