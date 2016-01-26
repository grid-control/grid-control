#-#  Copyright 2013-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, sys, logging
from hpfwk import NestedException

class PluginError(NestedException):
	pass

# Abstract class taking care of dynamic class loading 
class Plugin(object):
	alias = []
	configSections = []
	moduleMap = {}

	def getClass(cls, clsName, modulePaths = []):
		log = logging.getLogger('classloader.%s' % cls.__name__)
		log.log(logging.DEBUG1, 'Loading class %s' % clsName)

		# resolve class name/alias to complete class path 'myplugin -> module.submodule.MyPlugin'
		clsMap = dict(map(lambda k_v: (k_v[0].lower(), k_v[1]), cls.moduleMap.items()))
		clsSearchList = [clsName]
		clsNameStored = clsName
		clsFormat = lambda cls: '%s:%s' % (cls.__module__, cls.__name__)
		clsProcessed = []
		while clsSearchList:
			clsName = clsSearchList.pop()
			if clsName in clsProcessed: # Prevent lookup circles
				continue
			clsProcessed.append(clsName)
			clsModuleList = []
			if '.' in clsName: # module.submodule.class specification
				clsNameParts = clsName.split('.')
				clsName = clsNameParts[-1]
				clsModuleName = str.join('.', clsNameParts[:-1])
				log.log(logging.DEBUG2, 'Importing module %s' % clsModuleName)
				oldSysPath = list(sys.path)
				try:
					sys.path.extend(modulePaths)
					clsModuleList = [__import__(clsModuleName, {}, {}, [clsName])]
				except Exception:
					log.log(logging.DEBUG2, 'Unable to import module %s' % clsModuleName)
				sys.path = oldSysPath
			elif hasattr(sys.modules['__main__'], clsName):
				clsModuleList.append(sys.modules['__main__'])

			clsLoadedList = []
			for clsModule in clsModuleList:
				log.log(logging.DEBUG2, 'Searching for class %s:%s' % (clsModule.__name__, clsName))
				try:
					clsLoadedList.append(getattr(clsModule, clsName))
				except Exception:
					log.log(logging.DEBUG2, 'Unable to import class %s:%s' % (clsModule.__name__, clsName))

			for clsLoaded in clsLoadedList:
				if issubclass(clsLoaded, cls):
					log.log(logging.DEBUG1, 'Successfully loaded class %s' % clsFormat(clsLoaded))
					return clsLoaded
				log.log(logging.DEBUG, '%s is not of type %s!' % (clsFormat(clsLoaded), clsFormat(cls)))

			clsMapResult = clsMap.get(clsName.lower(), [])
			if isinstance(clsMapResult, str):
				clsSearchList.append(clsMapResult)
			else:
				clsSearchList.extend(clsMapResult)
		raise PluginError('Unable to load %r of type %r - tried:\n\t%s' % (clsNameStored, clsFormat(cls), str.join('\n\t', clsProcessed)))
	getClass = classmethod(getClass)

	# Get an instance of a derived class by specifying the class name and constructor arguments
	def getInstance(cls, clsName, *args, **kwargs):
		clsType = None
		try:
			clsType = cls.getClass(clsName)
			return clsType(*args, **kwargs)
		except Exception:
			raise PluginError('Error while creating instance of type %s (%s)' % (clsName, clsType))
	getInstance = classmethod(getInstance)

Plugin.pkgPaths = []

# Init plugin search paths
def initPlugins(basePath):
	# Package discovery
	for pkgName in filter(lambda p: os.path.isdir(os.path.join(basePath, p)), os.listdir(basePath)):
		pluginFile = os.path.join(basePath, pkgName, '.PLUGINS')
		if os.path.exists(pluginFile):
			__import__(pkgName) # Trigger initialisation of module
			for line in map(str.strip, open(pluginFile)):
				if line and not line.endswith(':'):
					tmp = line.split()
					(modulePath, module) = (tmp[0], tmp[1])
					for pluginName in tmp[1:]:
						Plugin.moduleMap.setdefault(pluginName, []).append('%s.%s' % (modulePath, module))

# NamedPlugin provides functionality to name plugin instances
class NamedPlugin(Plugin):
	defaultName = None
	tagName = None

	def __init__(self, config, name):
		self._name = name

	def getObjectName(self):
		return self._name
