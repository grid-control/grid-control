#-#  Copyright 2013-2015 Karlsruhe Institute of Technology
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

import sys, logging
from grid_control import utils
from grid_control.exceptions import NestedException
from python_compat import rsplit, set

class PluginError(NestedException):
	pass

# Abstract class taking care of dynamic class loading 
class LoadableObject(object):
	configSections = []
	moduleMap = {}

	def getClass(cls, clsName, modulePaths = []):
		log = logging.getLogger('classloader.%s' % cls.__name__)
		log.log(logging.DEBUG1, 'Loading class %s' % clsName)

		# resolve class name/alias to complete class path 'UserTask -> grid_control.tasks.user_task.UserTask'
		clsMap = dict(map(lambda (k, v): (k.lower(), v), cls.moduleMap.items()))
		clsSearchList = [clsName]
		clsNameStored = clsName
		clsFormat = lambda cls: '%s:%s' % (cls.__module__, cls.__name__)
		clsProcessed = set()
		while clsSearchList:
			clsName = clsSearchList.pop()
			if clsName in clsProcessed: # Prevent lookup circles
				continue
			clsProcessed.add(clsName)
			clsModuleList = []
			if '.' in clsName: # module.submodule.class specification
				clsModuleName, clsName = rsplit(clsName, '.', 1)
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

LoadableObject.pkgPaths = []


# NamedObject provides methods used by config.getClass methods to determine relevant sections
class NamedObject(LoadableObject):
	defaultName = None
	tagName = None

	def __init__(self, config, name):
		self._name = name

	def getObjectName(self):
		return self._name


# General purpose class factory
class ClassFactory:
	def __init__(self, config, base_opt_def, merge_opt_base, **kwargs):
		self._proxyList = config.getClassList(base_opt_def[0], base_opt_def[1], **kwargs)
		self._mergeCls = None
		if len(self._proxyList) > 1:
			self._mergeCls = config.getClass(merge_opt_base[0], merge_opt_base[1], **kwargs)

	# Get single instance by merging multiple sub instances if necessary
	def getInstance(self, *args, **kwargs):
		if len(self._proxyList) == 1:
			return self._proxyList[0].getInstance(*args, **kwargs)
		elif len(self._proxyList) > 1:
			return self._mergeCls.getInstance(self._proxyList, *args, **kwargs)


# Needed by getClass / getClasses to wrap the fixed arguments to the instantiation / name of the instance
class ClassWrapper:
	def __init__(self, baseClass, value, config, tags, inherit, defaultName):
		(self._baseClass, self._config, self._tags, self._inherit) = (baseClass, config, tags, inherit)
		(self._instClassName, self._instName) = utils.optSplit(value, ':')
		if self._instName == '':
			if not defaultName:
				self._instName = self._instClassName.split('.')[-1] # Default: (non fully qualified) class name as instance name
			else:
				self._instName = defaultName

	def __eq__(self, other): # Used to check for changes compared to old
		return str(self) == str(other)

	def __repr__(self):
		return '<class wrapper for %r (base: %r)>' % (str(self), self._baseClass.__name__)

	def __str__(self):  # Used to serialize config setting
		if self._instName == self._instClassName.split('.')[-1]: # take care of fully qualified class names
			return self._instClassName
		return '%s:%s' % (self._instClassName, self._instName)

	def getObjectName(self):
		return self._instName

	def getClass(self):
		from grid_control.config import SimpleConfigView
		configLoader = self._config.changeView(viewClass = SimpleConfigView, setSections = ['global'])
		modulePaths = configLoader.getPaths('module paths', mustExist = False, onChange = None)
		return self._baseClass.getClass(self._instClassName, modulePaths)

	def getInstance(self, *args, **kwargs):
		from grid_control.config import TaggedConfigView
		cls = self.getClass()
		if issubclass(cls, NamedObject):
			config = self._config.changeView(viewClass = TaggedConfigView,
				setClasses = [cls], setSections = None, setNames = [self._instName],
				addTags = self._tags, inheritSections = self._inherit)
			args = [config, self._instName] + list(args)
		try:
			return cls(*args, **kwargs)
		except Exception:
			raise PluginError('Error while creating instance of type %s (%s)' % (cls, str(self)))
