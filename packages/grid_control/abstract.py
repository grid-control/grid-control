from python_compat import rsplit
from exceptions import *
import utils, logging

# Abstract class taking care of dynamic class loading 
class LoadableObject(object):
	# Modify the module search path for the class - parent is used by NamedObject to impersonate caller
	def registerObject(cls, searchPath = [], base = None):
		if not base:
			base = cls
		if not hasattr(base, 'moduleMap'):
			(base.moduleMap, base.moduleMapDynamic, base.modPaths) = ({}, {}, [])
		splitUpFun = lambda x: rsplit(base.__module__, ".", x)[0]
		base.modPaths = utils.uniqueListRL(searchPath + base.modPaths + map(splitUpFun, range(base.__module__.count(".") + 1)))
	registerObject = classmethod(registerObject)


	def getClass(cls, clsName):
		log = logging.getLogger('classloader.%s' % cls.__name__)
		log.log(logging.DEBUG1, 'Loading class %s' % clsName)
		# resolve class name/alias to fully qualified class path
		def resolveClassName(name):
			resolveFun = cls.moduleMapDynamic.get(name.lower(), lambda x: x)
			classMap = dict(map(lambda (k, v): (k.lower(), v), cls.moduleMap.items()))
			cname = resolveFun(classMap.get(name.lower(), name))
			if cname == name:
				return name
			return resolveClassName(cname)
		clsName = resolveClassName(clsName)
		log.log(logging.DEBUG2, 'Loading resolved class %s' % clsName)
		mjoin = lambda x: str.join('.', x)
		# Yield search paths
		def searchPath(fqName):
			yield fqName
			for path in cls.modPaths + LoadableObject.pkgPaths:
				if not '.' in fqName:
					yield mjoin([path, fqName.lower(), fqName])
				yield mjoin([path, fqName])

		for modName in searchPath(clsName):
			parts = modName.split('.')
			try: # Try to import missing modules
				for pkg in map(lambda (i, x): mjoin(parts[:i+1]), enumerate(parts[:-1])):
					if pkg not in sys.modules:
						log.log(logging.DEBUG3, 'Importing module %s' % pkg)
						__import__(pkg)
				newcls = getattr(sys.modules[mjoin(parts[:-1])], parts[-1])
				assert(not isinstance(newcls, type(sys.modules['grid_control'])))
			except:
				log.log(logging.DEBUG2, 'Unable to import module %s' % modName)
				continue
			if issubclass(newcls, cls):
				log.log(logging.DEBUG1, 'Successfully loaded class %s:%s' % (newcls.__module__, newcls.__name__))
				return newcls
			raise ConfigError('%s is not of type %s' % (newcls, cls))
		raise ConfigError('%s "%s" does not exist in\n\t%s!' % (cls.__name__, clsName, str.join('\n\t', searchPath(clsName))))
	getClass = classmethod(getClass)


	def getInstance(cls, clsName, *args, **kwargs):
		clsType = None
		try:
			clsType = cls.getClass(clsName)
			return clsType(*args, **kwargs)
		except GCError:
			raise
		except:
			raise RethrowError('Error while creating instance of type %s (%s)' % (clsName, clsType))
	open = classmethod(getInstance)
	getInstance = classmethod(getInstance)

LoadableObject.pkgPaths = []


# NamedObject provides methods used by config.getClass methods to determine relevant sections
class NamedObject(LoadableObject):
	def __init__(self, config, name):
		self._name = name


	def getObjectName(self):
		return self._name


	# Modify the module search path for the class
	def registerObject(cls, searchPath = [], tagName = None, defaultName = None):
		if tagName or not hasattr(cls, 'tagName'):
			cls.tagName = tagName
		if defaultName or not hasattr(cls, 'defaultName'):
			cls.defaultName = defaultName
		LoadableObject.registerObject(searchPath, base = cls)
	registerObject = classmethod(registerObject)


	# Collects named config section
	def getAllConfigSections(cls, instName):
		def collectSections(clsCurrent): # Collect sections based on class hierarchie
			if clsCurrent != NamedObject:
				for section in clsCurrent.getConfigSections():
					if section.lower() != instName.lower():
						yield section + ' ' + instName
					yield section
				for clsBase in clsCurrent.__bases__:
					for section in collectSections(clsBase):
						yield section
		return list(collectSections(cls))
	getAllConfigSections = classmethod(getAllConfigSections)


	def getConfigSections(cls):
		return []
	getConfigSections = classmethod(getConfigSections)


	# Function to quickly create getConfigSections class members returning a fixed section list
	def createFunction_getConfigSections(clsParent, sections):
		def getConfigSectionsTemplate(cls):
			return sections
		return classmethod(getConfigSectionsTemplate)
	createFunction_getConfigSections = classmethod(createFunction_getConfigSections)



# General purpose class factory
class ClassFactory:
	def __init__(self, config, opt, optMerge, **kwargs):
		proxyList = config.getClassList(opt[0], opt[1], **kwargs)
		self._mergeCls = None
		if len(proxyList) > 1:
			self._mergeCls = config.getClass(optMerge[0], optMerge[1], **kwargs)
		self._classList = map(lambda clsProxy: lambda *cargs, **ckwargs: clsProxy.getInstance(*cargs, **ckwargs), proxyList)

	# Get single instance by merging multiple sub instances if necessary
	def getInstance(self, *args, **kwargs):
		if len(self._classList) == 1:
			return self._classList[0](*args, **kwargs)
		elif len(self._classList) > 1:
			return self._mergeCls.getInstance(self._classList, *args, **kwargs)


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

	def getInstance(self, *args, **kwargs):
		cls = self._baseClass.getClass(self._instClassName)
		if issubclass(cls, NamedObject):
			config = self._config.newClass(cls, [self._instName]).addTags(self._tags)
			return cls(config, self._instName, *args, **kwargs)
		return cls(*args, **kwargs)
