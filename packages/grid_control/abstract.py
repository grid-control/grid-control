from python_compat import *
from exceptions import *
import utils, logging

# Abstract class taking care of dynamic class loading 
class LoadableObject(object):
	def __init__(self):
		raise AbstractError

	# Modify the module search path for the class
	def registerObject(cls, searchPath = []):
		if not hasattr(cls, 'moduleMap'):
			(cls.moduleMap, cls.moduleMapDynamic, cls.modPaths) = ({}, {}, [])
		splitUpFun = lambda x: rsplit(cls.__module__, ".", x)[0]
		cls.modPaths = utils.uniqueListRL(searchPath + cls.modPaths + map(splitUpFun, range(cls.__module__.count(".") + 1)))
	registerObject = classmethod(registerObject)


	def getClass(cls, clsName):
		log = logging.getLogger('classloader.%s' % cls.__name__)
		log.log(logging.DEBUG1, 'Loading class %s' % clsName)
		# resolve class name/alias to fully qualified class path
		def resolveClassName(name):
			cname = cls.moduleMapDynamic.get(name, name)
			cname = cls.moduleMap.get(name, name)
			if cname == name:
				return name
			return resolveClassName(cname)
		clsName = resolveClassName(clsName)
		log.log(logging.DEBUG2, 'Loading resolved class %s' % clsName)
		mjoin = lambda x: str.join('.', x)
		# Yield search paths
		def searchPath(cname):
			cls.moduleMap = dict(map(lambda (k, v): (k.lower(), v), cls.moduleMap.items()))
			fqName = cls.moduleMap.get(cname.lower(), cname) # resolve module mapping
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


	def getLocalConfigSections(self):
		if self.__class__.__name__.lower() == self._name.lower():
			return [self._name]
		return [self.__class__.__name__ + ' ' + self._name]


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
	def __init__(self, cls, config, opt, optMerge, scope = None):
		self._proxyList = config.getClassList(opt[0], opt[1], cls = cls, scope = scope)
		self._mergeCls = None
		if len(self._proxyList) > 1:
			self._mergeCls = config.getClass(optMerge[0], optMerge[1], cls = cls, scope = scope)

	# Get single instance by merging multiple sub instances if necessary
	def getInstance(self, *args, **kwargs):
		clsList = []
		for clsUser in self._proxyList:
			try:
				clsList.append(clsUser(*args, **kwargs))
			except:
				raise RethrowError('Unable to load %s' % clsUser)
		if len(clsList) == 1:
			return clsList[0]
		elif len(clsList) > 1:
			return self._mergeCls(*clsList)
