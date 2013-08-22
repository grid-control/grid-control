from python_compat import *
from exceptions import *
import utils

class AbstractObject:
	def __init__(self):
		raise AbstractError

	# Modify the module search path for the class
	def dynamicLoaderPath(cls, path = []):
		if not hasattr(cls, 'moduleMap'):
			(cls.moduleMap, cls.modPaths) = ({}, [])
		splitUpFun = lambda x: rsplit(cls.__module__, ".", x)[0]
		cls.modPaths = utils.uniqueListRL(path + cls.modPaths + map(splitUpFun, range(cls.__module__.count(".") + 1)))
	dynamicLoaderPath = classmethod(dynamicLoaderPath)

	def getClass(cls, name):
		mjoin = lambda x: str.join('.', x)
		# Yield search paths
		def searchPath(cname):
			cls.moduleMap = dict(map(lambda (k, v): (k.lower(), v), cls.moduleMap.items()))
			name = cls.moduleMap.get(cname.lower(), cname) # resolve module mapping
			yield name
			for path in cls.modPaths + AbstractObject.pkgPaths:
				if not '.' in name:
					yield mjoin([path, name.lower(), name])
				yield mjoin([path, name])

		for modName in searchPath(name):
			parts = modName.split('.')
			try: # Try to import missing modules
				for pkg in map(lambda (i, x): mjoin(parts[:i+1]), enumerate(parts[:-1])):
					if pkg not in sys.modules:
						__import__(pkg)
				newcls = getattr(sys.modules[mjoin(parts[:-1])], parts[-1])
				assert(not isinstance(newcls, type(sys.modules['grid_control'])))
			except:
				continue
			if issubclass(newcls, cls):
				return newcls
			raise ConfigError('%s is not of type %s' % (newcls, cls))
		raise ConfigError('%s "%s" does not exist in\n\t%s!' % (cls.__name__, name, str.join('\n\t', searchPath(name))))
	getClass = classmethod(getClass)

	def getInstance(cls, name, *args, **kwargs):
		clsType = None
		try:
			clsType = cls.getClass(name)
			return clsType(*args, **kwargs)
		except GCError:
			raise
		except:
			raise RethrowError('Error while creating instance of type %s (%s)' % (name, clsType))
	open = classmethod(getInstance)
	getInstance = classmethod(getInstance)

AbstractObject.pkgPaths = []
