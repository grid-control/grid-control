# | Copyright 2013-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, sys, logging
from hpfwk.hpf_exceptions import ExceptionCollector, NestedException
from hpfwk.hpf_logging import init_hpf_logging

init_hpf_logging() # needed for additional logging levels

class PluginError(NestedException):
	pass

# Wrapper class to fix plugin arguments
class InstanceFactory(object):
	def __init__(self, bindValue, cls, *args, **kwargs):
		(self._bindValue, self._cls, self._args, self._kwargs) = (bindValue, cls, args, kwargs)

	def _fmt(self, args, kwargs, addEllipsis = False):
		if not logging.getLogger().isEnabledFor(logging.INFO1):
			return repr('%s.%s' % (self._cls.__module__, self._cls.__name__))
		args_str_list = []
		for arg in args:
			args_str_list.append(repr(arg))
		for k_v in kwargs.items():
			args_str_list.append('%s=%r' % k_v)
		if addEllipsis:
			args_str_list.append('...')
		return '%s(%s)' % (self._cls.__name__, str.join(', ', args_str_list))

	def __eq__(self, other): # Used to check for changes compared to old
		return self.bindValue() == other.bindValue()

	def __repr__(self):
		return '<instance factory for %s>' % self._fmt(self._args, self._kwargs, addEllipsis = True)

	def getClass(self):
		return self._cls

	def getBoundInstance(self, *args, **kwargs):
		args = self._args + args
		kwargs = dict(list(self._kwargs.items()) + list(kwargs.items()))
		try:
			return self._cls(*args, **kwargs)
		except Exception:
			raise PluginError('Error while creating instance: %s' % self._fmt(args, kwargs))

	def bindValue(self):
		return self._bindValue

# Abstract class taking care of dynamic class loading
class Plugin(object):
	alias = []
	configSections = []

	_pluginMap = {}
	_classMap = {}
	_clsCache = {}

	def registerClass(cls, module_name, cls_name, alias_list, base_cls_names):
		cls_path = '%s.%s' % (module_name, cls_name)
		depth = len(base_cls_names)
		for name in [cls_name] + alias_list:
			cls_name_entry = cls._pluginMap.setdefault(name.lower(), [])
			if cls_path not in cls_name_entry:
				if name != cls_name:
					depth = len(base_cls_names) + 1
				cls_name_entry.append((-depth, cls_path))
			for base_cls_name in base_cls_names:
				cls_base_entry = cls._classMap.setdefault(base_cls_name.lower(), [])
				tmp = {name: cls_name, 'depth': depth}
				if tmp not in cls_base_entry:
					cls_base_entry.append(tmp)
	registerClass = classmethod(registerClass)

	def getClassNames(cls):
		for parent in cls.__bases__:
			if hasattr(parent, 'alias') and (cls.alias == parent.alias):
				return [cls.__name__]
		return [cls.__name__] + cls.alias
	getClassNames = classmethod(getClassNames)

	def _getClassFromModules(cls, ec, log, clsName, clsModuleList, clsBadParents):
		clsFormat = lambda cls: '%s:%s' % (cls.__module__, cls.__name__)
		clsLoadedList = []
		for clsModule in clsModuleList:
			log.log(logging.DEBUG2, 'Searching for class %s:%s', clsModule.__name__, clsName)
			try:
				clsLoadedList.append(getattr(clsModule, clsName))
			except Exception:
				log.log(logging.DEBUG2, 'Unable to import class %s:%s', clsModule.__name__, clsName)
				ec.collect()

		for clsLoaded in clsLoadedList:
			try:
				if issubclass(clsLoaded, cls):
					log.log(logging.DEBUG1, 'Successfully loaded class %s', clsFormat(clsLoaded))
					yield clsLoaded
				clsBadParents.append(clsLoaded.__name__)
				log.log(logging.DEBUG, '%s is not of type %s!', clsFormat(clsLoaded), clsFormat(cls))
			except Exception:
				ec.collect()
	_getClassFromModules = classmethod(_getClassFromModules)

	def _getModule(cls, ec, log, clsName):
		clsNameParts = clsName.split('.')
		clsName = clsNameParts[-1]
		clsModuleName = str.join('.', clsNameParts[:-1])
		log.log(logging.DEBUG2, 'Importing module %s', clsModuleName)
		oldSysPath = list(sys.path)
		try:
			result = [__import__(clsModuleName, {}, {}, [clsName])]
		except Exception:
			result = []
			log.log(logging.DEBUG2, 'Unable to import module %s', clsModuleName)
			ec.collect()
		sys.path = oldSysPath
		return result
	_getModule = classmethod(_getModule)

	def _getClass(cls, log, clsName):
		# resolve class name/alias to complete class path 'myplugin -> module.submodule.MyPlugin'
		clsSearchList = [(0, clsName)]
		clsFormat = lambda cls: '%s:%s' % (cls.__module__, cls.__name__)
		clsProcessed = []
		clsBadParents = []
		clsFound = False
		ec = ExceptionCollector()
		while clsSearchList:
			_, clsSearchName = clsSearchList.pop()
			if clsSearchName in clsProcessed: # Prevent lookup circles
				continue
			clsProcessed.append(clsSearchName)
			clsModuleList = []
			if '.' in clsSearchName: # module.submodule.class specification
				clsModuleList.extend(cls._getModule(ec, log, clsSearchName))
				clsSearchName = clsSearchName.split('.')[-1]
			elif hasattr(sys.modules['__main__'], clsSearchName):
				clsModuleList.append(sys.modules['__main__'])

			for result in cls._getClassFromModules(ec, log, clsSearchName, clsModuleList, clsBadParents):
				clsFound = True
				yield result
			clsSearchList.extend(cls._pluginMap.get(clsSearchName.lower(), []))
			clsSearchList.sort() # sort by class inheritance depth

		if not clsFound:
			msg = 'Unable to load %r of type %r\n' % (clsName, clsFormat(cls))
			if clsProcessed:
				msg += '\tsearched plugin names:\n\t\t%s\n' % str.join('\n\t\t', clsProcessed)
			if clsBadParents:
				msg += '\tfound incompatible plugins:\n\t\t%s\n' % str.join('\n\t\t', clsBadParents)
			ec.raise_any(PluginError(msg))
			raise PluginError(msg)
	_getClass = classmethod(_getClass)

	def getAllClasses(cls, clsName):
		log = logging.getLogger('classloader.%s' % cls.__name__.lower())
		log.log(logging.DEBUG1, 'Loading all classes %s', clsName)
		return list(cls._getClass(log, clsName))
	getAllClasses = classmethod(getAllClasses)

	def getClass(cls, clsName):
		log = logging.getLogger('classloader.%s' % cls.__name__.lower())
		log.log(logging.DEBUG1, 'Loading class %s', clsName)
		if clsName not in cls._clsCache.get(cls, {}):
			for result in cls._getClass(log, clsName):
				cls._clsCache.setdefault(cls, {})[clsName] = result
				break # return only first class
		return cls._clsCache[cls][clsName]
	getClass = classmethod(getClass)

	def getClassList(cls):
		return Plugin._classMap.get(cls.__name__.lower(), [])
	getClassList = classmethod(getClassList)

	# Get an instance of a derived class by specifying the class name and constructor arguments
	def createInstance(cls, clsName, *args, **kwargs):
		clsType = None
		try:
			clsType = cls.getClass(clsName)
			return clsType(*args, **kwargs)
		except Exception:
			raise PluginError('Error while creating instance of type %s (%s)' % (clsName, clsType))
	createInstance = classmethod(createInstance)

	def bind(cls, value, **kwargs):
		for entry in value.split():
			yield InstanceFactory(entry, cls.getClass(entry))
	bind = classmethod(bind)

Plugin.pkgPaths = []

def safe_import(root, module):
	old_path = list(sys.path)
	try:
		result = __import__(str.join('.', module), {}, {}, module[-1])
	except Exception:
		sys.stderr.write('import error: %s %s\n%r' % (root, module, sys.path))
		raise
	sys.path = old_path
	return result

def import_modules(root, selector, package = None):
	sys.path = [os.path.abspath(root)] + sys.path

	if os.path.exists(os.path.join(root, '__init__.py')):
		package = (package or []) + [os.path.basename(root)]
		yield safe_import(root, package)
	else:
		package = []

	files = os.listdir(root)
	__import__('random').shuffle(files)
	for fn in files:
		if fn.endswith('.pyc'):
			os.remove(os.path.join(root, fn))
	for fn in files:
		if not selector(os.path.join(root, fn)):
			continue
		if os.path.isdir(os.path.join(root, fn)):
			for module in import_modules(os.path.join(root, fn), selector, package):
				yield module
		elif os.path.isfile(os.path.join(root, fn)) and fn.endswith('.py'):
			yield safe_import(root, package + [fn[:-3]])

	sys.path = sys.path[1:]

def get_plugin_classes(module_iterator):
	for module in module_iterator:
		try:
			cls_list = module.__all__
		except Exception:
			cls_list = dir(module)
		for cls_name in cls_list:
			cls = getattr(module, cls_name)
			try:
				if issubclass(cls, Plugin):
					yield cls
			except TypeError:
				pass

def create_plugin_file(package, selector):
	cls_dict = {}
	def fill_cls_dict(cls): # return list of dicts that were filled with cls information
		if cls == object:
			return [cls_dict]
		else:
			result = []
			for cls_base in cls.__bases__:
				for cls_base_dict in fill_cls_dict(cls_base):
					tmp = cls_base_dict.setdefault(cls, {})
					tmp.setdefault(None, cls)
					result.append(tmp)
			return result

	for cls in get_plugin_classes(import_modules(os.path.abspath(package), selector)):
		if cls.__module__.startswith(os.path.basename(package)):
			fill_cls_dict(cls)

	if not cls_dict:
		return

	def write_cls_hierarchy(fp, data, level = 0):
		if None in data:
			cls = data.pop(None)
			fp.write('%s * %s %s\n' % (' ' * level, cls.__module__, str.join(' ', cls.getClassNames())))
			fp.write('\n')
		key_order = []
		for cls in data:
			key_order.append(tuple((cls.__module__ + '.' + cls.__name__).split('.')[::-1] + [cls]))
		key_order.sort()
		for key_info in key_order:
			write_cls_hierarchy(fp, data[key_info[-1]], level + 1)
	fp = open(os.path.abspath(os.path.join(package, '.PLUGINS')), 'w')
	try:
		write_cls_hierarchy(fp, cls_dict)
	finally:
		fp.close()

# Init plugin search paths
def init_hpf_plugins(basePath):
	for pkgName in os.listdir(basePath):
		pluginFile = os.path.join(basePath, pkgName, '.PLUGINS')
		if os.path.exists(pluginFile):
			__import__(pkgName) # Trigger initialisation of module
			base_cls_info = {}
			for line in open(pluginFile):
				if not line.strip():
					continue
				tmp = line.split(' * ')
				module_info = tmp[1].split()
				(module_name, cls_name) = (module_info[0], module_info[1])
				base_cls_level = len(tmp[0])
				base_cls_info[base_cls_level] = cls_name
				for level in list(base_cls_info):
					if level > base_cls_level:
						base_cls_info.pop(level)
				Plugin.registerClass(module_name, cls_name, module_info[2:], base_cls_info.values())
