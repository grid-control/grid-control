#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testfwk').setup(__file__)
# - prolog marker
import os, logging
from testfwk import run_test, try_catch
from hpfwk import InstanceFactory, Plugin
from hpfwk.hpf_plugin import create_plugin_file


class X(Plugin):
	pass

class Y(Plugin):
	pass

class Test_Plugins:
	"""
	>>> logging.getLogger().setLevel(logging.INFO1)
	>>> InstanceFactory('dummy', logging.Logger, 'name', level=logging.INFO1)
	<instance factory for logging.Logger('name', level=13, ...)>
	>>> fac = InstanceFactory('dummy', logging.Logger, 'name', unknown=logging.INFO1)
	>>> fac
	<instance factory for logging.Logger('name', unknown=13, ...)>
	>>> try_catch(fac.create_instance_bound, 'PluginError', 'Error while creating instance')
	caught

	>>> p = Plugin.get_class('Plugin')
	>>> p
	<class 'hpfwk.hpf_plugin.Plugin'>
	>>> try_catch(lambda: Plugin.get_class('PluginX'), 'PluginError', 'Unable to load')
	caught

	>>> create_plugin_file(os.path.join(os.environ['GC_PACKAGES_PATH'], 'hpfwk'), lambda fn: fn.endswith('py'))
	{<class 'hpfwk.hpf_plugin.Plugin'>: {}}

	>>> (X.get_class('X'), Y.get_class('Y'))
	(<class '__main__.X'>, <class '__main__.Y'>)
	>>> try_catch(lambda: X.get_class('Y'), 'PluginError', 'found incompatible plugins')
	caught

	>>> try_catch(lambda: Plugin.get_class('hpfwk.Unknown'), 'PluginError', 'Unable to load')
	caught
	>>> try_catch(lambda: Plugin.get_class('unknown.Unknown'), 'PluginError', 'Unable to load')
	caught
	"""

run_test()
