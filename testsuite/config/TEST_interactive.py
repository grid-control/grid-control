#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import create_config, function_factory, run_test
from python_compat import set, sorted


def set_user_input(config, *values):
	config.prompt = function_factory(*values)

def test_interactivity(user_inputs, default, config_dict = None, **kwargs):
	config = create_config(config_dict = config_dict or {}).change_view(setSections = ['test'])
	set_user_input(config, *user_inputs)
	kwargs.setdefault('interactive_msg', 'prompt')
	result = config.get_bool('key', default, **kwargs)
	return result

def test_isetup(kuser = None, idefault = None, ikey = None, iconfig = True,
		userinput = False, kdefault = True):
	config_dict = {}
	if kuser is not None:
		config_dict.setdefault('test', {})['key'] = kuser
	if idefault is not None:
		config_dict.setdefault('interactive', {})['default'] = idefault
	if ikey is not None:
		config_dict.setdefault('interactive', {})['key interactive'] = ikey
	return test_interactivity([str(userinput)], kdefault, config_dict, interactive = iconfig)

def test_isetup_permutations(**kwargs):
	def get_perms(name, options):
		if name in kwargs:
			return [kwargs[name]]
		return options
	results = {}
	for kuser in get_perms('kuser', [None, True, False]):
		for idefault in get_perms('idefault', [None, True, False]):
			for ikey in get_perms('ikey', [None, True, False]):
				for iconfig in get_perms('iconfig', [True, False]):
					output = []
					for (entry, value) in [('kuser', kuser), ('idefault', idefault), ('ikey', ikey), ('iconfig', iconfig)]:
						if entry not in kwargs:
							output.append('%s:%s' % (entry[:2], value))
					tmp = test_isetup(kuser, idefault, ikey, iconfig, userinput = False, kdefault = True)
					results[str.join(';', output)] = tmp
	if len(set(results.values())) == 1:
		print('all = %r [%d]' % (set(results.values()).pop(), len(results)))
	else:
		for entry in sorted(results):
			print('%s = %s' % (entry, results[entry]))

class Test_Config_Setup:
	"""
	>>> test_isetup_permutations(kuser = False)
	all = False [18]
	>>> test_isetup_permutations(kuser = True)
	all = True [18]

	>>> test_isetup_permutations(kuser = None, ikey = False)
	all = True [6]
	>>> test_isetup_permutations(kuser = None, ikey = True)
	(('prompt [True]',), {})
	(('prompt [True]',), {})
	(('prompt [True]',), {})
	(('prompt [True]',), {})
	(('prompt [True]',), {})
	(('prompt [True]',), {})
	all = False [6]

	>>> test_isetup_permutations(kuser = None, ikey = None, iconfig = False)
	all = True [3]

	>>> test_isetup_permutations(kuser = None, ikey = None, iconfig = True)
	(('prompt [True]',), {})
	(('prompt [True]',), {})
	id:False = True
	id:None = False
	id:True = False
	"""

class Test_ConfigInteractive:
	"""
	>>> config = create_config()
	>>> config.is_interactive('option', default = False)
	False
	>>> config.is_interactive('option', default = False)
	False

	>>> config = create_config()
	>>> config.is_interactive('option', default = True)
	True
	>>> config.is_interactive('option', default = True)
	True
	"""

class Test_Config:
	"""
	>>> test_interactivity(['false'], True)
	(('prompt [True]',), {})
	False

	>>> test_interactivity(['hallo', 'welt', 'true'], True)
	(('prompt [True]',), {})
	Unable to parse bool: hallo
	(('prompt [True]',), {})
	Unable to parse bool: welt
	(('prompt [True]',), {})
	True

	>>> test_interactivity(['hallo', ''], True)
	(('prompt [True]',), {})
	Unable to parse bool: hallo
	(('prompt [True]',), {})
	True

	>>> test_interactivity(['hallo', ''], False)
	(('prompt [False]',), {})
	Unable to parse bool: hallo
	(('prompt [False]',), {})
	False

	>>> test_interactivity(['hallo', 'false'], False)
	(('prompt [False]',), {})
	Unable to parse bool: hallo
	(('prompt [False]',), {})
	False

	>>> test_interactivity(['hallo', 'welt', 'true'], False)
	(('prompt [False]',), {})
	Unable to parse bool: hallo
	(('prompt [False]',), {})
	Unable to parse bool: welt
	(('prompt [False]',), {})
	True
	"""

run_test()
