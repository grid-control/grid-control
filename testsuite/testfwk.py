import os, re, sys


def testfwk_print(value):
	print(value)


def cleanup_workflow():
	testfwk_remove_files(['work/gc-run.lib', 'work/jobs', 'work/output',
		'work/params.dat.gz', 'work/params.map.gz', 'work/sandbox', 'work'])


def cmp_obj(obj_x, obj_y, desc=''):
	from python_compat import sorted, izip, set
	if type(obj_x) != type(obj_y):
		return desc + 'different types %s %s' % (type(obj_x), type(obj_y))
	if isinstance(obj_x, (list, tuple, dict, set)):
		if len(obj_x) != len(obj_y):
			return desc + 'different number of elements len(%r)=%d len(%r)=%d' % (
				obj_x, len(obj_x), obj_y, len(obj_y))
		if isinstance(obj_x, (dict, set)):
			item_iter = enumerate(izip(sorted(obj_x, key=str), sorted(obj_y, key=str)))
		else:
			item_iter = enumerate(izip(obj_x, obj_y))
		for idx, (x_key, y_key) in item_iter:
			result = cmp_obj(x_key, y_key, desc + 'items (#%d):' % idx)
			if result is not None:
				return result
		if isinstance(obj_x, dict):
			for x_key in sorted(obj_x):
				result = cmp_obj(obj_x[x_key], obj_y[x_key], desc + 'values (key:%s):' % repr(x_key))
				if result is not None:
					return result
	elif isinstance(obj_x, (str, int, float)):
		if obj_x != obj_y:
			return desc + 'different objects %r %r' % (obj_x, obj_y)
	elif isinstance(obj_x, type(None)):
		assert obj_x == obj_y
	else:
		return 'unknown type %s' % type(obj_x)


def create_config(*args, **kwargs):
	from grid_control.config import create_config
	return create_config(*args, **kwargs)


def testfwk_create_workflow(user_config_dict=None, states=None):
	from grid_control.workflow import Workflow
	config_dict = {
		'global': {'task': 'UserTask', 'backend': 'Host'},
		'jobs': {'nseeds': 0, 'random variables': ''},
		'task': {
			'wall time': '1',
			'executable': os.path.join(os.environ['GC_TESTSUITE_BASE'], 'testfwk.py'),
			'task id': 'GC000000000000',
		},
	}
	user_config_dict = user_config_dict or {}
	for key in user_config_dict:
		config_dict.setdefault(key, {}).update(user_config_dict[key])
	config = create_config(config_dict=config_dict)
	states = states or []
	for (state, detail) in states:
		config.set_state(True, state, detail)
	workflow = Workflow(config, 'global')
	workflow.testsuite_config = config
	return workflow


def testsuite_display_job_obj_list(job_db, *args, **kwargs):
	from grid_control.job_db import Job
	for jobnum in job_db.get_job_list(*args, **kwargs):
		job_obj = job_db.get_job(jobnum)
		if not job_obj:
			infos = [str(jobnum), '<no stored data>']
		else:
			infos = [str(jobnum), Job.enum2str(job_obj.state).ljust(8), job_obj.gc_id or '<no id>']
			if job_obj.get('legacy_gc_id'):
				infos.append('(%s)' % job_obj.get('legacy_gc_id'))
		testfwk_print(str.join(' ', infos))


def testfwk_format_exception(ex_tuple):
	from hpfwk.hpf_debug import format_exception
	return format_exception(ex_tuple, show_file_stack=1, show_exception_stack=2)


def function_factory(*values, **kwargs):
	display = kwargs.pop('display', True)
	display_first = kwargs.pop('display_first', True)

	def fun(*args, **kwargs):
		if display:
			if not display_first:
				args = args[1:]
			testfwk_print(repr((args, kwargs)))
		result = fun.values[0]
		if len(fun.values) > 1:
			fun.values = fun.values[1:]
		return result
	fun.values = values
	return fun


def get_logger():
	import logging
	return logging.getLogger().handlers[0]


def testfwk_remove_files(files):
	import glob
	from grid_control.utils import remove_files
	files_list = []
	for fn in files:
		files_list.extend(glob.glob(fn))
	return remove_files(files_list)


def run_test(exit_fun=None, cleanup_fun=None):
	import doctest
	kwargs = {}
	if hasattr(doctest, 'REPORT_UDIFF'):
		kwargs = {'optionflags': doctest.REPORT_UDIFF}
	result = doctest.testmod(**kwargs)
	if exit_fun is not None:
		exit_fun()
	if (cleanup_fun is not None) and (result[0] == 0):
		cleanup_fun()
	sys.exit(result[0])


def testfwk_set_path(new_path):
	try:
		path = testfwk_set_path.backup
	except Exception:
		testfwk_set_path.backup = os.environ['PATH']
		path = os.environ['PATH']
	os.environ['PATH'] = str.join(':', [os.path.abspath(new_path)] + path.split(':')[1:])


def setup(fn):
	def add_path(dn):
		sys.path.insert(1, os.path.abspath(dn))
	sys.path.pop()
	dn_fn = os.path.dirname(fn)
	if not dn_fn:
		dn_fn = os.curdir
	dir_testsuite = os.path.abspath(os.path.dirname(__file__))
	os.environ['GC_TESTSUITE_BASE'] = dir_testsuite
	add_path(os.path.join(os.path.dirname(__file__), '..', 'packages'))  # gc dir (pip installed)
	add_path(dn_fn)  # test dir
	add_path(dir_testsuite)  # testsuite base dir
	__import__('hpfwk')  # to properly setup HPF_STARTUP_DIRECTORY
	os.chdir(dn_fn)
	os.environ['GC_TERM'] = 'gc_color256'
	from grid_control.logging_setup import GCStreamHandler
	GCStreamHandler.push_std_stream(TestsuiteStream(), TestsuiteStream())

	def testfwktestfwk_print_exception(etype, value, traceback, limit=None, file=None):
		file = file or sys.stderr
		file.write(testfwk_format_exception((etype, value, traceback)) + '\n')
		file.flush()
	import traceback
	traceback.print_exception = testfwktestfwk_print_exception


def str_dict_testsuite(mapping, keys=None):
	if keys is None:
		keys = list(mapping.keys())
		keys.sort()
	dict_list = []
	for key in keys:
		if key in mapping:
			dict_list.append('%s: %s' % (repr(key), repr(mapping[key])))
	return '{%s}' % str.join(', ', dict_list)


def try_catch(fun, catch='!!!', catch_value=None):
	try:
		fun()
	except SystemExit:
		if catch == 'SystemExit':
			testfwk_print('Exit with %r' % sys.exc_info()[1].args)
		else:
			testfwk_print('failed SystemExit')
	except Exception:
		messages = testfwk_format_exception(sys.exc_info())
		caught = False
		matching = False
		for line in messages.splitlines():
			if catch and line.strip().startswith(catch):
				matching = True
				if not catch_value:
					caught = True
			if matching and catch_value and (catch_value in line):
				caught = True
		if caught:
			testfwk_print('caught')
		else:
			testfwk_print(repr(messages))


def write_file(fn, content):
	fp = open(fn, 'w')
	fp.write(content)
	fp.close()


class DummyObj(object):
	def __init__(self, **struct):
		for item in struct:
			setattr(self, item, struct[item])

	def __repr__(self):
		return '%s(%s)' % (self.__class__.__name__, str_dict_testsuite(self.__dict__))


class TestsuiteStream(object):
	_modify = None

	def __init__(self, display_commands=False):
		self._display_commands = display_commands

	def flush(self):
		pass

	def set_modify(cls, fun):
		cls._modify = staticmethod(fun)
	set_modify = classmethod(set_modify)

	def write(self, value):
		for line in self._transform(value):
			if not line:
				msg = '-' * 5
			else:
				msg = line.rstrip()
			testfwk_print(msg)

	def _transform(self, value):
		from grid_control_gui.ansi import ANSI
		if self._display_commands:
			for attr_name in ANSI.__dict__:
				if (attr_name != 'esc') and getattr(ANSI, attr_name):
					value = value.replace(str(getattr(ANSI, attr_name)), '<%s>' % attr_name)
			value = re.sub('\x1b\[([0-9]*);([0-9]*)H', '<move:\\1,\\2>', value)
			value = re.sub('\x1b\[38;5;([0-9]*)m', '<grayscale:\\1>', value)
			value = value.replace(r'\r', '<move:line-start>')
			value = value.replace('\n', '<newline>\n')
		value = value.replace('IOError', 'XXError')
		value = value.replace('OSError', 'XXError')
		value = value.replace('python_compat', 'py_th_on_com_pat')
		value = value.replace(os.environ['GC_TESTSUITE_BASE'], '<testsuite dir>')
		value = value.replace(os.environ['GC_PACKAGES_PATH'], '<gc package dir>')
		value = re.sub(r'\'.*/debug.log\'', '\'.../debug.log\'', value)
		value = re.sub(r'\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d', '0000-00-00 00:00:00', value)
		value = re.sub(r'\d\d\d\d-\d\d-\d\d', '0000-00-00', value)
		value = re.sub(r'\(\d+:\d\d:\d\d\)', '(XX:XX:XX)', value)
		value = re.sub(r'Using batch system: .*', 'Using batch system: ---', value)
		value = re.sub(r'\| \d+\)', '| ...)', value)
		value = value.replace('\t', '  ').replace('<module>', '?')
		iter_lines = value.splitlines()
		if TestsuiteStream._modify:
			iter_lines = TestsuiteStream._modify(iter_lines)
		for line in iter_lines:
			if not self._display_commands:
				yield ANSI.strip_fmt(line).rstrip()
			elif ANSI.strip_fmt(line).rstrip() == line.rstrip():
				yield line.rstrip()
			else:
				yield repr(line.rstrip())
