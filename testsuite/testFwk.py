import os, re, sys


def cleanup_workflow():
	remove_files_testsuite(['work/gc-run.lib', 'work/jobs', 'work/output',
		'work/params.dat.gz', 'work/params.map.gz', 'work/sandbox', 'work'])


def cmp_obj(x, y, desc=''):
	from python_compat import sorted, izip, set
	if type(x) != type(y):
		return desc + 'different types %s %s' % (type(x), type(y))
	if isinstance(x, (list, tuple, dict, set)):
		if len(x) != len(y):
			return desc + 'different number of elements len(%r)=%d len(%r)=%d' % (x, len(x), y, len(y))
		if isinstance(x, (dict, set)):
			item_iter = enumerate(izip(sorted(x, key=str), sorted(y, key=str)))
		else:
			item_iter = enumerate(izip(x, y))
		for idx, (xi, yi) in item_iter:
			result = cmp_obj(xi, yi, desc + 'items (#%d):' % idx)
			if result is not None:
				return result
		if isinstance(x, dict):
			for xi in sorted(x):
				result = cmp_obj(x[xi], y[xi], desc + 'values (key:%s):' % repr(xi))
				if result is not None:
					return result
	elif isinstance(x, (str, int, float)):
		if x != y:
			return desc + 'different objects %r %r' % (x, y)
	elif isinstance(x, type(None)):
		assert x == y
	else:
		return 'unknown type %s' % type(x)


def create_config(*args, **kwargs):
	from grid_control.config import create_config
	return create_config(*args, **kwargs)


def create_workflow(userConfigDict=None, abort=None, states=None):
	from grid_control.workflow import Workflow
	config_dict = {
		'global': {'task': 'UserTask', 'backend': 'Host'},
		'jobs': {'nseeds': 0, 'random variables': ''},
		'task': {
			'wall time': '1',
			'executable': os.path.join(os.environ['GC_TESTSUITE_BASE'], 'testFwk.py'),
			'task id': 'GC000000000000',
		},
	}
	userConfigDict = userConfigDict or {}
	for key in userConfigDict:
		config_dict.setdefault(key, {}).update(userConfigDict[key])
	config = create_config(config_dict=config_dict)
	states = states or []
	for (state, detail) in states:
		config.set_state(True, state, detail)
	workflow = Workflow(config, 'global', abort)
	workflow.testsuite_config = config
	return workflow


def display_jobs(jobdb):
	from grid_control.job_db import Job
	for jobnum in jobdb.getJobs():
		job_obj = jobdb.getJob(jobnum)
		if not job_obj:
			infos = [str(jobnum), '<no stored data>']
		else:
			infos = [str(jobnum), Job.enum2str(job_obj.state).ljust(8), job_obj.gcID or '<no id>']
			if job_obj.dict.get('legacy_gcID'):
				infos.append('(%s)' % job_obj.dict.get('legacy_gcID'))
		print(str.join(' ', infos))


def format_exception_testsuite(ex_tuple):
	from hpfwk.hpf_debug import format_exception
	return format_exception(ex_tuple, show_file_stack=1, show_exception_stack=2)


def function_factory(*values, **kwargs):
	display = kwargs.pop('display', True)

	def fun(*args, **kwargs):
		if display:
			print((args, kwargs))
		result = fun.values[0]
		if len(fun.values) > 1:
			fun.values = fun.values[1:]
		return result
	fun.values = values
	return fun


def get_logger():
	import logging
	return logging.getLogger().handlers[0]


def remove_files_testsuite(files):
	import glob
	from grid_control.utils import remove_files
	files_list = []
	for fn in files:
		files_list.extend(glob.glob(fn))
	return remove_files(files_list)


def run_test(exit_fun=None, cleanup_fun=None):
	import doctest
	try:
		kwargs = {'optionflags': doctest.REPORT_UDIFF}
	except Exception:
		kwargs = {}
	result = doctest.testmod(**kwargs)
	if exit_fun is not None:
		exit_fun()
	if (cleanup_fun is not None) and (result[0] == 0):
		cleanup_fun()
	sys.exit(result[0])


def setPath(x):
	try:
		path = setPath.backup
	except:
		setPath.backup = os.environ['PATH']
		path = os.environ['PATH']
	os.environ['PATH'] = str.join(':', [os.path.abspath(x)] + path.split(':')[1:])


def setup(fn):
	sys_stderr = sys.stderr

	def add_path(dn):
		sys.path.append(os.path.abspath(dn))
	sys.path.pop()
	dn_fn = os.path.dirname(fn)
	if not dn_fn:
		dn_fn = os.curdir
	dir_testsuite = os.path.abspath(os.path.dirname(__file__))
	os.environ['GC_TESTSUITE_BASE'] = dir_testsuite
	add_path(dir_testsuite)  # testsuite base dir
	add_path(dn_fn)  # test dir
	add_path(os.path.join(os.path.dirname(__file__), '..', 'packages'))  # gc dir (pip installed)
	import hpfwk  # to properly setup HPF_STARTUP_DIRECTORY
	os.chdir(dn_fn)
	from grid_control.utils.table import ConsoleTable
	ConsoleTable._write_line = lambda self, x: sys.stdout.write(x + '\n')
	from grid_control.logging_setup import StdoutStreamHandler
	StdoutStreamHandler.testsuite_stream = TestStream()
	StdoutStreamHandler.get_stream = lambda self: self.testsuite_stream

	def testsuite_print_exception(etype, value, tb, limit=None, file=None):
		file = file or sys_stderr
		file.write(format_exception_testsuite((etype, value, tb)) + '\n')
		file.flush()
	import traceback
	traceback.print_exception = testsuite_print_exception


def str_dict_testsuite(d, keys=None):
	if keys is None:
		keys = list(d.keys())
		keys.sort()
	dict_list = []
	for key in keys:
		if key in d:
			dict_list.append('%s: %s' % (repr(key), repr(d[key])))
	return '{%s}' % str.join(', ', dict_list)


def try_catch(fun, catch='!!!', catch_value=None):
	try:
		fun()
	except SystemExit:
		if catch == 'SystemExit':
			print('Exit with %r' % sys.exc_info()[1].args)
		else:
			print('failed SystemExit')
	except Exception:
		messages = format_exception_testsuite(sys.exc_info())
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
			print('caught')
		else:
			print(messages)


def write_file(fn, content):
	fp = open(fn, 'w')
	fp.write(content)
	fp.close()


class DummyObj(object):
	def __init__(self, **struct):
		for x in struct:
			setattr(self, x, struct[x])

	def __repr__(self):
		return '%s(%s)' % (self.__class__.__name__, str_dict_testsuite(self.__dict__))


class TestStream(object):
	def __init__(self):
		self._modify = lambda x: x
		self._last_line_empty = False

	def flush(self):
		pass

	def set_modify(self, fun):
		self._modify = fun

	def write(self, value):
		for line in self._transform(value):
			if not line:
				print('-' * 5)
			else:
				print(line.rstrip())

	def _transform(self, value):
		value = value.replace('IOError', 'XXError')
		value = value.replace('OSError', 'XXError')
		value = value.replace('python_compat', 'py_th_on_com_pat')
		value = value.replace(os.environ['GC_TESTSUITE_BASE'], '<testsuite dir>')
		value = value.replace(os.environ['GC_PACKAGES_PATH'], '<gc package dir>')
		value = re.sub(r'\'.*/debug.log\'', '\'.../debug.log\'', value)
		value = re.sub(r'\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d', '0000-00-00 00:00:00', value)
		value = re.sub(r'\d\d\d\d-\d\d-\d\d', '0000-00-00', value)
		value = re.sub(r'\(\d+:\d\d:\d\d\)', '(XX:XX:XX)', value)
		value = value.replace('\t', '  ')
		for line in self._modify(value.splitlines()):
			yield line.rstrip()
