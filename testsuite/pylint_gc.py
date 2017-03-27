import astroid
from astroid import MANAGER, builder


AST_NUMBER = [astroid.Const(0)]
AST_LIST = [astroid.Const([])]
AST_FUN = [builder.parse('def fun(*args, **kwargs):\n\tpass')['fun']]


def register(linter):
	pass


def transform_class(node):
	if node.name == 'RestSession':
		_add_enum(node, ['GET', 'POST', 'DELETE', 'PUT'])
#	elif node.name == 'VirtualFile':
#		node.locals['getvalue'] = node.locals['get_tar_info']
#		node.locals['close'] = node.locals['get_tar_info']
	elif node.name == 'Logger':
		_add(node, 'log_process', [builder.parse('def log_process(self, proc, level=logging.WARNING, files=None, msg=None):\n\tpass')['log_process']])
		_add(node, 'log_time', [builder.parse('def log_time(self, level, msg, *args, **kwargs):\n\tpass')['log_time']])
	elif node.name == 'DataProvider':
		_add_enum(node, ['NEntries', 'URL', 'FileList', 'Locations', 'Metadata',
			'Nickname', 'Dataset', 'BlockName', 'Provider'])
	elif node.name == 'Job':
		_add_enum(node, ['INIT', 'SUBMITTED', 'DISABLED', 'READY', 'WAITING', 'QUEUED', 'ABORTED',
			'RUNNING', 'CANCEL', 'UNKNOWN', 'CANCELLED', 'DONE', 'FAILED', 'SUCCESS', 'IGNORED'])
	elif node.name == 'DataSplitter':
		_add_enum(node, ['NEntries', 'FileList', 'Locations', 'BlockName',
			'Invalid', 'Comment', 'Nickname', 'Metadata', 'MetadataHeader',
			'Skipped', 'Dataset', 'CommonPrefix'])
	elif node.name == 'WMS':
		_add_enum(node, ['STORAGE', 'WALLTIME', 'CPUTIME', 'MEMORY', 'CPUS', 'SOFTWARE',
			'BACKEND', 'QUEUES', 'SITES'])
	elif node.name == 'FileInfoProcessor':
		_add_enum(node, ['Hash', 'NameLocal', 'NameDest', 'Path'])
	elif node.name == 'Result':
		_add_multi(node, ['pnum_list_redo', 'pnum_list_disable'], AST_LIST)
		_add(node, 'args', [builder.parse('class X:\n\targs = []')['X'].locals['args']])
		_add(node, 'opts', [builder.parse('class X:\n\topts = {}')['X'].locals['opts']])
		_add(node, 'parser', [builder.parse('class X:\n\tdef usage(*args, **kwargs):\n\t\tpass\n\tdef exit_with_usage(*args, **kwargs):\n\t\tpass')['X']])
		_add(node, 'config_dict', [builder.parse('class X:\n\topts = {}')['X'].locals['opts']])


def transform_module(node):
	if node.name == 'logging':
		_add_enum(node, ['INFO1', 'INFO2', 'INFO3', 'DEBUG1', 'DEBUG2', 'DEBUG3', 'DEFAULT'])


def _add(entry, name, value):
	entry.locals[name] = value


def _add_enum(entry, enum_list):
	template = builder.parse(
		'def _intstr2enum(cls, value, default=Unspecified):\n' +
		'\tpass\n' +
		'def _str2enum(cls, value, *args):\n' +
		'\tpass\n' +
		'def _register_enum(cls, name):\n' +
		'\tpass\n' +
		'class X:\n' +
		'\tpass\n' +
		'X.enum_name_list = []\n' +
		'X.enum_value_list = []\n' +
		'X.enum2str = {}.get\n' +
		'X.register_enum = classmethod(_register_enum)\n' +
		'X.str2enum = classmethod(_str2enum)\n' +
		'X.intstr2enum = classmethod(_intstr2enum)\n'
	)['X']
	_add_multi(entry, enum_list, AST_NUMBER)
	for name in ['enum_value_list', 'enum_name_list', 'intstr2enum', 'enum2str', 'str2enum', 'register_enum']:
		_add(entry, name, template.locals[name])


def _add_multi(entry, name_list, value):
	for name in name_list:
		_add(entry, name, value)

MANAGER.register_transform(astroid.Module, transform_module)
MANAGER.register_transform(astroid.Class, transform_class)
