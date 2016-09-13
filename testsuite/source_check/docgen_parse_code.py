import ast, sys, getFiles, linecache, json

tmp = [] # used for debugging strange nodes

class ConfigVisitor(ast.NodeVisitor):
	def __init__(self):
		ast.NodeVisitor.__init__(self)
		self._caller_stack = []
		self._stack = []
		self.calls = []

	def generic_visit(self, node):
		self._stack.append(node)
		ast.NodeVisitor.generic_visit(self, node)
		self._stack.pop()

	def visit_FunctionDef(self, node):
		self._caller_stack.append(node.name)
		self.generic_visit(node)
		self._caller_stack.pop()

	def visit_ClassDef(self, node):
		self._caller_stack.append(node.name)
		self.generic_visit(node)
		self._caller_stack.pop()

	def visit_Call(self, node):
		self.calls.append((list(self._caller_stack), node, list(self._stack)))
		self.generic_visit(node)


def get_func_name(node):
	if isinstance(node, ast.Name):
		return node.id
	elif isinstance(node, ast.Attribute):
		return get_func_name(node.value) + '.' + node.attr
	elif isinstance(node, ast.Call):
		return get_func_name(node.func) + '(...)'
	elif isinstance(node, ast.Subscript):
		return get_func_name(node.value) + '[...]'
	elif isinstance(node, (ast.BinOp, ast.BoolOp)):
		return '<operation>'
	elif isinstance(node, ast.Str):
		return '<some string>'
	elif isinstance(node, ast.Lambda):
		return '<lambda>'
	return '???'


def analyse_file(fn):
	tree = ast.parse(open(fn).read())
	cv = ConfigVisitor()
	cv.visit(tree)
	return (tree, cv.calls)


def transform_call(fn, caller_stack, node):
	result = {'fn': fn, 'lineno': node.lineno, 'line': linecache.getline(fn, node.lineno),
		'fqfn': get_func_name(node.func), 'node': node, 'callers': caller_stack}
	assert('???' not in result['fqfn'])
	return result


enums = []
enums_use_hash = {}
config_calls = []
for (fn, fnrel) in getFiles.getFiles(showExternal = False, showAux = False, showScript = False, showTestsuite = False, showTypes = ['py']):
	if 'scriptlets' in fn:
		continue
	(tree, cc) = analyse_file(fn)
	for (caller_stack, node, parents) in cc:
		result = transform_call(fn, caller_stack, node)

		if 'make_enum' in result['fqfn']:
			if len(node.args) == 1:
				enumName = parents[-1].targets[0].id
			elif len(node.args) == 2:
				enumName = node.args[1].id
			enums_use_hash[enumName] = ('use_hash', 'False') not in map(lambda kw: (kw.arg, kw.value.id), result['node'].keywords)
			try:
				elements = map(lambda x: x.s, node.args[0].elts)
			except Exception:
				elements = '<manual>'
			enums.append((enumName, elements))
			continue

		elif '_query_config' in result['fqfn']:
			result['fqfn'] = get_func_name(node.args[0])
			node.args = node.args[1:]
			config_calls.append(result)

		elif 'config.is_interactive' in result['fqfn']:
			config_calls.append(result)

		elif 'config.get' in result['fqfn']:
			assert(result['node'].func.attr.startswith('get')) # prevent sequential calls with get
			use = True
			for key in ['get_config_name', 'get_work_path', 'get_state', 'get_option_list']: # internal API
				if key in result['fqfn']:
					use = False
			if use:
				config_calls.append(result)
#				print result


for result in config_calls:
	def parse_option_spec(value):
		if isinstance(value, ast.Str):
			return repr(value.s)
		elif isinstance(value, ast.Num):
			return value.n
		elif isinstance(value, ast.Name):
			if value.id == 'True':
				return True
			elif value.id == 'False':
				return False
			elif value.id == 'None':
				return None
			return '<name:%s>' % value.id
		elif isinstance(value, ast.Attribute):
			return '<attr:%s>' % value.attr.strip('_')
		elif isinstance(value, ast.List):
			return map(parse_option_spec, value.elts)
		elif isinstance(value, ast.Dict):
			return '{%s}' % str.join(', ', map(lambda k_v: '%s: %s' % k_v, zip(map(parse_option_spec, value.keys), map(parse_option_spec, value.values))))
		elif isinstance(value, ast.Call):
			if isinstance(value.func, ast.Name):
				if value.func.id == 'get_handler_option':
					base_opt = parse_option_spec(value.args[0]).strip()
					return ['<name:logger_name> %s' % base_opt, '<name:logger_name> <name:handler_name> %s' % base_opt]
				elif value.func.id == 'add_config_suffix':
					return ['<prefix> %s' % parse_option_spec(value.args[1]).strip()]
			elif isinstance(value.func, ast.Attribute):
				if value.func.attr == '_get_pproc_opt':
					return [
						'partition %s' % parse_option_spec(value.args[0]).strip("'"),
						'<name:datasource_name> partition %s' % parse_option_spec(value.args[0]).strip("'")
					]
			return '<call:%s(%s)>' % (get_func_name(value.func), str.join(', ', map(str, map(parse_option_spec, value.args))))

		elif isinstance(value, ast.BinOp):
			if isinstance(value.op, ast.Add):
				return '%s %s' % (parse_option_spec(value.left).strip().rstrip("'"), parse_option_spec(value.right).strip().lstrip("'"))
			elif isinstance(value.op, ast.Mod):
				try:
					return parse_option_spec(value.left) % parse_option_spec(value.right)
				except Exception:
					return parse_option_spec(value.left) + '%' + parse_option_spec(value.right)
			elif isinstance(value.op, ast.Mult):
				return eval('%s * %s' % (parse_option_spec(value.left), parse_option_spec(value.right)))

		return '<manual>'

	result['args'] = map(parse_option_spec, result['node'].args)
	result['kwargs'] = {}
	for keyword in result['node'].keywords:
		if keyword.arg in ['pargs', 'parse_item']:
			continue
		result['kwargs'][keyword.arg] = parse_option_spec(keyword.value)
	result['api'] = result['fqfn'].split('.')[-1]
	result['scope'] = result['fqfn'].split('.')[-2]
	result['on_change'] = result['kwargs'].pop('on_change', '<impossible>')
	result['on_valid'] = result['kwargs'].pop('on_valid', '<no validation>')
	result['persistent'] = result['kwargs'].pop('persistent', False)
	result.pop('node')

fp = open('docgen_config_calls.json', 'w')
json.dump(config_calls, fp, indent = 2, sort_keys = True)
fp.close()

fp = open('docgen_enums.json', 'w')
assert(len(enums) == len(dict(enums)))
json.dump({'enums': dict(enums), 'use_hash': enums_use_hash}, fp, indent = 2, sort_keys = True)
fp.close()
