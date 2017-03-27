import os, sys, logging, get_file_list
from python_compat import any, imap, lmap, set, sorted


def display_unused_exports(used_imports, available_imports):
	for (_, imports) in used_imports.items():
		for module in available_imports:
			# if module.__name__.endswith(import_src):
			for item in imports:
				try:
					available_imports[module].remove(item)
				except Exception:
					pass
	print '-' * 29
	for module, module_x in available_imports.items():
		if module_x:
			print "superflous exports", module.__name__, module_x


def main():
	stored_sys_path = list(sys.path)
	available_imports = {}
	used_imports = {}
	for (fn, fnrel) in get_file_list.get_file_list(show_type_list=['py'],
			show_external=True, show_aux=False, show_testsuite=False):
		logging.debug(fnrel)
		blacklist = ['/requests/', 'python_compat_', 'commands.py']
		if fn.endswith('go.py') or any(imap(lambda pat: pat in fn, blacklist)):
			continue
		for line in open(fn):
			if ('import' in line) and ('from' in line):
				import_lines = lmap(str.strip, line.split('import')[1].split(','))
				import_src = line.split('from')[1].split('import')[0].strip()
				used_imports.setdefault(import_src, set()).update(import_lines)

		module = None
		if ('/scripts/' in fn) and not fn.endswith('gc_scripts.py'):
			continue
		elif fn.endswith('__init__.py'):
			sys.path.append(os.path.dirname(os.path.dirname(fn)))
			module = __import__(os.path.basename(os.path.dirname(fn)))
		else:
			sys.path.append(os.path.dirname(fn))
			module = __import__(os.path.basename(fn).split('.')[0])
		if hasattr(module, '__all__'):
			mod_all = list(module.__all__)
			mod_sort = sorted(mod_all, key=str.lower)
			available_imports[module] = mod_sort
			if mod_all != mod_sort:
				print fn, module
				print "Unsorted", fn
				print "  -", mod_all
				print "  +", mod_sort
				print
		sys.path = list(stored_sys_path)
	display_unused_exports(used_imports, available_imports)


if __name__ == '__main__':
	main()
