def checkUsage(fn, raw):
	def getDupes(l):
		s = set(l)
		l = list(l)
		while s:
			l.remove(s.pop())
		return l

	tmp = raw.replace('def set(', '')
	list_imported = []
	for import_line in filter(lambda line: line.lstrip().startswith('import'), tmp.splitlines()):
		import_line = import_line.split('#')[0].strip()
		list_imported.extend(map(str.strip, import_line.replace('import', '').split(',')))
	if len(set(list_imported)) != len(list_imported):
		print fn, "duplicated libs!", getDupes(list_imported)
	for lib in list_imported:
		lib = lib.split('#')[0].strip()
		def chk(pat):
			return (pat % lib) not in tmp
		if (lib != 'testfwk') and chk('%s.') and chk('= %s\n') and chk('getattr(%s') and chk('(%s, '):
			print fn, "superflous", lib
	if fn.endswith('__init__.py'):
		return
	list_from = []
	list_source = []
	for import_line in filter(lambda line: line.lstrip().startswith('from '), tmp.splitlines()):
		if not 'import' in import_line:
			continue
		if '*' in import_line:
			print fn, "wildcard import!"
		else:
			if (len(import_line.split('#')[0].strip()) <= 100) and ('#' in import_line):
				print fn, 'invalid marker'
			import_line = import_line.split('#')[0].strip()
			list_from.extend(map(str.strip, import_line.split('import')[1].split(',')))
			list_source.append(import_line.split('import')[0].strip().split()[1])

	tmp = str.join('\n', filter(lambda line: not (
			line.lstrip().startswith('#') or
			line.lstrip().startswith('from ') or
			line.lstrip().startswith('import')),
		tmp.splitlines()))
	if len(set(list_source)) != len(list_source):
		print fn, "duplicated libs!", getDupes(list_source)
	for code in list_from:
		if ' as ' in code:
			code = code.split(' as ')[1]
		chk = lambda fmt: (fmt % code) in tmp
		if chk('%s(') or chk('%s.') or chk('raise %s') or chk('(%s)') or chk('=%s') or chk(' = %s') or \
				chk(' != %s') or chk('return %s') or chk(', %s)') or chk('(%s, ') or \
				chk('except %s') or chk(' %s,') or chk('\t%s,') or chk('%s, [') or chk('%s]') or \
				chk('or %s') or chk('%s not in'):
			continue
		if code in ['backends', 'datasets']:
			continue
		print fn, "superflous", code

if __name__ == '__main__':
	import logging, get_file_list
	for (fn, fnrel) in get_file_list.get_file_list(show_type_list=['py'], show_external=False, show_aux=False):
		logging.debug(fnrel)
		checkUsage(fnrel, open(fn).read())
