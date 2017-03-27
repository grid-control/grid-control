from grid_control.utils.file_objects import SafeFile
from python_compat import ifilter, imap, lfilter, lmap, set, sorted


def main():
	import logging, get_file_list
	for (fn, fnrel) in get_file_list.get_file_list(show_type_list=['py'],
			show_external=False, show_aux=False, show_source_check=True):
		logging.debug(fnrel)
		sort_import_lines(fn)
		sort_from_lines(fn)
		sort_python_compat_lines(fn)


def sort_from_lines(fn):
	# sort "from" order
	replacement_str_pair_list = []
	raw = SafeFile(fn).read_close()
	for import_line in ifilter(lambda line: line.startswith('from '), raw.splitlines()):
		try:
			_from, _source, _import, _what = import_line.split(None, 3)
			assert _from == 'from'
			assert _import == 'import'
			_comment = None
			if '#' in _what:
				_what, _comment = lmap(str.strip, _what.split('#', 1))
			import_list = sorted(imap(str.strip, _what.split(',')))
			new_import_line = 'from %s import %s' % (_source, str.join(', ', import_list))
			if _comment is not None:
				new_import_line += '  # ' + _comment
			replacement_str_pair_list.append((import_line, new_import_line))
		except:
			print fn, import_line
			raise
	for (old, new) in replacement_str_pair_list:
		raw = raw.replace(old, new)
	open(fn, 'w').write(raw)


def sort_import_lines(fn):
	# sort "import" order
	replacement_str_pair_list = []
	raw = SafeFile(fn).read_close()
	for import_line in ifilter(lambda line: line.startswith('import '), raw.splitlines()):
		import_list = sorted(imap(str.strip, import_line.replace('import ', '').split(',')),
			key=lambda x: (len(x), x))
		replacement_str_pair_list.append((import_line, 'import %s' % str.join(', ', import_list)))
	for (old, new) in replacement_str_pair_list:
		raw = raw.replace(old, new)
	open(fn, 'w').write(raw)


def sort_python_compat_lines(fn):
	output_line_list = []
	output_set = set()
	import_section = False
	for line in SafeFile(fn).iter_close():
		if line.startswith('from') or line.startswith('import'):
			import_section = True
		elif line.strip():
			if import_section:
				output_list = lfilter(lambda x: x.strip() != '', output_set)
				output_list.sort(key=lambda l: (not l.startswith('import'),
					('python_compat' in l), 'testfwk' not in l, lmap(lambda x: x.split('.'), l.split())))
				for output_line in output_list:
					output_line_list.append(output_line)
				output_line_list.append('\n\n')
				output_set = set()
			import_section = False
		if not import_section:
			output_line_list.append(line)
		else:
			output_set.add(line)
	if import_section:
		output_list = lfilter(lambda x: x.strip() != '', output_set)
		output_list.sort(key=lambda l: (not l.startswith('import'),
			'python_compat' not in l, lmap(lambda x: x.split('.'), l.split())))
		for output_line in output_list:
			output_line_list.append(output_line)

	fp = open(fn, 'w')
	for output_line in output_line_list:
		fp.write(output_line)


if __name__ == '__main__':
	main()
