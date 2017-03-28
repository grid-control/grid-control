import os, sys
from python_compat import any, lfilter, lmap, next, sorted


def collect_and_sort_onelevel(source_iter, do_print=False):
	sort_helper = []

	def _do_sort(sort_helper):
		if sort_helper:
			cls_tree = {
				'Exception'.lower(): ['0'],
				'NestedException'.lower(): ['0'],
				'object'.lower(): ['1'],
			}
			for (defclass, _) in sort_helper:
				try:
					if defclass.lstrip().startswith('class'):
						cls_parts = defclass.lstrip().split(' ')[1].rstrip().rstrip(':').rstrip(')').split('(')
						cls_name = cls_parts[0]
						cls_tree[cls_name.lower()] = cls_tree.get(cls_parts[1].lower(), [cls_parts[1]]) + [cls_name]
				except Exception:
					print "Error while processing", cls_tree, defclass
					raise

			for entry in sorted(sort_helper, key=lambda k: keyfun(cls_tree, k)):
				if do_print:
					key = keyfun(cls_tree, entry)
					if (key[1] == 1) and not key[0]:
						print key[-1]
				yield entry

	while True:
		value = next(source_iter, None)
		if value is None:
			break
		if isinstance(value, tuple):
			(defclass, src) = value
			sort_helper.append((defclass, list(collect_and_sort_onelevel(merge(src)))))
		else:
			for entry in _do_sort(sort_helper):
				yield entry
			sort_helper = []
			yield value
	for entry in _do_sort(sort_helper):
		yield entry


def keyfun(cls_tree, value):
	tmp = value[0].lstrip().split(' ')
	name = tmp[1].split('(')[0]
	name = name.lower()
	cls_bases = []
	cls_base_outside = 0
	if tmp[0] == 'class':
		cls_bases = cls_tree.get(name)
	cls_bases = lmap(lambda base_name: base_name.strip(','), cls_bases)

	is_private = (name[0] == '_') and (name[1] != '_')
	name_prio = {'__new__': -2, '__init__': -1}

	if '0' in cls_bases:  # error classes on top!
		return (is_private, 0, tuple(cls_bases), name)
	elif tmp[0] != 'class':  # functions
		if not is_private:
			print '\tfun:', name
		return (is_private, 1, name_prio.get(name, 0), name)
	else:  # classes
		return (is_private, 2, (cls_base_outside, len(cls_bases), tuple(cls_bases)), name)


def merge(lines):
	my_iter = iter(lines)
	while True:
		value = next(my_iter, None)
		if value is None:
			break
		if not isinstance(value, str):
			yield value
		elif value.lstrip().startswith('def ') or value.lstrip().startswith('class '):
			next_value = next(my_iter, None)
			assert next_value is not None
			yield (value, next_value)
		else:
			yield value


def sort_file(fn):
	fp = open(fn)
	fn_lines_all = fp.readlines()
	fp.close()
	if not lfilter(lambda x: x, fn_lines_all):
		return
	fn_lines = iter(fn_lines_all)

	fn_depth_map = {-1: []}
	for line in fn_lines:
		old_depth = max(fn_depth_map)

		if not line.strip():  # empty lines - belong to current depth
			fn_depth_map[old_depth].append(line)
			continue
		keyword_list = ['classmethod', 'staticmethod', 'make_enum', '# <global-state>', '# <alias>']

		def _match_keywords(keyword):
			return (keyword in line) and (not 'return %s' % keyword in line)
		if any(lmap(_match_keywords, keyword_list)):
			fn_depth_map[old_depth].append(line)
			continue
		cur_depth = line.replace(line.lstrip(), '').count('\t')  # indent depth

		if cur_depth >= old_depth:  # handle same depth
			fn_depth_map.setdefault(cur_depth, []).append(line)
			continue

		while True:
			depth = max(fn_depth_map)
			if depth <= cur_depth:
				break
			fn_depth_map.setdefault(depth - 1, []).append(fn_depth_map.pop(depth))
		fn_depth_map.setdefault(cur_depth, []).append(line)

	fn_depth_list = sorted(fn_depth_map)
	fn_depth_list.reverse()
	for depth in fn_depth_list:
		if depth > 0:
			fn_depth_map[depth - 1].append(fn_depth_map.pop(depth))

	fp = open(fn + '.unsorted', 'w')
	fp.write(open(fn).read())
	fp.close()
	unfold(open(fn, 'w'), collect_and_sort_onelevel(merge(fn_depth_map[0]), True))
	os.unlink(fn + '.unsorted')


def unfold(stream, value_list):
	for value in value_list:
		if isinstance(value, list):
			unfold(stream, value)
		elif isinstance(value, tuple):
			stream.write(value[0].rstrip() + '\n')
			unfold(stream, value[1])
		else:
			stream.write(value.rstrip() + '\n')


for arg in sys.argv[1:]:
	sort_file(arg)
