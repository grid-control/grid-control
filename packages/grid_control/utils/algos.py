# | Copyright 2007-2017 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import operator
from python_compat import ifilter, ismap, izip_longest, next, sort_inplace, sorted, unspecified


def accumulate(iterable, empty, do_emit, do_add=lambda item, buffer: True, add_fun=operator.add):
	buf = empty
	for item in iterable:
		if do_add(item, buf):
			buf = add_fun(buf, item)
		if do_emit(item, buf):
			if buf != empty:
				yield buf
			buf = empty
	if buf != empty:
		yield buf


def dict_union(*args):
	tmp = dict()
	for mapping in args:
		tmp.update(mapping)
	return tmp


def filter_dict(mapping, key_filter=lambda k: True, value_filter=lambda v: True):
	def _filter_items(k_v):
		return key_filter(k_v[0]) and value_filter(k_v[1])
	return dict(ifilter(_filter_items, mapping.items()))


def get_list_difference(list_old, list_new, key_fun, on_matching_fun,
		is_sorted=False, key_fun_sort=None):
	(list_added, list_missing, list_matching) = ([], [], [])
	if not is_sorted:
		list_new = sorted(list_new, key=key_fun_sort or key_fun)
		list_old = sorted(list_old, key=key_fun_sort or key_fun)
	(iter_new, iter_old) = (iter(list_new), iter(list_old))
	(new, old) = (next(iter_new, None), next(iter_old, None))
	while True:
		if (new is None) or (old is None):
			break
		key_new = key_fun(new)
		key_old = key_fun(old)
		if key_new < key_old:  # new[npos] < old[opos]
			list_added.append(new)
			new = next(iter_new, None)
		elif key_new > key_old:  # new[npos] > old[opos]
			list_missing.append(old)
			old = next(iter_old, None)
		else:  # new[npos] == old[opos] according to *active* comparison
			on_matching_fun(list_added, list_missing, list_matching, old, new)
			(new, old) = (next(iter_new, None), next(iter_old, None))
	while new is not None:
		list_added.append(new)
		new = next(iter_new, None)
	while old is not None:
		list_missing.append(old)
		old = next(iter_old, None)
	return (list_added, list_missing, list_matching)


def grouper(iterable, output_len, fillvalue=None):
	args = [iter(iterable)] * output_len
	return izip_longest(fillvalue=fillvalue, *args)


def intersect_first_dict(dict1, dict2):
	for key1 in list(dict1.keys()):
		if (key1 in dict2) and (dict1[key1] != dict2[key1]):
			dict1.pop(key1)


def reverse_dict(mapping):
	def _swap(value1, value2):
		return (value2, value1)
	return dict(ismap(_swap, mapping.items()))


def safe_index(indexable, idx, default=None):
	try:
		return indexable.index(idx)
	except Exception:
		return default


def split_list(iterable, fun, sort_key=unspecified):
	# single pass on iterable!
	(result_true, result_false) = ([], [])
	for value in iterable:
		if fun(value):
			result_true.append(value)
		else:
			result_false.append(value)
	if not unspecified(sort_key):
		sort_inplace(result_true, key=sort_key)
		sort_inplace(result_false, key=sort_key)
	return (result_true, result_false)


__all__ = ['accumulate', 'dict_union', 'filter_dict', 'get_list_difference', 'grouper',
	'intersect_first_dict', 'reverse_dict', 'safe_index', 'split_list']
