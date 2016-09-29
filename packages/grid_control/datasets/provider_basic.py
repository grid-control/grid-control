# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.config import ConfigError
from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.utils.parsing import parse_json, parse_list
from python_compat import lmap, rsplit


class FileProvider(DataProvider):
	# Provides information about a single file
	# required format: <path to data file>|<number of events>[@SE1,SE2]
	alias_list = ['file']

	def __init__(self, config, datasource_name, dataset_expr, dataset_nick=None, dataset_proc=None):
		DataProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick, dataset_proc)

		(self._path, self._events, selist) = utils.split_opt(dataset_expr, '|@')
		self._selist = parse_list(selist, ',') or None
		if not (self._path and self._events):
			raise ConfigError('Invalid dataset expression!\nCorrect: /local/path/to/file|events[@SE1,SE2]')

	def _iter_blocks_raw(self):
		yield {
			DataProvider.Dataset: self._path,
			DataProvider.Locations: self._selist,
			DataProvider.FileList: [{
				DataProvider.URL: self._path, DataProvider.NEntries: int(self._events)
			}]
		}


class ListProvider(DataProvider):
	# Takes dataset information from an configuration file - required format:
	#   <path to list of data files>[@<forced prefix>][%[/]<selected dataset>[#<selected block>][#]]
	alias_list = ['list']

	def __init__(self, config, datasource_name, dataset_expr, dataset_nick=None, dataset_proc=None):
		DataProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick, dataset_proc)
		self._common_prefix = max(DataProvider.enum_value_list) + 1
		self._common_metadata = max(DataProvider.enum_value_list) + 2

		self._entry_handler_info = {
			'events': (DataProvider.NEntries, int, 'block entry counter'),
			'id': (None, None, 'dataset ID'),  # legacy key - skip
			'metadata': (DataProvider.Metadata, parse_json, 'metadata description'),
			'metadata common': (self._common_metadata, parse_json, 'common metadata'),
			'nickname': (DataProvider.Nickname, str, 'dataset nickname'),
			'prefix': (self._common_prefix, str, 'common prefix'),
			'se list': (DataProvider.Locations, lambda value: parse_list(value, ','), 'block location'),
		}

		(path, self._forced_prefix, self._filter) = utils.split_opt(dataset_expr, '@%')
		self._filename = config.resolve_path(path, True, 'Error resolving dataset file: %s' % path)

	def _create_block(self, block_name):
		result = {
			DataProvider.Locations: None,
			DataProvider.FileList: [],
			self._common_prefix: None,
			self._common_metadata: [],
		}
		result.update(DataProvider.parse_block_id(block_name.lstrip('[').rstrip(']')))
		return result

	def _finish_block(self, block):
		block.pop(self._common_prefix)
		block.pop(self._common_metadata)
		return block

	def _iter_blocks_raw(self):
		def _filter_block(block):
			if self._filter:
				return self._filter in '/%s#' % DataProvider.get_block_id(block)
			return True
		try:
			fp = open(self._filename, 'r')
		except Exception:
			raise DatasetError('Unable to open dataset file %s' % repr(self._filename))
		try:
			for block in self._parse_file(fp):
				if _filter_block(block):
					self._raise_on_abort()
					yield block
			fp.close()
		except Exception:
			fp.close()
			raise

	def _parse_entry(self, block, url, value):
		if self._forced_prefix:
			url = '%s/%s' % (self._forced_prefix, url)
		elif block[self._common_prefix]:
			url = '%s/%s' % (block[self._common_prefix], url)
		value = value.split(' ', 1)
		result = {
			DataProvider.URL: url,
			DataProvider.NEntries: _try_apply(value[0], int, 'entries of file %s' % repr(url))
		}
		if len(value) > 1:
			file_metadata_list = _try_apply(value[1], parse_json, 'metadata of file %s' % repr(url))
		else:
			file_metadata_list = []
		if block[self._common_metadata] or file_metadata_list:
			result[DataProvider.Metadata] = block[self._common_metadata] + file_metadata_list
		return result

	def _parse_file(self, iterable):
		block = None
		for idx, line in enumerate(iterable):
			try:
				# Found start of block:
				line = line.strip()
				if line.startswith(';'):
					continue
				elif line.startswith('['):
					if block:
						yield self._finish_block(block)
					block = self._create_block(line)
				elif line != '':  # TODO: improve parsing for files not following the conventions
					tmp = lmap(str.strip, utils.QM('[' in line, line.split(' = ', 1), rsplit(line, '=', 1)))
					if len(tmp) != 2:
						raise DatasetError('Malformed entry in dataset file:\n%s' % line)
					(key, value) = (tmp[0], tmp[1])  # avoid false positives for unpacking checkers
					handler_info = self._entry_handler_info.get(key.lower())
					if handler_info:
						(prop, parser, msg) = handler_info
						if prop is not None:
							block[prop] = _try_apply(value, parser, msg)
					else:
						block[DataProvider.FileList].append(self._parse_entry(block, key, value))
			except Exception:
				raise DatasetError('Unable to parse %s:%d\n\t%s' % (repr(self._filename), idx, repr(line)))
		if block:
			yield self._finish_block(block)


def _try_apply(value, fun, desc):
	try:
		return fun(value)
	except Exception:
		raise DatasetError('Unable to parse %s: %s' % (desc, repr(value)))
