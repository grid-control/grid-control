# | Copyright 2012-2016 Karlsruhe Institute of Technology
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

import csv
from grid_control.parameters.psource_base import ParameterError, ParameterInfo, ParameterMetadata, ParameterSource
from grid_control.parameters.psource_basic import InternalParameterSource
from grid_control.utils.activity import Activity
from grid_control.utils.file_objects import ZipFile
from grid_control.utils.parsing import parse_json, str_dict
from python_compat import ifilter, imap, izip, json, lfilter, lmap, sorted


class CSVParameterSource(InternalParameterSource): # Reader for CSV files
	alias_list = ['csv']

	def __init__(self, fn, format = 'sniffed'):
		(self._fn, self._format) = (fn, format)
		fp = open(fn)
		try:
			first_line = fp.readline()
			sniffed = csv.Sniffer().sniff(first_line)
			csv.register_dialect('sniffed', sniffed)
			tmp = list(csv.DictReader(fp, first_line.strip().split(sniffed.delimiter) + [None], dialect = format))
		finally:
			fp.close()
		for entry in tmp:
			entry.pop(None, None)
			if None in entry.values():
				raise ParameterError('Malformed entry in csv file %r: {%s}' % (fn, str_dict(entry)))

		def cleanup_dict(d):
			# strip all key value entries
			tmp = tuple(imap(lambda item: lmap(str.strip, item), d.items()))
			# filter empty parameters
			return lfilter(lambda k_v: k_v[0] != '', tmp)
		keys = []
		if len(tmp):
			keys = sorted(imap(ParameterMetadata, tmp[0].keys()), key = lambda k: k.value)
		values = lmap(lambda d: dict(cleanup_dict(d)), tmp)
		InternalParameterSource.__init__(self, values, keys)

	def __repr__(self):
		if self._format == 'sniffed':
			return 'csv(%r)' % self._fn
		return 'csv(%r, %r)' % (self._fn, self._format)

	def create_psrc(cls, pconfig, repository, src = 'CSV'): # pylint:disable=arguments-differ
		fn = pconfig.get(src, 'source')
		return CSVParameterSource(fn, pconfig.get(src, 'format', 'sniffed'))
	create_psrc = classmethod(create_psrc)


class GCDumpParameterSource(ParameterSource): # Reader for grid-control dump files - get_psrc_hash is not implemented to keep it from being used by users
	def __init__(self, fn):
		ParameterSource.__init__(self)
		fp = ZipFile(fn, 'r')
		try:
			header = fp.readline().lstrip('#').strip()
			self._keys = []
			if header:
				self._keys = parse_json(header)
			def parse_line(line):
				if not line.startswith('#'):
					pNumStr, stored = lmap(str.strip, line.split('\t', 1))
					return ('!' in pNumStr, int(pNumStr.rstrip('!')), lmap(parse_json, stored.split('\t')))
			self._values = lmap(parse_line, fp.readlines())
		finally:
			fp.close()

	def fill_parameter_content(self, pNum, result):
		result[ParameterInfo.ACTIVE] = not self._values[pNum][0]
		for (key, value) in izip(self._keys, self._values[pNum][2]):
			if value is not None:
				result[key] = value

	def fill_parameter_metadata(self, result):
		result.extend(imap(ParameterMetadata, self._keys))

	def get_parameter_len(self):
		return len(self._values)

	def write(cls, fn, ps_len, ps_metadata, psp_iter): # write parameter part of parameter adapter
		fp = ZipFile(fn, 'w')
		try:
			vn_list = sorted(lmap(lambda p: p.value, ifilter(lambda p: not p.untracked, ps_metadata)))
			fp.write('# %s\n' % json.dumps(vn_list))
			activity = Activity('Writing parameter dump')
			for job_num, psp in enumerate(psp_iter):
				activity.update('Writing parameter dump [%d/%d]' % (job_num + 1, ps_len))
				psp_str = str.join('\t', imap(lambda k: json.dumps(psp.get(k, '')), vn_list))
				if psp.get(ParameterInfo.ACTIVE, True):
					fp.write('%d\t%s\n' % (job_num, psp_str))
				else:
					fp.write('%d!\t%s\n' % (job_num, psp_str))
			activity.finish()
		finally:
			fp.close()
	write = classmethod(write)
