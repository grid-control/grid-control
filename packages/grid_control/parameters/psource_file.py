# | Copyright 2012-2017 Karlsruhe Institute of Technology
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
from grid_control.parameters.psource_base import ParameterError, ParameterInfo, ParameterMetadata, ParameterSource  # pylint:disable=line-too-long
from grid_control.parameters.psource_internal import InternalParameterSource
from grid_control.utils.activity import ProgressActivity
from grid_control.utils.file_tools import GZipTextFile
from grid_control.utils.parsing import parse_json, str_dict_linear
from python_compat import ifilter, imap, izip, json, lfilter, lidfilter, lmap, sorted


class CSVParameterSource(InternalParameterSource):  # Reader for CSV files
	alias_list = ['csv']

	def __init__(self, fn, format='sniffed'):
		(self._fn, self._format) = (fn, format)
		fp = open(fn)
		try:
			first_line = fp.readline()
			sniffed = csv.Sniffer().sniff(first_line)
			csv.register_dialect('sniffed', sniffed)
			csv_header = first_line.strip().split(sniffed.delimiter) + [None]
			psp_list = list(csv.DictReader(fp, csv_header, dialect=format))
		finally:
			fp.close()
		for psp in psp_list:
			psp.pop(None, None)
			if None in psp.values():
				raise ParameterError('Malformed entry in csv file %r: {%s}' % (fn, str_dict_linear(psp)))

		def _cleanup_dict(mapping):  # strip all key value entries and filter empty parameters
			tmp = tuple(imap(lambda item: lmap(str.strip, item), mapping.items()))
			return dict(lfilter(lambda k_v: k_v[0] != '', tmp))
		output_vn_list = sorted(imap(ParameterMetadata, lidfilter(csv_header)), key=lambda k: k.value)
		InternalParameterSource.__init__(self, lmap(_cleanup_dict, psp_list), output_vn_list)

	def __repr__(self):
		if self._format == 'sniffed':
			return 'csv(%r)' % self._fn
		return 'csv(%r, %r)' % (self._fn, self._format)

	def create_psrc(cls, pconfig, repository, ref_name='CSV'):  # pylint:disable=arguments-differ
		fn = pconfig.get(ref_name, 'source')
		return CSVParameterSource(fn, pconfig.get(ref_name, 'format', 'sniffed'))
	create_psrc = classmethod(create_psrc)


class GCDumpParameterSource(ParameterSource):
	# Reader for grid-control dump files
	# get_psrc_hash is not implemented to keep it from being used by users
	def __init__(self, fn):
		ParameterSource.__init__(self)
		fp = GZipTextFile(fn, 'r')
		try:
			header = fp.readline().lstrip('#').strip()
			self._output_vn_list = []
			if header:
				self._output_vn_list = parse_json(header)

			def _parse_line(line):
				if not line.startswith('#'):
					pnum_str, stored_json = line.split('\t', 1)
					is_invalid = '!' in pnum_str
					pnum = int(pnum_str.replace('!', ' '))
					return (is_invalid, pnum, lmap(parse_json, stored_json.strip().split('\t')))
			self._values = lmap(_parse_line, fp.readlines())
		finally:
			fp.close()

	def fill_parameter_content(self, pnum, result):
		result[ParameterInfo.ACTIVE] = not self._values[pnum][0]
		for (output_vn, value) in izip(self._output_vn_list, self._values[pnum][2]):
			if value is not None:
				result[output_vn] = value

	def fill_parameter_metadata(self, result):
		result.extend(imap(ParameterMetadata, self._output_vn_list))

	def get_parameter_len(self):
		return len(self._values)

	def write(cls, fn, psrc_len, psrc_metadata, psp_iter):  # write parameter part of parameter adapter
		fp = GZipTextFile(fn, 'w')
		try:
			vn_list = sorted(lmap(lambda p: p.value, ifilter(lambda p: not p.untracked, psrc_metadata)))
			fp.write('# %s\n' % json.dumps(vn_list))
			progress = ProgressActivity('Writing parameter dump', progress_max=psrc_len)
			for jobnum, psp in enumerate(psp_iter):
				progress.update_progress(jobnum)
				psp_str = str.join('\t', imap(lambda k: json.dumps(psp.get(k, '')), vn_list))
				if psp.get(ParameterInfo.ACTIVE, True):
					fp.write('%d\t%s\n' % (jobnum, psp_str))
				else:
					fp.write('%d!\t%s\n' % (jobnum, psp_str))
			progress.finish()
		finally:
			fp.close()
	write = classmethod(write)
