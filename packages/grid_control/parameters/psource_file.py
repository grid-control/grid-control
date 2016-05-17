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
from grid_control import utils
from grid_control.parameters.psource_base import ParameterInfo, ParameterMetadata, ParameterSource
from grid_control.parameters.psource_basic import InternalParameterSource
from grid_control.utils.file_objects import ZipFile
from grid_control.utils.parsing import parseJSON
from python_compat import ifilter, imap, irange, izip, json, lfilter, lmap, sorted

# Reader for grid-control dump files - getHash is not implemented to keep it from being used by users
class GCDumpParameterSource(ParameterSource):
	def __init__(self, fn):
		ParameterSource.__init__(self)
		fp = ZipFile(fn, 'r')
		try:
			keyline = fp.readline().lstrip('#').strip()
			self._keys = []
			if keyline:
				self._keys = parseJSON(keyline)
			def parseLine(line):
				if not line.startswith('#'):
					pNumStr, stored = lmap(str.strip, line.split('\t', 1))
					return ('!' in pNumStr, int(pNumStr.rstrip('!')), lmap(parseJSON, stored.split('\t')))
			self._values = lmap(parseLine, fp.readlines())
		finally:
			fp.close()

	def getMaxParameters(self):
		return len(self._values)

	def fillParameterKeys(self, result):
		result.extend(imap(ParameterMetadata, self._keys))

	def fillParameterInfo(self, pNum, result):
		result[ParameterInfo.ACTIVE] = not self._values[pNum][0]
		for (key, value) in izip(self._keys, self._values[pNum][2]):
			if value is not None:
				result[key] = value

	def write(cls, fn, pa):
		fp = ZipFile(fn, 'w')
		try:
			keys = sorted(ifilter(lambda p: not p.untracked, pa.getJobKeys()))
			fp.write('# %s\n' % json.dumps(keys))
			maxN = pa.getMaxJobs()
			if maxN:
				activity = utils.ActivityLog('Writing parameter dump')
				for jobNum in irange(maxN):
					activity.finish()
					activity = utils.ActivityLog('Writing parameter dump [%d/%d]' % (jobNum + 1, maxN))
					meta = pa.getJobInfo(jobNum)
					if meta.get(ParameterInfo.ACTIVE, True):
						fp.write('%d\t%s\n' % (jobNum, str.join('\t', imap(lambda k: json.dumps(meta.get(k, '')), keys))))
					else:
						fp.write('%d!\t%s\n' % (jobNum, str.join('\t', imap(lambda k: json.dumps(meta.get(k, '')), keys))))
				activity.finish()
		finally:
			fp.close()
	write = classmethod(write)


# Reader for CSV files
class CSVParameterSource(InternalParameterSource):
	def __init__(self, fn, format = 'sniffed'):
		sniffed = csv.Sniffer().sniff(open(fn).readline())
		csv.register_dialect('sniffed', sniffed)
		tmp = list(csv.DictReader(open(fn), dialect = format))

		def cleanupDict(d):
			# strip all key value entries
			tmp = tuple(imap(lambda item: lmap(str.strip, item), d.items()))
			# filter empty parameters
			return lfilter(lambda k_v: k_v[0] != '', tmp)
		keys = []
		if len(tmp):
			keys = lmap(ParameterMetadata, tmp[0].keys())
		values = lmap(lambda d: dict(cleanupDict(d)), tmp)
		InternalParameterSource.__init__(self, values, keys)

	def create(cls, pconfig = None, src = 'CSV'): # pylint:disable=arguments-differ
		fn = pconfig.get(src, 'source')
		return CSVParameterSource(fn, pconfig.get(src, 'format', 'sniffed'))
	create = classmethod(create)
ParameterSource.managerMap['csv'] = 'CSVParameterSource'
