# | Copyright 2016 Karlsruhe Institute of Technology
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
from grid_control.backends import WMS
from hpfwk import APIError
from python_compat import identity, ifilter, imap, lmap

class JDLWriter(object):
	def __init__(self):
		self._esc_dict = {'\\': r'\\', '\"': r'\"', '\n': r'\n'}
		self._fmt = utils.DictFormat(' = ')


	def _escape(self, value):
		return '"' + str.join('', imap(lambda char: self._esc_dict.get(char, char), value)) + '"'

	def _format_reqs_storage(self, locations):
		if locations:
			location_iter = imap(lambda x: 'Member(%s, other.GlueCESEBindGroupSEUniqueID)' % self._escape(x), locations)
			return '( %s )' % str.join(' || ', location_iter)

	def _format_reqs_sites(self, sites):
		fmt = lambda x: 'RegExp(%s, other.GlueCEUniqueID)' % self._escape(x)
		(blacklist, whitelist) = utils.splitBlackWhiteList(sites)
		sitereqs = lmap(lambda x: '!' + fmt(x), blacklist)
		if whitelist:
			sitereqs.append('(%s)' % str.join(' || ', imap(fmt, whitelist)))
		if sitereqs:
			return '( %s )' % str.join(' && ', sitereqs)

	def _format_reqs(self, req_list, result):
		req_string_list = ['other.GlueHostNetworkAdapterOutboundIP']
		for req_type, arg in req_list:
			if req_type == WMS.SOFTWARE:
				req_string_list.append('Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' % self._escape(arg))
			elif req_type == WMS.WALLTIME:
				if arg > 0:
					req_string_list.append('(other.GlueCEPolicyMaxWallClockTime >= %d)' % int((arg + 59) / 60))
			elif req_type == WMS.CPUTIME:
				if arg > 0:
					req_string_list.append('(other.GlueCEPolicyMaxCPUTime >= %d)' % int((arg + 59) / 60))
			elif req_type == WMS.MEMORY:
				if arg > 0:
					req_string_list.append('(other.GlueHostMainMemoryRAMSize >= %d)' % arg)
			elif req_type == WMS.STORAGE:
				req_string_list.append(self._format_reqs_storage(arg))
			elif req_type == WMS.SITES:
				req_string_list.append(self._format_reqs_sites(arg))
			elif req_type == WMS.CPUS:
				pass # Handle number of cpus in makeJDL
			else:
				raise APIError('Unknown requirement type %r or argument %r' % (WMS.enum2str(req_type), arg))
		result['Requirements'] = str.join(' && ', ifilter(identity, req_string_list))

	def _format_cpus(self, req_list, result):
		for req_type, arg in req_list:
			if (req_type == WMS.CPUS) and (arg > 1):
				result['CpuNumber'] = arg

	def prepare(self, req_list, result = None):
		result = result or dict()
		self._format_reqs(req_list, result)
		self._format_cpus(req_list, result)
		return result

	def format(self, req_list, result = None):
		contents = self.prepare(req_list, result)
		return self._fmt.format(contents, format = '%s%s%s;\n')
