#!/usr/bin/env python
# | Copyright 2015-2016 Karlsruhe Institute of Technology
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

import os
from grid_control.gc_exceptions import UserError
from grid_control.utils.webservice import JSONRestClient

class DBS3LiteClient(object):
	def __init__(self, url):
		self._reader_url = '%s/%s' % (url, 'DBSReader')
		self._writer_url = '%s/%s' % (url, 'DBSWriter')
		self._migrate_url = '%s/%s' % (url, 'DBSMigrate')
		self._proxy_path = os.environ.get('X509_USER_PROXY', '')
		if not os.path.exists(self._proxy_path):
			raise UserError('VOMS proxy needed to query DBS3! Environment variable X509_USER_PROXY is "%s"' % self._proxy_path)
		self._jrc = JSONRestClient(cert = self._proxy_path)

	def listBlocks(self, **kwargs):
		return self._jrc.get(url=self._reader_url, api='blocks', params=kwargs)

	def listFiles(self, **kwargs):
		return self._jrc.get(url=self._reader_url, api='files', params=kwargs)

	def listFileParents(self, **kwargs):
		return self._jrc.get(url=self._reader_url, api='fileparents', params=kwargs)

	def insertBulkBlock(self, data):
		return self._jrc.post(url=self._writer_url, api='bulkblocks', data=data)

	def migrateSubmit(self, data):
		return self._jrc.post(url=self._migrate_url, api='submit', data=data)

	def migrateStatus(self, **kwargs):
		return self._jrc.get(url=self._migrate_url, api='status', params=kwargs)
