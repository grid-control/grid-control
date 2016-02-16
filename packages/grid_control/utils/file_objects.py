#-#  Copyright 2014-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import gzip
from python_compat import BytesBufferBase, bytes2str, imap, str2bytes, tarfile

class SafeFile(object):
	def __init__(self, fn, mode = 'r', keepOld = False):
		assert(mode in ['r', 'w'])
		self._mode = mode
		self._alias = fn
		if mode == 'w':
			self._alias = fn + '.tmp'
		self._fp = open(fn, mode)

	def write(self, value):
		self._fp.write(value)
		self._fp.truncate()

	def __del__(self):
		self._fp.close()


class VirtualFile(BytesBufferBase):
	def __init__(self, name, lines):
		BytesBufferBase.__init__(self, str2bytes(str.join('', lines)))
		self.name = name
		self.size = len(self.getvalue())

	def getTarInfo(self):
		info = tarfile.TarInfo(self.name)
		info.size = self.size
		return (info, self)


class ZipFile(object):
	def __init__(self, fn, mode):
		self._fp = gzip.open(fn, mode)

	def write(self, data):
		self._fp.write(str2bytes(data))

	def readline(self):
		return bytes2str(self._fp.readline())

	def readlines(self):
		return imap(bytes2str, self._fp.readlines())

	def close(self):
		return self._fp.close()
