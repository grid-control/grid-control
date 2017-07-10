# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

import os, gzip
from python_compat import BytesBufferBase, bytes2str, identity, imap, str2bytes, tarfile


def erase_content(fp):
	fp.flush()
	fp.seek(0)
	fp.truncate(0)
	fp.flush()


def with_file(fp, fun):
	try:
		return fun(fp)
	finally:
		fp.close()


def with_file_iter(fp, iter_fun):
	try:  # old python versions can't use finally
		for result in iter_fun(fp):
			yield result
	except Exception:
		fp.close()
		raise
	fp.close()


class GZipTextFile(object):
	def __init__(self, fn, mode):
		self._fp = gzip.open(fn, mode)

	def close(self):
		return self._fp.close()

	def readline(self):
		return bytes2str(self._fp.readline())

	def readlines(self):
		return imap(bytes2str, self._fp.readlines())

	def write(self, data):
		self._fp.write(str2bytes(data))


class SafeFile(object):
	def __init__(self, fn, mode='r', keep_old=False):
		if mode not in ['r', 'w', 'rb', 'wb']:
			raise Exception('Invalid file mode selected: %r' % mode)
		(self._fn, self._fp, self._mode, self._keep_old) = (fn, None, mode, keep_old)
		if self._mode.startswith('w'):
			self._fp = open(self._fn + '.tmp', mode)
		else:
			self._fp = open(self._fn, mode)

	def __repr__(self):
		return '%s(fn = %r, mode = %r, keep_old = %s, handle = %r)' % (
			self.__class__.__name__, self._fn, self._mode, self._keep_old, self._fp)

	def close(self):
		if self._fp:
			self._fp.close()
			if self._mode.startswith('w'):
				if self._keep_old:
					os.rename(self._fn, self._fn + '.old')
				os.rename(self._fn + '.tmp', self._fn)
			self._fp = None

	def iter_close(self):
		return with_file_iter(self._fp, identity)

	def read(self):
		return self._fp.read()

	def read_close(self):
		try:
			return self.read()
		finally:
			self.close()

	def readlines(self):
		return self._fp.readlines()

	def write(self, value):
		self._fp.write(value)
		self._fp.truncate()

	def write_close(self, value):
		try:
			self.write(value)
		finally:
			self.close()

	def writelines(self, value):
		self._fp.writelines(value)
		self._fp.truncate()


class VirtualFile(BytesBufferBase):
	def __init__(self, name, lines):
		BytesBufferBase.__init__(self, str2bytes(str.join('', lines)))
		self.name = name
		self.size = len(self.getvalue())

	def __repr__(self):
		return '%s(%r, size=%d)' % (self.__class__.__name__, self.name, self.size)

	def close(self):
		self.seek(0)

	def get_tar_info(self):
		info = tarfile.TarInfo(self.name)
		info.size = self.size
		return (info, self)
