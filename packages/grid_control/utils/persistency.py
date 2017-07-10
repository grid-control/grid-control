# | Copyright 2013-2017 Karlsruhe Institute of Technology
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

from grid_control.utils.file_tools import SafeFile
from python_compat import identity, imap


def load_dict(fn, delimeter, fmt_key=identity, fmt_value=identity):
	result = {}
	for line in SafeFile(fn).iter_close():
		(key, value) = line.split(delimeter, 1)
		result[fmt_key(key.strip())] = fmt_value(value.strip())
	return result


def save_dict(value, fn, delimeter, fmt_key=identity, fmt_value=identity, key_list=None):
	SafeFile(fn, 'w').write_close(str.join('\n', imap(lambda key: '%s%s%s' % (
		fmt_key(key), delimeter, fmt_value(value[key])), key_list or value)))
