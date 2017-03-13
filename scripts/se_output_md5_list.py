#!/usr/bin/env python
# | Copyright 2017 Karlsruhe Institute of Technology
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

import os, sys, logging
from gc_scripts import FileInfo, get_script_object_cmdline, iter_jobnum_output_dn, iter_output_files


def _main():
	if len(sys.argv) < 2:
		sys.stderr.write('Syntax: %s <config file> [<job selector>]\n\n' % sys.argv[0])
		sys.exit(os.EX_USAGE)
	script_obj = get_script_object_cmdline(sys.argv[1:], only_success=True)
	base_output_dn = script_obj.config.get_work_path('output')
	for (_, output_dn) in iter_jobnum_output_dn(base_output_dn, script_obj.job_db.get_job_list()):
		for fi in iter_output_files(output_dn):
			se_path = fi[FileInfo.Path].replace('file://', '').replace('dir://', '')
			logging.getLogger('script').info('%s  %s/%s', fi[FileInfo.Hash], se_path, fi[FileInfo.NameDest])


if __name__ == '__main__':
	sys.exit(_main())
