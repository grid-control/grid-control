#!/bin/bash
# | Copyright 2009-2011 Karlsruhe Institute of Technology
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

# grid-control: https://ekptrac.physik.uni-karlsruhe.de/trac/grid-control

# (jobNum, sandbox, stdout, stderr) (local.sh) (...)
export GC_SANDBOX="$2"
GC_STDOUT="$3"
GC_STDERR="$4"
shift 4
cd $GC_SANDBOX
(
	nice $@
) > "$GC_STDOUT" 2> "$GC_STDERR" &
echo $!
