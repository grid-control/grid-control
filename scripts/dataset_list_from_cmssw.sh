#!/bin/bash
# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

if [ -z "$1" ]; then
	echo "Usage: $0 DatasetName"
	echo "The list of files is taken from the standard input."
	echo "Example:"
	echo -e "\tls *.root | $0 MyDataset"
	exit 1
fi

if [ -z "$CMSSW_BASE" ]; then
	echo "This script requires an CMSSW environment!"
	exit 1
fi

echo "[$1]"
while read FILE; do
	EVENTS="$(edmFileUtil -e file://$FILE | grep "^$FILE" | sed -e "s/.*( \(.*\)events.*/\1/")"
	echo "file://$(readlink -e $FILE) = $EVENTS"
done
