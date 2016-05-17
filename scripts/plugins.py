#!/usr/bin/env python
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

from gcSupport import Options, Plugin, scriptOptions, utils
from python_compat import lmap, sorted

parser = Options(usage = '%s <BasePlugin>')
options = scriptOptions(parser)
if not options.args:
	utils.exitWithUsage(parser.usage())

def getDisplayList(aliasDict):
	tableList = []
	for name in aliasDict:
		# sorted by length of name and depth
		by_len_depth = sorted(aliasDict[name], key = lambda d_a: (-len(d_a[1]), d_a[0]))
		# sorted by depth and name
		by_depth_name = sorted(aliasDict[name], key = lambda d_a: (d_a[0], d_a[1]))
		new_name = by_len_depth.pop()[1]
		aliasList = lmap(lambda d_a: d_a[1], by_depth_name)
		aliasList.remove(new_name)
		entry = {'Name': new_name, 'Alias': str.join(', ', aliasList)}
		if ('Multi' not in name) and ('Base' not in name):
			tableList.append(entry)
	return tableList

def displayList(clsList):
	header = [('Name', 'Name')]
	fmtString = 'l'
	for entry in clsList:
		if entry['Alias']:
			header.append(('Alias', 'Alias'))
			fmtString = 'rl'
			break
	utils.printTabular(header, sorted(clsList, key = lambda x: x['Name'].lower()), fmtString = fmtString)

if __name__ == '__main__':
	BasePlugin = Plugin.getClass(options.args[0])
	aliasDict = {}
	for entry in BasePlugin.getClassList():
		depth = entry.pop('depth', 0)
		(alias, name) = entry.popitem()
		aliasDict.setdefault(name, []).append((depth, alias))
	aliasDict.pop(options.args[0])
	displayList(getDisplayList(aliasDict))
