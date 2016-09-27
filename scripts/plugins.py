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

from gcSupport import Options, displayPluginList, get_pluginList, scriptOptions, utils


parser = Options(usage = '%s <BasePlugin>')
options = scriptOptions(parser)
if not options.args:
	utils.exit_with_usage(parser.usage())

if __name__ == '__main__':
	displayPluginList(get_pluginList(options.args[0]))
