#-#  Copyright 2016 Karlsruhe Institute of Technology
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

import sys, optparse

class Options(object):
	def __init__(self, usage = ''):
		self._parser = optparse.OptionParser(usage = self._fmt_usage(usage))
		self._dest = []
		self._groups = {}
		self._groups_usage = {None: self._fmt_usage(usage)}

	def parse(self, args = None, arg_keys = None):
		(opts, cmd_args) = self._parser.parse_args(args = args or sys.argv[1:])
		config_dict = {}
		for option in self._dest:
			config_dict[option.replace('_', ' ')] = str(getattr(opts, option))
		for arg_idx, arg_key in enumerate(arg_keys or []):
			if arg_idx < len(cmd_args):
				if arg_idx == len(arg_keys) - 1:
					config_dict[arg_key] = str.join(' ', cmd_args[arg_idx:])
				else:
					config_dict[arg_key] = cmd_args[arg_idx]
		return (opts, cmd_args, config_dict)

	def usage(self, name = None):
		return self._groups_usage[name]

	def section(self, name, desc, usage = ''):
		self._groups_usage[name] = self._fmt_usage(usage)
		if '%s' in usage:
			usage = 'Usage: ' + self._fmt_usage(usage)
		self._groups[name] = optparse.OptionGroup(self._parser, desc, usage)
		self._parser.add_option_group(self._groups[name])

	def addText(self, group, option, default = None, help = '', short = '', dest = None):
		return self._add(group, option, short, default, 'store', help, dest)

	def addList(self, group, option, default = None, help = '', short = '', dest = None):
		return self._add(group, option, short, default or [], 'append', help, dest)

	def addAccu(self, group, option, default = 0, help = '', short = '', dest = None):
		return self._add(group, option, short, default, 'count', help, dest)

	def addFlag(self, group, option, default, help, short = '', dest = None):
		if default == False:
			return self._add(group, option, short, default, 'store_true', help, dest)
		return self._add(group, option, short, default, 'store_false', help, dest)

	def _fmt_usage(self, usage):
		if '%s' in usage:
			return usage % sys.argv[0]
		return usage

	def _get_group(self, group):
		if group is None:
			return self._parser
		return self._groups[group]

	def _add(self, group, option, short, default, action, help_msg, dest):
		group = self._get_group(group)
		dest = dest or option.replace('-', '_').replace(' ', '_')
		self._dest.append(dest)
		return group.add_option(short, '--' + option, dest = dest,
			default = default, action = action, help = help_msg)
