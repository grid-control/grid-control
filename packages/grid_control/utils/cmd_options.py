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

import sys, optparse # pylint:disable=deprecated-module

class Options(object):
	def __init__(self, usage = '', add_help_option = True):
		self._parser = optparse.OptionParser(usage = self._fmt_usage(usage), add_help_option = add_help_option)
		self._dest = []
		self._groups = {}
		self._groups_usage = {None: self._fmt_usage(usage)}
		self._defaults = []

	def parse(self, args = None, arg_keys = None):
		(opts, cmd_args) = self._parser.parse_args(args = args or sys.argv[1:])
		config_dict = {}
		for option in self._dest:
			if getattr(opts, option) is not None:
				config_dict[self._get_normed(option, ' ')] = str(getattr(opts, option))
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

	def addText(self, group, short, option, default = None, help = '', dest = None):
		return self._add(group, short, option, default, 'store', help, dest)

	def addList(self, group, short, option, default = None, help = '', dest = None):
		return self._add(group, short, option, default or [], 'append', help, dest)

	def addAccu(self, group, short, option, default = 0, help = '', dest = None):
		return self._add(group, short, option, default, 'count', help, dest)

	def addBool(self, group, short, option, default, help = '', dest = None):
		if default is False:
			return self._add(group, short, option, default, 'store_true', help, dest)
		return self._add(group, short, option, default, 'store_false', help, dest)

	def addFlag(self, group, short_pair, option_pair, default, help_pair = ('', ''), dest = None):
		if default:
			self._defaults.append(option_pair[0])
		else:
			self._defaults.append(option_pair[1])
		dest = dest or self._get_normed(option_pair[0], '_')
		self._add(group, short_pair[1], option_pair[1], default, 'store_false', help_pair[1], dest)
		return self._add(group, short_pair[0], option_pair[0], default, 'store_true', help_pair[0], dest)

	def addFSet(self):
		pass

	def _fmt_usage(self, usage):
		if '%s' in usage:
			return usage % sys.argv[0]
		return usage

	def _get_normed(self, value, norm):
		return value.replace('-', norm).replace(' ', norm).replace('_', norm)

	def _get_group(self, group):
		if group is None:
			return self._parser
		return self._groups[group]

	def _add(self, group, short, option, default, action, help_msg, dest):
		group = self._get_group(group)
		dest = dest or self._get_normed(option, '_')
		self._dest.append(dest)
		if short.strip():
			short = '-' + short
		return group.add_option(short.strip(), '--' + option, dest = dest,
			default = default, action = action, help = help_msg)
