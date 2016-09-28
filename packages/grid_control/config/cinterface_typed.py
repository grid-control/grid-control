# | Copyright 2014-2016 Karlsruhe Institute of Technology
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

import os, sys, signal
from grid_control.config.cinterface_base import ConfigInterface
from grid_control.config.config_entry import ConfigError, add_config_suffix
from grid_control.config.cview_base import SimpleConfigView
from grid_control.config.matcher_base import DictLookup, ListFilter, ListOrder, Matcher
from grid_control.utils import resolve_path, resolve_paths
from grid_control.utils.data_structures import make_enum
from grid_control.utils.parsing import parse_bool, parse_dict, parse_list, parse_time, str_dict_cfg, str_time_short
from grid_control.utils.thread_tools import GCEvent
from hpfwk import APIError, ExceptionCollector, Plugin
from python_compat import any, get_user_input, identity, ifilter, imap, lmap, sorted, unspecified


class TypedConfigInterface(ConfigInterface):
	# Config interface class accessing typed data using an string interface provided by configView
	def get_bool(self, option, default=unspecified, **kwargs):
		# Getting boolean config options - feature: true and false are not the only valid expressions
		def str2obj(value):
			result = parse_bool(value)
			if result is None:
				raise ConfigError('Valid boolean expressions are: "true", "false"')
			return result
		return self._get_internal('bool', bool.__str__, str2obj, None, option, default, **kwargs)

	def get_composited_plugin(self, option, default=unspecified,
			default_compositor=unspecified, option_compositor=None,
			cls=Plugin, tags=None, inherit=False, require_plugin=True,
			pargs=None, pkwargs=None, **kwargs):
		# Return composite class - default classes are also given in string form!
		cls_list = []
		for factory in self._get_plugin_factory_list(option, default, cls, tags, inherit, require_plugin,
				single_plugin=False, desc='composite plugin', **kwargs):
			cls_list.append(factory.create_instance_bound(*(pargs or ()), **(pkwargs or {})))
		if len(cls_list) == 1:
			return cls_list[0]
		elif not cls_list:  # require_plugin == False
			return None
		if not option_compositor:
			option_compositor = add_config_suffix(option, 'manager')
		return self.get_plugin(option_compositor, default_compositor, cls, tags, inherit,
			pargs=tuple([cls_list] + list(pargs or [])), **kwargs)

	def get_dict(self, option, default=unspecified, parser=identity, strfun=str, **kwargs):
		# Returns a tuple with (<dictionary>, <keys>) - the keys are sorted by order of appearance
		# Default key is accessed via key == None (None is never in keys!)
		return self._get_internal('dictionary',
			obj2str=lambda value: str_dict_cfg(value, parser, strfun),
			str2obj=lambda value: parse_dict(value, parser),
			def2obj=lambda value: (value, sorted(ifilter(lambda key: key is not None, value.keys()))),
			option=option, default_obj=default, **kwargs)

	def get_float(self, option, default=unspecified, **kwargs):
		# Handling floating point config options - using strict float (de-)serialization
		return self._get_internal('float', float.__str__, float, None, option, default, **kwargs)

	def get_int(self, option, default=unspecified, **kwargs):
		# Getting integer config options - using strict integer (de-)serialization
		return self._get_internal('int', int.__str__, int, None, option, default, **kwargs)

	def get_list(self, option, default=unspecified, parse_item=identity, **kwargs):
		# Get whitespace separated list (space, tab, newline)
		return self._get_internal('list',
			obj2str=lambda value: '\n' + str.join('\n', imap(str, value)),
			str2obj=lambda value: lmap(parse_item, parse_list(value, None)),
			def2obj=None, option=option, default_obj=default, **kwargs)

	def get_path(self, option, default=unspecified, must_exist=True, **kwargs):
		# Return resolved path (search paths given in config_vault['path:search'])
		def parse_path(value):
			if value == '':
				return ''
			return self.resolve_path(value, must_exist, 'Error resolving path %s' % value)
		return self._get_internal('path', obj2str=str.__str__, str2obj=parse_path, def2obj=None,
			option=option, default_obj=default, **kwargs)

	def get_path_list(self, option, default=unspecified, must_exist=True, **kwargs):
		# Return multiple resolved paths (each line processed same as get_path)
		def patlist2pathlist(value, must_exist):
			exc = ExceptionCollector()
			search_path_list = self._config_view.config_vault.get('path:search', [])
			for pattern in value:
				try:
					for fn in resolve_paths(pattern, search_path_list, must_exist, ConfigError):
						yield fn
				except Exception:
					exc.collect()
			exc.raise_any(ConfigError('Error resolving paths'))
		return self._get_internal('paths',
			obj2str=lambda value: '\n' + str.join('\n', patlist2pathlist(value, False)),
			str2obj=lambda value: list(patlist2pathlist(parse_list(value, None), must_exist)),
			def2obj=None, option=option, default_obj=default, **kwargs)

	def get_plugin(self, option, default=unspecified,
			cls=Plugin, tags=None, inherit=False, require_plugin=True, pargs=None, pkwargs=None, **kwargs):
		# Return class - default class is also given in string form!
		factories = self._get_plugin_factory_list(option, default, cls, tags, inherit, require_plugin,
			single_plugin=True, desc='plugin', **kwargs)
		if factories:
			return factories[0].create_instance_bound(*(pargs or ()), **(pkwargs or {}))

	def get_time(self, option, default=unspecified, **kwargs):
		# Get time in seconds - input base is hours
		def str2obj(value):
			try:
				return parse_time(value)  # empty or negative values are mapped to -1
			except Exception:
				raise ConfigError('Valid time expressions have the format: hh[:mm[:ss]]')
		return self._get_internal('time', str_time_short, str2obj, None, option, default, **kwargs)

	def resolve_path(self, value, must_exist, error_msg):
		# Resolve path
		try:
			return resolve_path(value, self._config_view.config_vault.get('path:search', []),
				must_exist, ConfigError)
		except Exception:
			raise ConfigError(error_msg)

	def set_bool(self, option, value, opttype='=', source=None):
		# Setting boolean config options
		return self._set_internal('bool', bool.__str__, option, value, opttype, source)

	def set_int(self, option, value, opttype='=', source=None):
		# Setting integer config options - using strict integer (de-)serialization
		return self._set_internal('int', int.__str__, option, value, opttype, source)

	def set_time(self, option, value, opttype='=', source=None):
		# Set time in seconds - input base is hours
		return self._set_internal('time', str_time_short, option, value, opttype, source)

	def _get_plugin_factory_list(self, option, default=unspecified,
			cls=Plugin, tags=None, inherit=False, require_plugin=True, single_plugin=False,
			desc='plugin factories', **kwargs):
		if isinstance(cls, str):
			cls = Plugin.get_class(cls)

		def _bind_plugins(value):
			obj_list = list(cls.bind(value, config=self, inherit=inherit, tags=tags or []))
			if single_plugin and len(obj_list) > 1:
				raise ConfigError('This option only allows to specify a single plugin!')
			if require_plugin and not obj_list:
				raise ConfigError('This option requires to specify a valid plugin!')
			return obj_list
		return self._get_internal(desc,
			obj2str=lambda value: str.join('\n', imap(lambda obj: obj.get_bind_value(), value)),
			str2obj=_bind_plugins, def2obj=_bind_plugins, option=option, default_obj=default, **kwargs)

CommandType = make_enum(['executable', 'command'])


class SimpleConfigInterface(TypedConfigInterface):
	def __init__(self, configView, default_on_change=unspecified, default_on_valid=unspecified):
		TypedConfigInterface.__init__(self, configView, default_on_change, default_on_valid)
		self._interactive_enabled = None  # delay config query

	def get_choice(self, option, choices, default=unspecified,
			obj2str=str.__str__, str2obj=str, def2obj=None, **kwargs):
		default_str = self._get_default_str(default, def2obj, obj2str)

		def _cap_default(value):  # capitalize default value
			if value == default_str:
				return value.upper()
			return value.lower()
		choices_str = str.join('/', imap(_cap_default, imap(obj2str, choices)))
		if (default not in choices) and not unspecified(default):
			raise APIError('Invalid default choice "%s" [%s]!' % (default, choices_str))
		if 'interactive_msg' in kwargs:
			kwargs['interactive_msg'] += (' [%s]' % choices_str)

		def checked_str2obj(value):
			obj = str2obj(value)
			if obj not in choices:
				raise ConfigError('Invalid choice "%s" [%s]!' % (value, choices_str))
			return obj
		return self._get_internal('choice', obj2str, checked_str2obj, def2obj, option, default,
			interactive_msg_append_default=False, **kwargs)

	def get_choice_yes_no(self, option, default=unspecified, **kwargs):
		return self.get_choice(option, [True, False], default,
			obj2str={True: 'yes', False: 'no'}.get, str2obj=parse_bool, **kwargs)

	def get_command(self, option, default=unspecified, **kwargs):
		script_type = self.get_enum(add_config_suffix(option, 'type'),
			CommandType, CommandType.executable, **kwargs)
		if script_type == CommandType.executable:
			return self.get_path(option, default, **kwargs)
		return os.path.expandvars(self.get(option, default, **kwargs))

	def get_enum(self, option, enum, default=unspecified, subset=None, **kwargs):
		return self.get_choice(option, subset or enum.enum_value_list, default,
			obj2str=enum.enum2str, str2obj=enum.str2enum, **kwargs)

	def get_event(self, name):
		vault_key = 'event:%s' % name
		if vault_key not in self._config_view.config_vault:
			self._config_view.config_vault[vault_key] = GCEvent()
		return self._config_view.config_vault[vault_key]

	def get_filter(self, option, default=unspecified,
			negate=False, filter_parser=str, filter_str=str.__str__,
			default_matcher='start', default_filter='strict', default_order=ListOrder.source, **kwargs):
		matcher_opt = add_config_suffix(option, 'matcher')
		matcher_obj = self.get_plugin(matcher_opt, default_matcher,
			cls=Matcher, pargs=(matcher_opt,), pkwargs=kwargs)
		filter_expr = self.get(option, default, str2obj=filter_parser, obj2str=filter_str, **kwargs)
		filter_order_opt = add_config_suffix(option, 'order')
		filter_order = self.get_enum(filter_order_opt, ListOrder, default_order, **kwargs)
		return self.get_plugin(add_config_suffix(option, 'plugin'), default_filter, cls=ListFilter,
			pargs=(filter_expr, matcher_obj, filter_order, negate), **kwargs)

	def get_lookup(self, option, default=unspecified,
			default_matcher='start', single=True, include_default=False, **kwargs):
		matcher_args = {}
		if 'on_change' in kwargs:
			matcher_args['on_change'] = kwargs['on_change']
		matcher_opt = add_config_suffix(option, 'matcher')
		matcher_obj = self.get_plugin(matcher_opt, default_matcher,
			cls=Matcher, pargs=(matcher_opt,), **matcher_args)
		(source_dict, source_order) = self.get_dict(option, default, **kwargs)
		return DictLookup(source_dict, source_order, matcher_obj, single, include_default)

	def get_matcher(self, option, default=unspecified, default_matcher='start', negate=False,
			filter_parser=str, filter_str=str.__str__, **kwargs):
		matcher_opt = add_config_suffix(option, 'matcher')
		matcher_obj = self.get_plugin(matcher_opt, default_matcher,
			cls=Matcher, pargs=(matcher_opt,), pkwargs=kwargs)
		filter_expr = self.get(option, default, str2obj=filter_parser, obj2str=filter_str, **kwargs)
		return matcher_obj.create_matcher(filter_expr)

	def get_state(self, statename, detail='', default=False):
		# Get state - bool stored in hidden "state" section - any given detail overrides global state
		view = self.change_view(view_class=SimpleConfigView, setSections=['state'])
		state = view.get_bool('#%s' % statename, default, on_change=None)
		if detail:
			state = view.get_bool('#%s %s' % (statename, detail), state, on_change=None)
		return state

	def is_interactive(self, option, default):
		if isinstance(option, list):
			user_option_exists = any(imap(lambda opt: opt in self.get_option_list(), option))
		else:
			user_option_exists = option in self.get_option_list()
		# global switch to enable / disable interactive option queries
		config_interactive = self.change_view(interface_cls=TypedConfigInterface,
			view_class=SimpleConfigView, setSections=['interactive'])
		if self._interactive_enabled is None:
			self._interactive_enabled = config_interactive.get_bool('default', True, on_change=None)
		icfg = config_interactive.get_bool(add_config_suffix(option, 'interactive'),
			self._interactive_enabled and default, on_change=None)
		return icfg and not user_option_exists

	def prompt(self, prompt):
		return get_user_input('%s: ' % prompt)

	def set_choice(self, option, value, opttype='=', source=None, obj2str=str.__str__):
		return self._set_internal('choice', obj2str, option, value, opttype, source)

	def set_state(self, value, statename, detail=''):
		# Set state - bool stored in hidden "state" section
		option = ('#%s %s' % (statename, detail)).strip()
		view = self.change_view(view_class=SimpleConfigView, setSections=['state'])
		return view.set(option, str(value), '=')

	def _get_internal(self, desc, obj2str, str2obj, def2obj, option, default_obj,
			interactive=True, interactive_msg=None, interactive_msg_append_default=True, **kwargs):
		# interactive mode only overrides default values from the code
		if interactive_msg and self.is_interactive(option, interactive):
			prompt = interactive_msg
			if interactive_msg_append_default and not unspecified(default_obj):
				prompt += (' [%s]' % self._get_default_str(default_obj, def2obj, obj2str))
			while True:
				handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
				try:
					user_input = self.prompt(prompt).strip()
				except Exception:
					sys.stdout.write('\n')
					sys.exit(os.EX_DATAERR)
				signal.signal(signal.SIGINT, handler)
				if user_input != '':
					try:
						default_obj = str2obj(user_input)
					except Exception:
						sys.stdout.write('Unable to parse %s: %s\n' % (desc, user_input))
						continue
				break
		return TypedConfigInterface._get_internal(self, desc, obj2str, str2obj, def2obj,
			option, default_obj, **kwargs)
