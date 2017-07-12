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

import os, sys, signal
from grid_control.config.cinterface_base import ConfigInterface
from grid_control.config.config_entry import ConfigError, join_config_locations
from grid_control.config.cview_base import SimpleConfigView
from grid_control.config.matcher_base import DictLookup, ListFilter, ListOrder, Matcher
from grid_control.utils import resolve_path, resolve_paths
from grid_control.utils.data_structures import make_enum
from grid_control.utils.parsing import parse_bool, parse_dict_cfg, parse_list, parse_time, str_dict_cfg, str_time_short  # pylint:disable=line-too-long
from grid_control.utils.thread_tools import GCEvent
from grid_control.utils.user_interface import UserInputInterface
from hpfwk import APIError, ExceptionCollector, Plugin, clear_current_exception
from python_compat import any, identity, ifilter, imap, lmap, sorted, unspecified


CommandType = make_enum(['executable', 'command'])  # pylint: disable=invalid-name


class TypedConfigInterface(ConfigInterface):
	# Config interface class accessing typed data using an string interface provided by config_view
	def get_bool(self, option, default=unspecified, **kwargs):
		# Getting boolean config options - feature: true and false are not the only valid expressions
		def _str2obj(value):
			result = parse_bool(value)
			if result is None:
				raise ConfigError('Valid boolean expressions are: "true", "false"')
			return result
		return self._get_internal('bool', bool.__str__, _str2obj, None, option, default, **kwargs)

	def get_composited_plugin(self, option, default=unspecified, default_compositor=unspecified,
			option_compositor=None, cls=Plugin, require_plugin=True,
			pargs=None, pkwargs=None, bind_args=None, bind_kwargs=None, **kwargs):
		# Return composite class - default classes are also given in string form!
		cls_list = []
		for factory in self._get_plugin_factory_list(option, default, cls, require_plugin,
				single_plugin=False, desc='composite plugin',
				bind_args=bind_args, bind_kwargs=bind_kwargs, **kwargs):
			cls_list.append(factory.create_instance_bound(*(pargs or ()), **(pkwargs or {})))
		if len(cls_list) == 1:
			return cls_list[0]
		elif not cls_list:  # require_plugin == False
			return None
		if not option_compositor:
			option_compositor = join_config_locations(option, 'manager')
		return self.get_plugin(option_compositor, default_compositor, cls,
			pargs=tuple([cls_list] + list(pargs or [])),
			bind_args=bind_args, bind_kwargs=bind_kwargs, **kwargs)

	def get_dict(self, option, default=unspecified, default_order=None,
			parser=identity, strfun=str, **kwargs):
		# Returns a tuple with (<dictionary>, <keys>) - the keys are sorted by order of appearance
		# Default key is accessed via key == None (None is never in keys!)
		def _def2obj(value):
			return (value, default_order or sorted(ifilter(lambda key: key is not None, value.keys())))
		return self._get_internal('dictionary',
			obj2str=lambda value: str_dict_cfg(value, parser, strfun),
			str2obj=lambda value: parse_dict_cfg(value, parser),
			def2obj=_def2obj, option=option, default_obj=default, **kwargs)

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
		def _parse_path(value):
			if value == '':
				return ''
			return self.resolve_path(value, must_exist, 'Error resolving path %s' % value)
		return self._get_internal('path', obj2str=str.__str__, str2obj=_parse_path, def2obj=None,
			option=option, default_obj=default, **kwargs)

	def get_path_list(self, option, default=unspecified, must_exist=True, **kwargs):
		# Return multiple resolved paths (each line processed same as get_path)
		def _patlist2pathlist(value, must_exist):
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
			obj2str=lambda value: '\n' + str.join('\n', _patlist2pathlist(value, False)),
			str2obj=lambda value: list(_patlist2pathlist(parse_list(value, None), must_exist)),
			def2obj=None, option=option, default_obj=default, **kwargs)

	def get_plugin(self, option, default=unspecified, cls=Plugin,
			require_plugin=True, pargs=None, pkwargs=None, bind_args=None, bind_kwargs=None, **kwargs):
		# Return class - default class is also given in string form!
		factories = self._get_plugin_factory_list(option, default, cls, require_plugin,
			single_plugin=True, desc='plugin', bind_args=bind_args, bind_kwargs=bind_kwargs, **kwargs)
		if factories:
			return factories[0].create_instance_bound(*(pargs or ()), **(pkwargs or {}))

	def get_time(self, option, default=unspecified, **kwargs):
		# Get time in seconds - input base is hours
		def _str2obj(value):
			try:
				return parse_time(value)  # empty or negative values are mapped to -1
			except Exception:
				raise ConfigError('Valid time expressions have the format: hh[:mm[:ss]]')
		return self._get_internal('time', str_time_short, _str2obj, None, option, default, **kwargs)

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
			cls=Plugin, require_plugin=True, single_plugin=False,
			desc='plugin factories', bind_args=None, bind_kwargs=None, **kwargs):
		bind_kwargs = dict(bind_kwargs or {})
		bind_kwargs.setdefault('config', self)
		if isinstance(cls, str):
			cls = Plugin.get_class(cls)

		def _bind_plugins(value):
			obj_list = list(cls.bind(value, *(bind_args or []), **bind_kwargs))
			if single_plugin and len(obj_list) > 1:
				raise ConfigError('This option only allows to specify a single plugin!')
			if require_plugin and not obj_list:
				raise ConfigError('This option requires to specify a valid plugin!')
			return obj_list
		return self._get_internal(desc,
			obj2str=lambda value: str.join('\n', imap(lambda obj: obj.get_bind_value(), value)),
			str2obj=_bind_plugins, def2obj=_bind_plugins, option=option, default_obj=default, **kwargs)

	get_fn = get_path
	get_fn_list = get_path_list
	get_dn = get_path
	get_dn_list = get_path_list


class SimpleConfigInterface(TypedConfigInterface):
	def __init__(self, config_view, default_on_change=unspecified, default_on_valid=unspecified):
		TypedConfigInterface.__init__(self, config_view, default_on_change, default_on_valid)
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

		def _checked_str2obj(value):
			obj = str2obj(value)
			if obj not in choices:
				raise ConfigError('Invalid choice "%s" [%s]!' % (value, choices_str))
			return obj
		return self._get_internal('choice', obj2str, _checked_str2obj, def2obj, option, default,
			interactive_msg_append_default=False, **kwargs)

	def get_choice_yes_no(self, option, default=unspecified, **kwargs):
		return self.get_choice(option, [True, False], default,
			obj2str={True: 'yes', False: 'no'}.get, str2obj=parse_bool, **kwargs)

	def get_command(self, option, default=unspecified, **kwargs):
		script_type = self.get_enum(join_config_locations(option, 'type'),
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
			default_matcher='StartMatcher', default_filter='StrictListFilter',
			default_order=ListOrder.source, on_change=unspecified, **kwargs):
		matcher_opt = join_config_locations(option, 'matcher')
		matcher_obj = self.get_plugin(matcher_opt, default_matcher, cls=Matcher,
			pargs=(matcher_opt,), pkwargs=kwargs, on_change=on_change)
		filter_expr = self.get(option, default,
			str2obj=filter_parser, obj2str=filter_str, on_change=on_change, **kwargs)
		filter_order = self.get_enum(join_config_locations(option, 'order'), ListOrder,
			default_order, on_change=on_change, **kwargs)
		return self.get_plugin(join_config_locations(option, 'plugin'), default_filter, cls=ListFilter,
			pargs=(filter_expr, matcher_obj, filter_order, negate), on_change=on_change, **kwargs)

	def get_lookup(self, option, default=unspecified, default_matcher='StartMatcher',
			single=True, include_default=False, on_change=unspecified, **kwargs):
		matcher_args = {}
		if 'on_change' in kwargs:
			matcher_args['on_change'] = kwargs['on_change']
		matcher_opt = join_config_locations(option, 'matcher')
		matcher_obj = self.get_plugin(matcher_opt, default_matcher,
			cls=Matcher, pargs=(matcher_opt,), on_change=on_change, **matcher_args)
		(source_dict, source_order) = self.get_dict(option, default, on_change=on_change, **kwargs)
		return DictLookup(source_dict, source_order, matcher_obj, single, include_default)

	def get_matcher(self, option, default=unspecified, default_matcher='StartMatcher',
			negate=False, filter_parser=None, filter_str=str.__str__, on_change=unspecified, **kwargs):
		matcher_opt = join_config_locations(option, 'matcher')
		matcher_obj = self.get_plugin(matcher_opt, default_matcher,
			cls=Matcher, pargs=(matcher_opt,), pkwargs=kwargs, on_change=on_change)

		def _filter_parser(value):
			return str.join(' ', value.split())
		filter_expr = self.get(option, default, str2obj=filter_parser or _filter_parser,
			obj2str=filter_str, on_change=on_change, **kwargs)
		return matcher_obj.create_matcher(filter_expr)

	def get_state(self, statename, detail='', default=False):
		# Get state - bool stored in hidden "state" section - any given detail overrides global state
		view = self.change_view(view_class=SimpleConfigView, set_sections=['state'])
		state = view.get_bool('#%s' % statename, default, on_change=None)
		if detail:
			state = view.get_bool('#%s %s' % (statename, detail), state, on_change=None)
		return state

	def is_interactive(self, option, default):
		option_list_all = self.get_option_list()
		if isinstance(option, list):
			user_option_exists = any(imap(option_list_all.__contains__, option))
		else:
			user_option_exists = option in option_list_all
		# global switch to enable / disable interactive option queries
		config_interactive = self.change_view(interface_cls=TypedConfigInterface,
			view_class=SimpleConfigView, set_sections=['interactive'])
		if self._interactive_enabled is None:
			self._interactive_enabled = config_interactive.get_bool('default', True, on_change=None)
		icfg = config_interactive.get_bool(join_config_locations(option, 'interactive'),
			self._interactive_enabled and default, on_change=None)
		return icfg and not user_option_exists

	def set_choice(self, option, value, opttype='=', source=None, obj2str=str.__str__):
		return self._set_internal('choice', obj2str, option, value, opttype, source)

	def set_state(self, value, statename, detail=''):
		# Set state - bool stored in hidden "state" section
		option = ('#%s %s' % (statename, detail)).strip()
		view = self.change_view(view_class=SimpleConfigView, set_sections=['state'])
		return view.set(option, str(value), '=')

	def _get_internal(self, desc, obj2str, str2obj, def2obj, option, default_obj,
			interactive=True, interactive_msg=None, interactive_msg_append_default=True, **kwargs):
		# interactive mode only overrides default values from the code
		uii = UserInputInterface()
		if interactive_msg and self.is_interactive(option, interactive):
			prompt = interactive_msg
			if interactive_msg_append_default and not unspecified(default_obj):
				prompt += (' [%s]' % self._get_default_str(default_obj, def2obj, obj2str))
			while True:
				handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
				try:
					user_input = uii.prompt_text('%s: ' % prompt)
				except Exception:
					sys.exit(os.EX_DATAERR)
				signal.signal(signal.SIGINT, handler)
				if user_input != '':
					try:
						default_obj = str2obj(user_input)
					except Exception:
						clear_current_exception()
						self._log.warning('Unable to parse %s: %s\n', desc, user_input)
						continue
				break
		return TypedConfigInterface._get_internal(self, desc, obj2str, str2obj, def2obj,
			option, default_obj, **kwargs)
