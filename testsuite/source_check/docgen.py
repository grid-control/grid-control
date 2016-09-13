import json, sys, os
#sys.tracebacklimit = 0

try:
	unicode
	def remove_unicode(obj):
		if unicode == str:
			return obj
		if type(obj) in (list, tuple, set):
			(obj, oldType) = (list(obj), type(obj))
			for i, v in enumerate(obj):
				obj[i] = remove_unicode(v)
			obj = oldType(obj)
		elif isinstance(obj, dict):
			result = {}
			for k, v in obj.items():
				result[remove_unicode(k)] = remove_unicode(v)
			return result
		elif isinstance(obj, unicode):
			return obj.encode('utf-8')
		return obj
except NameError:
	def remove_unicode(obj):
		return obj


def get_json(fn):
	fp = open(fn)
	result = json.load(fp)
	fp.close()
	return remove_unicode(result)

config_calls = get_json('docgen_config_calls.json')
enums = get_json('docgen_enums.json')['enums']
plugin_infos = get_json('docgen_plugin_infos.json')
user = get_json('docgen_user.json')
user_used = set()
user_json = json.dumps(user, indent = 4, sort_keys = True).replace(' ' * 4, '\t').replace(' \n', '\n')
open('docgen_user.json', 'w').write(user_json)

opt_to_cc = {}

def mkOpt(value):
	tmp = value.lower().replace("'", ' ')
	while '  ' in tmp:
		tmp = tmp.replace('  ', ' ')
	return tmp.strip()

for cc in config_calls:
#	cc['id'] = os.urandom(10)
	if cc['fqfn'].startswith('pconfig') or cc['fqfn'].startswith('self._parameter_config'):
		continue
	cc['raw_args'] = list(cc['args'])
	arg_options = cc['args'].pop(0)
	if isinstance(arg_options, list):
		cc['options'] = list(map(mkOpt, arg_options))
	else:
		cc['options'] = [mkOpt(arg_options)]
	cc['option'] = cc['options'][-1].strip()

	if cc['api'] == 'get_enum':
		cc['enum'] = cc['args'].pop(0)

	cc['default'] = None
	if cc['args']:
		cc['default'] = cc['args'].pop(0)
		if cc['api'] in ['get_list', 'get_path_list']:
			if not isinstance(cc['default'], str):
				cc['default'] = repr(str.join(' ', map(lambda x: str(x).strip("'"), cc['default'])))
		if cc['api'] in ['get_enum']:
			for enum, enum_values in enums.items():
				if ('<name:%s>' % enum == cc['enum']):
					cc['enum'] = enum
					if enum_values == '<manual>':
						cc['enum_values'] = '<manual>'
					else:
						cc['enum_values'] = str.join('|', enum_values)
						for value in enum_values:
							cc['default'] = cc['default'].replace('<attr:%s>' % value, '%s' % value)
						break
	cc['default_raw'] = cc['default']

	if cc['api'] == 'get_composited_plugin':
		cc['compositor'] = cc['args'].pop(0)

	if cc['api'] == 'get_lookup':
		cc['default_matcher'] = cc['kwargs'].get('default_matcher', 'start')

	if cc['api'] == 'get_time':
		if isinstance(cc['default'], int):
			t = cc['default']
			if t > 0:
				cc['default'] = '%02d:%02d:%02d' % (int(t / 3600), (t / 60) % 60, t % 60)

	if cc['api'] == 'get_matcher':
		cc['default_matcher'] = cc['kwargs'].get('default_matcher', 'start')

	if cc['api'] == 'get_filter':
		cc['default_matcher'] = cc['kwargs'].get('default_matcher', 'start')
		cc['default_order'] = cc['kwargs'].get('default_order', '<attr:source>')
		for enum in enums['ListOrder']:
			cc['default_order'] = cc['default_order'].replace('<attr:%s>' % enum, enum)
		cc['default_filter'] = cc['kwargs'].get('default_filter', 'strict')

	if cc['api'] in ['get_plugin', 'get_composited_plugin']:
		cls_name = cc['kwargs'].pop('cls')
		if cls_name.startswith('<name:'):
			cc['cls'] = cls_name[6:-1]
		else:
			cc['cls'] = cls_name.strip("'")
		cc['cls_bases'] = plugin_infos.get(cc['cls'], {}).get('bases', []) + [cc['cls']]

	if cc['api'] in ['get_path', 'get_path_list']:
		cc['must_exist'] = cc['kwargs'].pop('must_exist', True)

	if cc['args']:
		print(json.dumps(cc, indent = 2))
		raise Exception('Unknown args!')

	cc['kwargs'].pop('strfun', None)
	cc['kwargs'].pop('parser', None)
	cc['kwargs'].pop('interactive', None)

	if cc['kwargs']:
		#print(json.dumps(cc, indent = 2))
		pass

	if cc['callers'][-1] == '__init__':
		cc['callers'].pop()
	cc['location'] = str.join('.', cc['callers'])
	cc['bases'] = plugin_infos.get(cc['callers'][0], {}).get('bases', [])
	opt_to_cc.setdefault(cc['option'], []).append(cc)

cc_by_location = {}
for location in user['manual options']:
	for cc in user['manual options'][location]:
		cc.update(user['api'][cc['api']])
		cc['output_altopt'] = ''
cc_by_location.update()

used_remap = set()

# Apply user documentation
for opt in list(opt_to_cc):
	for cc in opt_to_cc[opt]:
		if len(opt_to_cc[opt]) > 1:
			if (cc['option'] in user['options']) and not (user['options'][cc['option']].get('disable_dupe_check', False)):
				raise Exception('User option %s is not specific enough! %s' % (cc['options'], json.dumps(opt_to_cc[opt], indent = 2)))
		opt_to_cc[opt] = cc
		user_used.add(cc['option'])
		cc['output_altopt'] = ''
		if len(cc['options']) > 1:
			cc['output_altopt'] = ' / %s' % str.join(' / ', cc['options'][:-1])
		cc['option_display'] = cc['option']
		for entry in user['option_map']:
			cc['option_display'] = cc['option_display'].replace(entry, user['option_map'][entry])
			cc['output_altopt'] = cc['output_altopt'].replace(entry, user['option_map'][entry])
		cc.update(user['api'][cc['api']])
		cc.update(user['options'].get(cc['option'], {}))
		user_used.add(cc['location'] + ':' + cc['option'])
		cc.update(user['options'].get(cc['location'] + ':' + cc['option'], {}))
		if cc['location'] in user['location_remap']:
			used_remap.add(cc['location'])
			cc['location'] = user['location_remap'][cc['location']]

		if 'cls_bases' in cc:
			plugin_info = None
			for cls_base in cc['cls_bases']:
				plugin_info = user['plugin_details'].get(cls_base, plugin_info)
			cc['plugin_singular'] = plugin_info[0]
			cc['plugin_plural'] = plugin_info[1]

		cc['output_default'] = ''
		if cc['default'] is not None:
			default = str(cc['default']).strip()
			for call in cc.get('call', []):
				default = default.replace('<call:%s>' % call, cc['call'][call])
			default_map = cc.get('default_map', {})
			for key in default_map:
				if key not in default:
					raise Exception('Unused default map: %r = %r\n%r' % (key, default_map[key], default))
			default = default_map.get(default, default)
			cc['output_default'] = user['format']['output_default'] % default
		cc['user_text'] = cc.get('user_text', '') % cc

		add_options = []
		for sub_cc in cc.get('add_options', []):
			tmp = dict(cc)
			tmp.update(sub_cc)
			add_options.append(tmp)
		cc['add_options'] = add_options
		if not cc.get('disabled'):
			cc_by_location.setdefault(cc['location'], []).append(cc)

for location in user['location_force']:
	cc_by_location.setdefault(location, [])

for location in user['location_remap']:
	if location not in used_remap:
		raise Exception('Remap location is unused: %s' % location)

for location in user['location_whitelist'] + user['location_blacklist']:
	if location not in cc_by_location:
		raise Exception('User specified location %r does not exist' % location)

def display_option(cc):
	opt_line = (cc.get('opt_line', user['format']['output_opt']) % cc).strip()
	while '%(' in opt_line:
		opt_line = opt_line % cc
	for enum, enum_values in enums.items():
		if '<enum_values:%s>' % enum in opt_line:
			opt_line = opt_line.replace('<enum_values:%s>' % enum, str.join('|', enum_values))
	print('  * %s' % opt_line)
	desc = (cc.get('desc_line', user['format']['output_desc']) % cc).strip()
	if desc:
		print('    %s' % desc)
	else:
		if sys.argv[1:] == ['stop']:
			print cc['option']
			raise
		elif sys.argv[1:] == ['count']:
			print('N/A')


def display_location(location_list):
	if '.' in location_list[0]:
		print cc_by_location.get(location_list[0])
		raise Exception('Invalid location %r' % location_list[0])
	print('%s options' % location_list[0])
	print('-'*len('%s options' % location_list[0]))
	print('')
	all_cc = {}
	for location in location_list:
		for cc in cc_by_location.get(location, []):
			all_cc[cc['option']] = cc
	for opt in sorted(all_cc, key = lambda x: (all_cc[x].get('default') is not None, all_cc[x]['option_display'])):
		cc = all_cc[opt]
		display_option(cc)
		for sub_cc in cc.get('add_options', []):
			display_option(sub_cc)
	print('')

def display_location_deep(location):
	bases = [location]
	for base in plugin_infos.get(location, {}).get('bases', []):
		if base not in ['object', 'Plugin', 'ConfigurablePlugin', 'NamedPlugin']:
			bases.append(base)
	display_location(bases)

if True:
#	for location in cc_by_location:
	print('grid-control options')
	print('====================')
	print('')
	for location in user['location_whitelist']:
		display_location_deep(location)
	for location in sorted(cc_by_location, key = lambda loc: (tuple(plugin_infos.get(loc, {}).get('bases', [])), loc)):
		if location not in (user['location_blacklist'] + user['location_whitelist']):
			display_location_deep(location)

for entry in user['options']:
	if entry not in user_used:
		print 'Unused: %r %r' % (entry, user['options'][entry])
		print
