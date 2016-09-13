import os, sys, json, getFiles

if sys.argv[1:]:
	base_dir = sys.argv[1]
else:
	base_dir = '../../packages'
sys.path.append(base_dir)
from hpfwk.hpf_plugin import get_plugin_list, import_modules

def select(path):
	for pat in ['/share', '_compat_', '/requests', '/xmpp']:
		if pat in path:
			return False
	return True

clsdata = {}
inherit_data = {}
clsconfig = {}
for package in os.listdir(base_dir):
	package = os.path.abspath(os.path.join(base_dir, package))
	if os.path.isdir(package):
		for cls in sorted(get_plugin_list(import_modules(os.path.abspath(package), select)), key = lambda c: c.__name__):
			cls_name = cls.__name__.split('.')[-1]
			if cls.config_section_list:
				clsdata.setdefault(cls_name, {})['config'] = cls.config_section_list
				clsdata.setdefault(cls_name, {}).setdefault('scope', {})['config'] = cls.config_section_list
			bases = list(map(lambda x: x.__name__, cls.iter_class_bases())[1:]) + ['object']
			bases.reverse()
			if bases:
				clsdata.setdefault(cls_name, {})['bases'] = bases
			else:
				print clsdata, bases
				
				raise

fp = open('docgen_plugin_infos.json', 'w')
json.dump(clsdata, fp, indent = 2)
fp.close()
