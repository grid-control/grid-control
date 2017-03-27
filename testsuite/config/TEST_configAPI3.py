#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import TestsuiteStream, create_config, run_test, testfwk_remove_files, write_file
from grid_control.gc_plugin import NamedPlugin


class Task(NamedPlugin):
	config_tag_name = 'tasktag'
	config_section_list = ['task']

class WMS(NamedPlugin):
	config_tag_name = 'wmstag'
	config_section_list = ['wms', 'backend']
	def __init__(self, config, name):
		config = config.change_view(set_sections=['global', 'dataset'])
		self._proxy = config.get('proxy', 'defaultproxy')

	def __repr__(self):
		return '%s(%s)' % (self.__class__.__name__, self._proxy)

class Local(WMS):
	config_section_list = WMS.config_section_list + ['local']
	def __init__(self, config, name):
		WMS.__init__(self, config, name)

class PBS(Local):
	config_section_list = Local.config_section_list + ['mybatch']
	def __init__(self, config, name):
		Local.__init__(self, config, name)

write_file('configfile.conf', """
[global]                       wms = PBS:mypbs

[global]                       proxy = global
[global tasktag:mytag]         proxy = global tasktag:mytag
[dataset]                      proxy = dataset
[dataset invalid]              proxy = dataset invalid
[dataset tasktag:mytag]        proxy = dataset tasktag:mytag
[wms]                          proxy = wms
[wms invalidtag:invalid]       proxy = wms invalidtag:invalid
[wms tasktag:mytag]            proxy = wms tasktag:mytag
[wms tasktag:invalid]          proxy = wms tasktag:invalid
[wms tasktag:datasettask]      proxy = wms tasktag:datasettask
[wms tasktag:mytag tasktag:datasettask] proxy = wms tasktag:mytag tasktag:datasettask
[wms mypbs]                    proxy = wms mypbs
[wms tasktag:mytag mypbs]      proxy = wms mypbs tasktag:mytag
[backend]                      proxy = backend
[backend tasktag:mytag]        proxy = backend tasktag:mytag
[backend mypbs]                proxy = backend mypbs
[backend tasktag:mytag mypbs]  proxy = backend mypbs tasktag:mytag
[local]                        proxy = local
[local tasktag:mytag]          proxy = local tasktag:mytag
[local mypbs]                  proxy = local mypbs
[local tasktag:mytag mypbs]    proxy = local mypbs tasktag:mytag
[mybatch]                      proxy = mybatch
[mybatch tasktag:mytag]        proxy = mybatch tasktag:mytag
[mybatch tasktag:mytask]       proxy = mybatch tasktag:mytask
[mybatch mypbs]                proxy = mybatch mypbs
[mybatch tasktag:mytag mypbs]  proxy = mybatch mypbs tasktag:mytag
""")

class Test_ConfigWMS(object):
	"""
	>>> config = create_config(config_file='configfile.conf')
	>>> config = config.change_view(view_class='TaggedConfigView', set_sections=['global'], set_tags=[Task(config, 'datasettask')])
	>>> config.get_plugin('wms', cls=WMS)
	PBS(mybatch mypbs)
	>>> config.get_plugin('wms', cls=WMS, bind_kwargs={'tags': [Task(config, 'mytag')]})
	PBS(mybatch mypbs tasktag:mytag)
	>>> config = config.change_view(set_sections=None)

	>>> config.write(TestsuiteStream(), print_source=True)
	[backend]
	proxy = backend                    ; <testsuite dir>/config/configfile.conf:16
	-----
	[dataset]
	proxy = dataset                    ; <testsuite dir>/config/configfile.conf:5
	-----
	[global]
	; source: configfile.conf
	plugin paths += <testsuite dir>/config
	proxy = global                     ; <testsuite dir>/config/configfile.conf:3
	wms = PBS:mypbs                    ; <testsuite dir>/config/configfile.conf:1
	-----
	[global!]
	config id ?= configfile            ; <default>
	; source: <default>
	plugin paths ?= <testsuite dir>/config
	; source: <default>
	workdir ?= <testsuite dir>/config/work.configfile
	; source: <default>
	workdir base ?= <testsuite dir>/config
	-----
	[local]
	proxy = local                      ; <testsuite dir>/config/configfile.conf:20
	-----
	[mybatch]
	proxy = mybatch                    ; <testsuite dir>/config/configfile.conf:24
	-----
	[wms]
	proxy = wms                        ; <testsuite dir>/config/configfile.conf:8
	-----
	[wms tasktag:datasettask]
	proxy = wms tasktag:datasettask    ; <testsuite dir>/config/configfile.conf:12
	-----
	[wms tasktag:mytag tasktag:datasettask]
	; source: <testsuite dir>/config/configfile.conf:13
	proxy = wms tasktag:mytag tasktag:datasettask
	-----
	[wms!]
	proxy ?= defaultproxy              ; <default>
	-----
	"""

run_test(cleanup_fun=lambda: testfwk_remove_files(['configfile.conf']))
