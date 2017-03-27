#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import TestsuiteStream, create_config, run_test, testfwk_remove_files, try_catch, write_file
from grid_control.gc_plugin import NamedPlugin


class Test1(NamedPlugin):
	config_tag_name = 'tag'
	config_section_list = ['section1', 'section2']

class Test2(Test1):
	config_section_list = ['section3', 'section4']

write_file('configfile0.conf', """
[global]
include = configfile1.conf
include override = configfileA.conf

[section1]
option1 = f0s1o1_0
option1 += f0s1o1_1
option4 -=

[section2]
option1 += f0s2o1_0
option2 = f0s2o2_0
option3 += f0s2o3_0
	f0s2o3_0a

[section3]
option1 ^= f0s3s1_0

[section4]
option2 = f0s4o2_0
option3 ^= f0s4o3_0

[section1]
option1 += f0s1o1_2
""")

write_file('configfile1.conf', """
[global] include = configfile2.conf
[section1] option4 = f1s1o4_0
[section2] option3 += f1s2o3_0
[section3] option2 += f1s3o2_0
[section4] option3 ^= f1s4o3_0
""")

write_file('configfile2.conf', """
[section2] option3 += f2s2o3_0
""")

write_file('configfileA.conf', """
[section2] option3 += fAs2o3_0
""")

class Test_ConfigViews:
	"""
	>>> config = create_config(config_file='configfile0.conf')
	>>> config
	<SimpleConfigInterface(view = <SimpleConfigView(sections = None)>)>
	>>> config.write(TestsuiteStream())
	[global]
	plugin paths += <testsuite dir>/config
	-----
	[global!]
	config id ?= configfile0
	plugin paths ?= <testsuite dir>/config
	workdir ?= <testsuite dir>/config/work.configfile0
	workdir base ?= <testsuite dir>/config
	-----
	[section1]
	option1 = f0s1o1_0
	option1 += f0s1o1_1
	option1 += f0s1o1_2
	option4 = f1s1o4_0
	option4 -=
	-----
	[section2]
	option1 += f0s2o1_0
	option2 = f0s2o2_0
	option3 += f2s2o3_0
	option3 += f1s2o3_0
	option3 +=
	  f0s2o3_0
	  f0s2o3_0a
	option3 += fAs2o3_0
	-----
	[section3]
	option1 ^= f0s3s1_0
	option2 += f1s3o2_0
	-----
	[section4]
	option2 = f0s4o2_0
	option3 ^= f1s4o3_0
	option3 ^= f0s4o3_0
	-----
	>>> test1 = Test1(config, 'test1')
	>>> test2 = Test2(config, 'test2')

	>>> view1 = config.change_view(view_class='TaggedConfigView', set_names=['name1'], set_classes=[Test1], set_tags=[test1])
	>>> view1
	<SimpleConfigInterface(view = <TaggedConfigView(class = ['section1', 'section2'], sections = None, names = ['name1'], tags = [('tag', 'test1')])>)>
	>>> view2 = config.change_view(view_class='TaggedConfigView', set_names=['name2'], set_classes=[Test2], set_tags=[test1, test2])
	>>> view2
	<SimpleConfigInterface(view = <TaggedConfigView(class = ['section3', 'section4'], sections = None, names = ['name2'], tags = [('tag', 'test1'), ('tag', 'test2')])>)>
	>>> try_catch(lambda: config.change_view(view_class='TaggedConfigView', set_tags=[view1]), 'APIError', 'does not define a valid tag name')
	caught

	>>> view1.set('option3', 'x1o3', '?=') is None
	False
	>>> view1.set('option3', 'x2o3', '?=') is None
	False
	>>> view1.set('option3', 'x3o3', '?=') is None
	False
	>>> view2.set('option3', 'x4o3', '?=') is None
	False
	>>> try_catch(lambda: view2.set('option_crash', view2), 'APIError', 'invalid object')
	caught

	>>> view1.get_option_list()
	['option1', 'option2', 'option3', 'option4']
	>>> 'v1o1', view1.get_list('option1', ['do1'])
	('v1o1', ['f0s1o1_0', 'f0s1o1_1', 'f0s1o1_2', 'f0s2o1_0'])
	>>> 'v1o2', view1.get_list('option2', ['do2'])
	('v1o2', ['f0s2o2_0'])
	>>> 'v1o3', view1.get_list('option3', ['do3'])
	('v1o3', ['x3o3', 'f2s2o3_0', 'f1s2o3_0', 'f0s2o3_0', 'f0s2o3_0a', 'fAs2o3_0'])
	>>> 'v1o4', view1.get_list('option4', ['do4'])
	('v1o4', ['do4'])

	>>> view2.get_option_list()
	['option1', 'option2', 'option3']
	>>> 'v2o1', view2.get_list('option1', ['do1'])
	('v2o1', ['f0s3s1_0', 'do1'])
	>>> 'v2o2', view2.get_list('option2', ['do2'])
	('v2o2', ['f0s4o2_0'])
	>>> 'v2o3', view2.get_list('option3', ['do3'])
	('v2o3', ['f0s4o3_0', 'f1s4o3_0', 'x4o3'])

	>>> config.write(TestsuiteStream(), print_source=False, print_minimal=True, print_default=False)
	[global]
	plugin paths += <testsuite dir>/config
	-----
	[section1]
	option1 =
	  f0s1o1_0
	  f0s1o1_1
	  f0s1o1_2
	option4 -=
	-----
	[section2]
	option1 += f0s2o1_0
	option2 = f0s2o2_0
	option3 +=
	  f2s2o3_0
	  f1s2o3_0
	  f0s2o3_0
	  f0s2o3_0a
	  fAs2o3_0
	-----
	[section2 name1 tag:test1!]
	option3 ?= x1o3
	-----
	[section3]
	option1 ^= f0s3s1_0
	option2 += f1s3o2_0
	-----
	[section4]
	option2 = f0s4o2_0
	option3 ^=
	  f0s4o3_0
	  f1s4o3_0
	-----
	[section4 name2 tag:test1 tag:test2!]
	option3 ?= x4o3
	-----
	"""

run_test(cleanup_fun=lambda: testfwk_remove_files(['configfile0.conf', 'configfile1.conf', 'configfile2.conf', 'configfileA.conf']))
