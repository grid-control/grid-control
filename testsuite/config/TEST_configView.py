#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import re, sys
from testfwk import run_test
from grid_control.config.config_entry import ConfigContainer, ConfigEntry
from grid_control.config.cview_base import HistoricalConfigView
from python_compat import unspecified


def createHCV(cur={}, old=None):
	print('cur = %r\nold = %r' % (cur, old))
	def fill(container, values):
		if values != None:
			for key in values:
				container.append(ConfigEntry('x', key, values[key], '=', 'x'))

	curContainer = ConfigContainer('cur')
	fill(curContainer, cur)
	oldContainer = ConfigContainer('old')
	fill(oldContainer, old)
	if old == None:
		oldContainer.enabled = False
	hcv = HistoricalConfigView('view', oldContainer, curContainer)
	hcv._get_section = lambda *args, **kwargs: 'x'
	hcv._get_section_key = lambda x: x
	return hcv

def showGet(hcv, option, default, persistent):
	try:
		formatValue = lambda x: re.sub('<.*unspecified.*>', '<no default>', str(x))
		def formatEntry(entry):
			if entry:
				return str(formatValue(entry.value))
			return '<no entry>'
		msg = ['%7s' % persistent, '%15s' % formatValue(default), '%5s' % option, '=']
		(old, cur) = hcv.get([option], default, persistent)
		msg += ['old:', formatEntry(old)]
		msg += ['|', 'cur:', formatEntry(cur)]
		print(str.join(' ', msg))
	except Exception:
		msg += ['!EXCEPTION! %s' % sys.exc_info()[1].args]
		print(str.join(' ', msg))

def doTest(cur, old, persistent):
	hcv = createHCV(cur, old)
	showGet(hcv, 'key', unspecified, persistent)
	showGet(hcv, 'key', 'default1', persistent)
	showGet(hcv, 'key', unspecified, persistent)
	showGet(hcv, 'key', 'default2', persistent)
	hcv.write(sys.stdout, print_oneline=True)

class Test_ConfigView:
	"""
	>>> doTest({}, None, False)
	cur = {}
	old = None
	  False    <no default>   key = !EXCEPTION! "[x] key" does not exist!
	  False        default1   key = old: <no entry> | cur: default1
	  False    <no default>   key = old: <no entry> | cur: default1
	  False        default2   key = !EXCEPTION! Inconsistent default values! ('default1' != 'default2')
	[x!] key ?= default1
	>>> doTest({}, {}, False)
	cur = {}
	old = {}
	  False    <no default>   key = !EXCEPTION! "[x] key" does not exist!
	  False        default1   key = old: <no entry> | cur: default1
	  False    <no default>   key = old: <no entry> | cur: default1
	  False        default2   key = !EXCEPTION! Inconsistent default values! ('default1' != 'default2')
	[x!] key ?= default1
	>>> doTest({}, {'key': 'oldvalue'}, False)
	cur = {}
	old = {'key': 'oldvalue'}
	  False    <no default>   key = !EXCEPTION! "[x] key" does not exist!
	  False        default1   key = old: oldvalue | cur: default1
	  False    <no default>   key = old: oldvalue | cur: default1
	  False        default2   key = !EXCEPTION! Inconsistent default values! ('default1' != 'default2')
	[x!] key ?= default1
	>>> doTest({'key': 'value'}, None, False)
	cur = {'key': 'value'}
	old = None
	  False    <no default>   key = old: <no entry> | cur: value
	  False        default1   key = old: <no entry> | cur: value
	  False    <no default>   key = old: <no entry> | cur: value
	  False        default2   key = !EXCEPTION! Inconsistent default values! ('default1' != 'default2')
	[x] key = value
	[x!] key ?= default1
	>>> doTest({'key': 'value'}, {}, False)
	cur = {'key': 'value'}
	old = {}
	  False    <no default>   key = old: <no entry> | cur: value
	  False        default1   key = old: <no entry> | cur: value
	  False    <no default>   key = old: <no entry> | cur: value
	  False        default2   key = !EXCEPTION! Inconsistent default values! ('default1' != 'default2')
	[x] key = value
	[x!] key ?= default1
	>>> doTest({'key': 'value'}, {'key': 'oldvalue'}, False)
	cur = {'key': 'value'}
	old = {'key': 'oldvalue'}
	  False    <no default>   key = old: oldvalue | cur: value
	  False        default1   key = old: oldvalue | cur: value
	  False    <no default>   key = old: oldvalue | cur: value
	  False        default2   key = !EXCEPTION! Inconsistent default values! ('default1' != 'default2')
	[x] key = value
	[x!] key ?= default1


	>>> doTest({}, None, True)
	cur = {}
	old = None
	   True    <no default>   key = !EXCEPTION! "[x] key" does not exist!
	   True        default1   key = old: <no entry> | cur: default1
	   True    <no default>   key = old: <no entry> | cur: default1
	   True        default2   key = old: <no entry> | cur: default1
	[x!] key ?= default1
	>>> doTest({}, {}, True)
	cur = {}
	old = {}
	   True    <no default>   key = !EXCEPTION! "[x] key" does not exist!
	   True        default1   key = old: <no entry> | cur: default1
	   True    <no default>   key = old: <no entry> | cur: default1
	   True        default2   key = old: <no entry> | cur: default1
	[x!] key ?= default1
	>>> doTest({}, {'key': 'oldvalue'}, True)
	cur = {}
	old = {'key': 'oldvalue'}
	   True    <no default>   key = old: oldvalue | cur: oldvalue
	   True        default1   key = old: oldvalue | cur: oldvalue
	   True    <no default>   key = old: oldvalue | cur: oldvalue
	   True        default2   key = old: oldvalue | cur: oldvalue
	[x!] key ?= oldvalue
	>>> doTest({'key': 'value'}, None, True)
	cur = {'key': 'value'}
	old = None
	   True    <no default>   key = old: <no entry> | cur: value
	   True        default1   key = old: <no entry> | cur: value
	   True    <no default>   key = old: <no entry> | cur: value
	   True        default2   key = old: <no entry> | cur: value
	[x] key = value
	[x!] key ?= default1
	>>> doTest({'key': 'value'}, {}, True)
	cur = {'key': 'value'}
	old = {}
	   True    <no default>   key = old: <no entry> | cur: value
	   True        default1   key = old: <no entry> | cur: value
	   True    <no default>   key = old: <no entry> | cur: value
	   True        default2   key = old: <no entry> | cur: value
	[x] key = value
	[x!] key ?= default1
	>>> doTest({'key': 'value'}, {'key': 'oldvalue'}, True)
	cur = {'key': 'value'}
	old = {'key': 'oldvalue'}
	   True    <no default>   key = old: oldvalue | cur: value
	   True        default1   key = old: oldvalue | cur: value
	   True    <no default>   key = old: oldvalue | cur: value
	   True        default2   key = old: oldvalue | cur: value
	[x] key = value
	[x!] key ?= oldvalue
	"""

run_test()
