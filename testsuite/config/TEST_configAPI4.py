#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, run_test
from grid_control.config.cfiller_base import ConfigFiller
from python_compat import lmap


def create(values):
	cf = ConfigFiller.create_instance('StringConfigFiller', values)
	return create_config(additional=[cf])

def testmod(ref, value, *args):
	config = create(lmap(lambda x: '[section] key %s' % x, value.split()))
	try:
		var = config.get('key', *args).replace('\n', ' - ')
	except Exception:
		var = '<exception>'
	if ref != var:
		return (ref, var)

class Test_ConfigViews:
	"""
	>>> testmod('value', '=value')
	>>> testmod('value', '?=value')
	>>> testmod('value', '=value', '<default>')

	next is complicated - a dynamic set would write to the default section (with higher and inverted priority!)

	>>> testmod('<default>', '?=value', '<default>')

	>>> testmod('a', '=a ?=b')
	>>> testmod('a', '=a ?=b', '0')
	>>> testmod('c', '=a ?=b *=c')
	>>> testmod('c', '=a ?=b *=c', '0')
	>>> testmod('c', '=a ?=b *=c =d')
	>>> testmod('c', '=a ?=b *=c =d', '0')
	>>> testmod('c', '=a ?=b *=c *=d')
	>>> testmod('c', '=a ?=b *=c *=d', '0')

	>>> testmod('b', '^=a *=b')
	>>> testmod('b', '^=a *=b', '0')
	>>> testmod('b', '+=a *=b')
	>>> testmod('b', '+=a *=b', '0')

	>>> testmod('b - a', '+=a ^=b')
	>>> testmod('b - 0 - a', '+=a ^=b', '0')
	>>> testmod('c - b - a', '+=a ^=b ^=c')
	>>> testmod('c - b - 0 - a', '+=a ^=b ^=c', '0')
	>>> testmod('d - b - c - a', '+=a ^=b ?=c ^=d')
	>>> testmod('d - b - 0 - a', '+=a ^=b ?=c ^=d', '0')

	>>> testmod('<exception>', '+=a ^=b ?=c ^=d -=')
	>>> testmod('e', '+=a ^=b ?=c ^=d -= =e')

	>>> testmod('a', '=abc -=bc')
	>>> testmod('', '=a -=a')
	>>> testmod('b', '+=a =b')
	>>> testmod('b - a', '+=a !=b')
	"""

run_test()
