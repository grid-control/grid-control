import os


for (root, dirs, files) in os.walk('.'):
	def findTestFwk(dn):
		if 'testfwk.py' in os.listdir(dn):
			return dn
		return findTestFwk(os.path.join(dn, '..'))
	for fn in files:
		if fn.startswith('TEST_') and fn.endswith('.py'):
			fn = os.path.join(root, fn)
			print(fn)
			lines = open(fn).readlines()
			for idx, line in enumerate(lines):
				if line.startswith('# - prolog marker'):
					break
			else:
				raise Exception('No prolog marker found in %r!' % fn)
			fp = open(fn, 'w')
			fp.write("""#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), %r))
__import__('testfwk').setup(__file__)
""" % findTestFwk(root).replace(root, '').lstrip('/'))
			fp.write('# - prolog marker\n')
			fp.write(str.join('', lines[idx+1:]))
			if not lines[-1].startswith('run_test(') and not (fn.endswith('fuzz.py') or fn.endswith('scale.py')):
				print('run_test missing')
