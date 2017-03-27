#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import time, random
from testfwk import create_config, testfwk_remove_files
from grid_control.parameters.padapter import TrackedParameterAdapter
from grid_control.parameters.psource_basic import SimpleParameterSource
from grid_control.parameters.psource_meta import CrossParameterSource
from python_compat import irange, lmap


p1v = lmap(lambda x: x + 100, irange(10))
p2v = lmap(lambda x: x + 200, irange(10))
p3v = lmap(lambda x: x + 300, irange(10))
p4v = lmap(lambda x: x + 400, irange(10))
p5v = lmap(lambda x: x + 500, irange(10))
p6v = lmap(lambda x: x + 600, irange(10))

config = create_config(config_dict = {'global': {'workdir': '.'}})

time_list = []

def main():
	testfwk_remove_files(['task.dat', 'params.dat.gz', 'params.map.gz'])
	for x in irange(20):
		p1 = SimpleParameterSource('A', list(p1v))
		p2 = SimpleParameterSource('B', list(p2v))
		p3 = SimpleParameterSource('C', list(p3v))
		p4 = SimpleParameterSource('D', list(p4v))
		p5 = SimpleParameterSource('E', list(p5v))
		p6 = SimpleParameterSource('F', list(p6v))
		plist = [p1, p2, p3, p4]
		random.shuffle(plist)
		ps = CrossParameterSource(*plist)
		start = time.time()
		pa = TrackedParameterAdapter(config, ps)
		end = time.time() - start
		if x != 0:
			print end, int(float(ps.get_parameter_len()) / end)
			time_list.append(float(ps.get_parameter_len()) / end)
		for pv in [p1v, p2v, p3v, p4v, p5v, p6v]:
			random.shuffle(pv)
	print time_list
	print sum(time_list) / len(time_list)

main()
