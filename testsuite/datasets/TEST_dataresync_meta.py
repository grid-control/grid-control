#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import remove_files_testsuite, run_test
from grid_control import utils
from grid_control.datasets import DataProvider, DataSplitter
from testDS import printSplit
from testResync import doResync, do_initSplit


DP = DataProvider

class Test_SplitterResyncMeta:
	"""
	>>> utils.display_table.mode = 'parseable'

	DataI:
	metadata = ['KEY1', 'KEY2', 'KEY3']
	file1 = 10 ['Value11', 'Value12', 'Value13']
	file2 = 10 ['Value21', 'Value22', 'Value23']
	file3 = 10 ['Value31', 'Value32', 'Value33']
	filex = 10 ['TEST']

	>>> do_initSplit(doprint = False, nEvent = 15, srcFN = "dataI.dbs")
	>>> printSplit(DataSplitter.load_partitions_for_script("datamap-resync.tar"), meta = True)
	x|SIZE|SKIP|Files|Metadata|Invalid
	0|15|0|['file1', 'file2']|[['Value11', 'Value12', 'Value13'], ['Value21', 'Value22', 'Value23']]|
	1|15|5|['file2', 'file3']|[['Value21', 'Value22', 'Value23'], ['Value31', 'Value32', 'Value33']]|
	2|10|0|['filex']|[['TEST']]|

	DataJ:
	metadata = ['KEY1', 'KEY2', 'KEY3', 'KEY4']
	file2 = 10 ['Value21', 'XXXXX22', 'Value23']
	file3 = 10 ['Value31', 'Value32', 'Value33']
	file4 = 10 ['Value42', 'Value43', 'Value44']
	filex = 10

	>>> cfg = {'resync metadata': 'KEY2', 'resync mode KEY2': 'disable'}
	>>> doResync(doprint = False, config_dict = {'dataset': cfg}, srcFN = "dataI.dbs", modFN = "dataJ.dbs", doRename = False)
	>>> printSplit(DataSplitter.load_partitions_for_script("datamap-resync.tar"), meta = True)
	x|SIZE|SKIP|Files|Metadata|Invalid
	0|5|0|['file2']|[['Value11', 'Value12', 'Value13'], ['Value21', 'Value22', 'Value23']]|True
	1|15|5|['file2', 'file3']|[['Value21', 'Value22', 'Value23'], ['Value31', 'Value32', 'Value33']]|True
	2|10|0|['filex']||
	3|10|0|['file4']|[['Value42', 'Value43', 'Value44']]|

	>>> remove_files_testsuite(['data-new.dbs', 'data-old.dbs', 'datamap-resync.tar', 'datamap-resync-new.tar'])
	"""

run_test()
