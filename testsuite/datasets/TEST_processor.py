#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import TestsuiteStream, create_config, run_test, try_catch
from grid_control.datasets import DataProcessor, DataProvider
from testDS import create_source, display_source
from python_compat import irange, lmap


def get_files(pat, value):
	return dict.fromkeys(lmap(lambda x: pat % x, irange(value)), 1)

class Test_DataProcessor:
	"""
	>>> config = create_config()
	>>> dp1 = DataProcessor.create_instance('null')
	>>> dp2 = DataProcessor.create_instance('empty', config, 'dataset')
	>>> DataProcessor.create_instance('MultiDataProcessor', config, [dp1, dp2], 'dataset')
	<null> => <empty:files=True, blocks=True>
	>>> blocks = DataProvider.create_instance('ListProvider', config, 'dataset', 'dataI.dbs').get_block_list_cached(show_stats=True)
	 * Dataset 'dataI.dbs':
	  contains 1 block with 4 files with 40 entries

	>>> try_catch(lambda: list(DataProcessor(config, 'dataset').process([{DataProvider.Dataset: 'test'}])), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: display_source(create_source([('/PRI/PROC/DS', {'events': 1, 'se list': 'se.site1,se.site2'})])), 'DatasetError', 'There are no dataset files specified for dataset ')
	caught
	>>> try_catch(lambda: display_source(create_source([('/PRI/PROC/DS', {'file': 1, 'metadata': '["KEY"]', 'metadata common': '["VALUE1", "VALUE2"]'})])), 'DatasetError', 'Unable to set 2 common metadata items with 1 metadata keys')
	caught
	>>> try_catch(lambda: display_source(create_source([('/PRI/PROC/DS', {'file': '1 ["VALUE"]', 'metadata': '["KEY"]', 'metadata common': '["VALUE1"]'})])), 'DatasetError', 'Unable to set 1 file metadata items with 1 metadata keys')
	caught
	>>> display_source(create_source([('/PRI/PROC/DS', {'file': 1, 'metadata': '["KEY1", "KEY2"]', 'metadata common': '["VALUE1"]'})]), show_stats=True)
	Summary: Running over 1 block with 1 file with 1 entry
	BlockName  = 0
	Dataset    = /PRI/PROC/DS
	FileList   = ["URL=file NEntries=1 Metadata={KEY1 = 'VALUE1'}"]
	Locations  = None
	Metadata   = {KEY1, KEY2}
	NEntries   = 1
	Nickname   = PRI_PROC_DS
	Provider   = ConfigDataProvider

	>>> display_source(create_source([('/PRI/PROC/DS', {'file1': 1, 'file2': 1, 'file3': 1, 'file4': 1})], {'dataset limit files': 20}))
	BlockName  = 0
	Dataset    = /PRI/PROC/DS
	FileList   = ['URL=file1 NEntries=1', 'URL=file2 NEntries=1', 'URL=file3 NEntries=1', 'URL=file4 NEntries=1']
	Locations  = None
	NEntries   = 4
	Nickname   = PRI_PROC_DS
	Provider   = ConfigDataProvider
	>>> display_source(create_source([('/PRI/PROC/DS', {'file1': 1, 'file2': 1, 'file3': 1, 'file4': 1})], {'dataset limit files': 3}))
	BlockName  = 0
	Dataset    = /PRI/PROC/DS
	FileList   = ['URL=file1 NEntries=1', 'URL=file2 NEntries=1', 'URL=file3 NEntries=1']
	Locations  = None
	NEntries   = 3
	Nickname   = PRI_PROC_DS
	Provider   = ConfigDataProvider
	>>> display_source(create_source([('/PRI/PROC/DS', {'file1': 1, 'file2': 1, 'file3': 1, 'file4': 1})], {'dataset limit files': 1}))
	BlockName  = 0
	Dataset    = /PRI/PROC/DS
	FileList   = ['URL=file1 NEntries=1']
	Locations  = None
	NEntries   = 1
	Nickname   = PRI_PROC_DS
	Provider   = ConfigDataProvider
	>>> display_source(create_source([('/PRI/PROC/DSA', get_files('file_A%d', 100)), ('/PRI/PROC/DSB', get_files('file_B%d', 150))], {'dataset limit files fraction': 0.03}))
	BlockName  = 0
	Dataset    = /PRI/PROC/DSA
	FileList   = ['URL=file_a0 NEntries=1', 'URL=file_a1 NEntries=1', 'URL=file_a10 NEntries=1']
	Locations  = None
	NEntries   = 3
	Nickname   = PRI_PROC_DSA
	Provider   = ConfigDataProvider
	====
	BlockName  = 0
	Dataset    = /PRI/PROC/DSB
	FileList   = ['URL=file_b0 NEntries=1', 'URL=file_b1 NEntries=1', 'URL=file_b10 NEntries=1', 'URL=file_b100 NEntries=1']
	Locations  = None
	NEntries   = 4
	Nickname   = PRI_PROC_DSB
	Provider   = ConfigDataProvider
	>>> display_source(create_source([('/PRI/PROC/DS', {'file1': 1, 'file2': 1, 'file3': 1, 'file4': 1})], {'dataset limit files': 0}))

	>>> display_source(create_source([('/PRI/PROC/DS', {'file1': 1, 'events': 1, 'se list': 'se.site1,se.site2'})]))
	BlockName  = 0
	Dataset    = /PRI/PROC/DS
	FileList   = ['URL=file1 NEntries=1']
	Locations  = ['se.site1', 'se.site2']
	NEntries   = 1
	Nickname   = PRI_PROC_DS
	Provider   = ConfigDataProvider

	>>> display_source(create_source([('/PRI/PROC/DS', {'file1': 1})], {'nickname full name': False}), keys=[DataProvider.Dataset, DataProvider.Nickname])
	Dataset    = /PRI/PROC/DS
	Nickname   = PRI

	>>> src = create_source([
	... ('/PRIVATE/DS#1', {'file1': 1, 'file2': 2}),
	... ('/PRIVATE/DS#2', {'file1': 1, 'file3': 3}),
	... ('/PRIVATE/DS#3', {'file4': 1}),
	... ('/PRIVATE/DS#3', {'file4': 1}),
	... ],
	... {'dataset check unique url': 'warn', 'dataset check unique block': 'warn', 'dataset processor': 'sort unique', 'dataset block sort': True})

	>>> display_source(create_source([
	... ('/PRIVATE/DS#1', {'file1': 1, 'file2': 2}),
	... ('/PRIVATE/DS#2', {'file1': 1, 'file3': 3})],
	... {'dataset check unique url': 'skip'}))
	BlockName  = 1
	Dataset    = /PRIVATE/DS
	FileList   = ['URL=file1 NEntries=1', 'URL=file2 NEntries=2']
	Locations  = None
	NEntries   = 3
	Nickname   = DS
	Provider   = ConfigDataProvider
	====
	BlockName  = 2
	Dataset    = /PRIVATE/DS
	FileList   = ['URL=file3 NEntries=3']
	Locations  = None
	NEntries   = 4
	Nickname   = DS
	Provider   = ConfigDataProvider

	>>> try_catch(lambda: display_source(create_source([
	... ('/PRIVATE/DS#1', {'file1': 1, 'file2': 2}),
	... ('/PRIVATE/DS#2', {'file1': 1, 'file3': 3})],
	... {'dataset check unique url': 'abort'})), 'DatasetError', 'Multiple occurences of URL')
	caught
	>>> try_catch(lambda: display_source(create_source([
	... ('/PRIVATE/DS#1', {'file1': 1, 'file2': 2}),
	... ('/PRIVATE/DS#1', {'file1': 1, 'file2': 2})],
	... {'dataset check unique url': 'ignore', 'dataset check unique block': 'abort'})), 'DatasetError', 'Multiple occurences of block')
	caught

	>>> display_source(create_source([
	... ('/PRIVATE/DS#1', {'file1': 1, 'file2': 2}),
	... ('/PRIVATE/DS#1', {'file1': 1, 'file2': 2})],
	... {'dataset check unique url': 'ignore', 'dataset check unique block': 'skip'}))
	BlockName  = 1
	Dataset    = /PRIVATE/DS
	FileList   = ['URL=file1 NEntries=1', 'URL=file2 NEntries=2']
	Locations  = None
	NEntries   = 3
	Nickname   = DS
	Provider   = ConfigDataProvider

	>>> src.get_dataset_name_list()
	 * Dataset '/PRIVATE/DS#1':
	  contains 1 block with 2 files with 3 entries
	 * Dataset '/PRIVATE/DS#2':
	  contains 1 block with 2 files with 4 entries
	 * Dataset '/PRIVATE/DS#3':
	  contains 1 block with 1 file with 1 entry
	 * Dataset '/PRIVATE/DS#3':
	  contains 1 block with 1 file with 1 entry
	['/PRIVATE/DS']

	>>> display_source(src)
	Multiple occurences of URL: 'file1'! (This check can be configured with 'dataset check unique url')
	Multiple occurences of URL: 'file4'! (This check can be configured with 'dataset check unique url')
	Multiple occurences of block: "/PRIVATE/DS#3"! (This check can be configured with 'dataset check unique block')
	BlockName  = 1
	Dataset    = /PRIVATE/DS
	FileList   = ['URL=file1 NEntries=1', 'URL=file2 NEntries=2']
	Locations  = None
	NEntries   = 3
	Nickname   = DS
	Provider   = ConfigDataProvider
	====
	BlockName  = 2
	Dataset    = /PRIVATE/DS
	FileList   = ['URL=file1 NEntries=1', 'URL=file3 NEntries=3']
	Locations  = None
	NEntries   = 4
	Nickname   = DS
	Provider   = ConfigDataProvider
	====
	BlockName  = 3
	Dataset    = /PRIVATE/DS
	FileList   = ['URL=file4 NEntries=1']
	Locations  = None
	NEntries   = 1
	Nickname   = DS
	Provider   = ConfigDataProvider
	====
	BlockName  = 3
	Dataset    = /PRIVATE/DS
	FileList   = ['URL=file4 NEntries=1']
	Locations  = None
	NEntries   = 1
	Nickname   = DS
	Provider   = ConfigDataProvider

	>>> display_source(create_source([
	... ('/PRIVATE/DS#1', {'file1': 1, 'nickname': 'nick1'}),
	... ('/PRIVATE/DS#2', {'file2': 1, 'nickname': 'nick2'}),
	... ],
	... {'dataset check nickname consistency': 'warn'}),
	... keys=[DataProvider.BlockName, DataProvider.Nickname])
	Different blocks of dataset "/PRIVATE/DS" have different nicknames: ["nick1", "nick2"]
	BlockName  = 1
	Nickname   = nick1
	====
	BlockName  = 2
	Nickname   = nick2

	>>> try_catch(lambda: display_source(create_source([
	... ('/PRIVATE/DS#1', {'file1': 1, 'nickname': 'nick1'}),
	... ('/PRIVATE/DS#2', {'file2': 1, 'nickname': 'nick2'}),
	... ],
	... {'dataset check nickname consistency': 'abort'}),
	... keys=[DataProvider.BlockName, DataProvider.Nickname]), 'DatasetError', 'have different nicknames')
	caught

	>>> display_source(create_source([
	... ('/PRIVATE/DS1', {'file1': 1, 'nickname': 'nick'}),
	... ('/PRIVATE/DS2', {'file2': 1, 'nickname': 'nick'}),
	... ],
	... {'dataset check nickname collision': 'warn'}),
	... keys=[DataProvider.Dataset, DataProvider.Nickname])
	Multiple datasets use the same nickname "nick": ["/PRIVATE/DS1", "/PRIVATE/DS2"]
	Dataset    = /PRIVATE/DS1
	Nickname   = nick
	====
	Dataset    = /PRIVATE/DS2
	Nickname   = nick

	>>> src = create_source([
	... ('/PRIVATE/DS1', {'file1': 1, 'nickname': 'nick'}),
	... ('/PRIVATE/DS2', {'file2': 1, 'nickname': 'nick'}),
	... ],
	... {'dataset check nickname collision': 'abort'})
	>>> try_catch(lambda: display_source(src,
	... keys=[DataProvider.Dataset, DataProvider.Nickname]), 'DatasetError', 'Multiple datasets use the same nickname')
	caught

	>>> src = create_source([
	... ('/PRIVATE/DSA', {'file1': 1, 'file2': 2, 'file3': 2, 'file4': 1, 'file5': 3, 'file6': 2}),
	... ('/PRIVATE/DSB', {'filex': 2, 'filey': 3, 'filez': 1}),
	... ],
	... {'dataset processor': 'estimate', 'target partitions': 3})
	>>> tmp = src.get_block_list_cached(False)
	>>> src.testsuite_config.change_view(view_class='SimpleConfigView', set_sections=None).write(TestsuiteStream(), print_default=False)
	[dataset]
	dataset processor = estimate
	target partitions = 3
	-----
	[dataset!]
	events per job = 6
	files per job = 3
	-----
	[datasource /private/dsa]
	file1 = 1
	file2 = 2
	file3 = 2
	file4 = 1
	file5 = 3
	file6 = 2
	-----
	[datasource /private/dsb]
	filex = 2
	filey = 3
	filez = 1
	-----

	>>> src = create_source([
	... ('/PRIVATE/DSA', {'file1': 1, 'file2': 2, 'file3': 2, 'file4': 1, 'file5': 3, 'file6': 2}),
	... ('/PRIVATE/DSB', {'filex': 2, 'filey': 3, 'filez': 1}),
	... ],
	... {'dataset processor': 'estimate', 'target partitions per nickname': 3})
	>>> tmp = src.get_block_list_cached(False)
	>>> src.testsuite_config.change_view(view_class='SimpleConfigView', set_sections=None).write(TestsuiteStream(), print_default=False)
	[dataset]
	dataset processor = estimate
	target partitions per nickname = 3
	-----
	[dataset!]
	events per job =
	  DSA => 4
	  DSB => 2
	files per job =
	  DSA => 2
	  DSB => 1
	-----
	[datasource /private/dsa]
	file1 = 1
	file2 = 2
	file3 = 2
	file4 = 1
	file5 = 3
	file6 = 2
	-----
	[datasource /private/dsb]
	filex = 2
	filey = 3
	filez = 1
	-----

	>>> tmp = create_source([
	... ('/PRIVATE/DSA', {'file1': 1, 'file2': 2, 'file3': 2, 'file4': 1, 'file5': 3, 'file6': 2}),
	... ],
	... {'dataset processor': 'estimate'}).get_block_list_cached(False)
	"""

run_test()
