#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, function_factory, run_test, testfwk_remove_files, try_catch
from grid_control.utils.user_interface import UserInputInterface
from hpfwk import Plugin
from testDS import create_scan


class Test_InfoScanner:
	"""
	>>> InfoScanner = Plugin.get_class('InfoScanner')
	>>> try_catch(lambda: InfoScanner(create_config(), 'dataset')._iter_datasource_items(*([None]*5)), 'AbstractError', 'is an abstract function')
	caught
	"""

class Test_ScanProvider:
	"""
	>>> create_scan('')
	BlockName  = 747fbe96
	Dataset    = /PRIVATE/Dataset_866f8d4511b34fccc92d2600378ee899
	FileList   = ['URL=./test_Zee_0.root NEntries=-1', 'URL=./test_Zmm_0.root NEntries=-1', 'URL=./test_Zmm_1.root NEntries=-1', 'URL=./test_gg.root NEntries=-1']
	Locations  = None
	NEntries   = -4

	>>> create_scan('file://')
	BlockName  = 24451d91
	Dataset    = /PRIVATE/Dataset_bdb8a9032623a7ffbd33aaf34fec15b4
	FileList   = ['URL=file://test_Zee_0.root NEntries=-1', 'URL=file://test_Zmm_0.root NEntries=-1', 'URL=file://test_Zmm_1.root NEntries=-1', 'URL=file://test_gg.root NEntries=-1']
	Locations  = None
	NEntries   = -4

	>>> create_scan('./*.root', settings={'source recurse': True})
	BlockName  = 0208b250
	Dataset    = /PRIVATE/Dataset_59e20836f0922296b80565f908524fa8
	FileList   = ['URL=./test/test_tt_0.root NEntries=-1', 'URL=./test/test_tt_1.root NEntries=-1', 'URL=./test_Zee_0.root NEntries=-1', 'URL=./test_Zmm_0.root NEntries=-1', 'URL=./test_Zmm_1.root NEntries=-1', 'URL=./test_gg.root NEntries=-1']
	Locations  = None
	NEntries   = -6

	>>> try_catch(lambda: create_scan('file://*.root', settings={'source recurse': True}), 'DatasetError', 'Recursion is not supported')
	caught

	>>> create_scan('*.root', settings={'filename prefix': 'xrootd://'})
	BlockName  = 97e1ba01
	Dataset    = /PRIVATE/Dataset_286a5637661ead35db68ba8c0ba1d334
	FileList   = ['URL=xrootd://./test_Zee_0.root NEntries=-1', 'URL=xrootd://./test_Zmm_0.root NEntries=-1', 'URL=xrootd://./test_Zmm_1.root NEntries=-1', 'URL=xrootd://./test_gg.root NEntries=-1']
	Locations  = None
	NEntries   = -4

	>>> create_scan('', settings={'delimeter match': '_:2', 'delimeter dataset key': '_:1:2'})
	BlockName  = dcbb81ca
	Dataset    = /PRIVATE/Dataset_a40220d55130fe8c6d25d8d87bdab3f9
	FileList   = ['URL=./test_Zmm_0.root NEntries=-1', 'URL=./test_Zmm_1.root NEntries=-1']
	Locations  = None
	NEntries   = -2
	====
	BlockName  = 05b97743
	Dataset    = /PRIVATE/Dataset_b67d992fd0937b1f13a8dcfa016d5913
	FileList   = ['URL=./test_Zee_0.root NEntries=-1']
	Locations  = None
	NEntries   = -1

	>>> create_scan('', settings={'delimeter match': '_:2', 'delimeter dataset key': '_:1:2'})
	BlockName  = dcbb81ca
	Dataset    = /PRIVATE/Dataset_a40220d55130fe8c6d25d8d87bdab3f9
	FileList   = ['URL=./test_Zmm_0.root NEntries=-1', 'URL=./test_Zmm_1.root NEntries=-1']
	Locations  = None
	NEntries   = -2
	====
	BlockName  = 05b97743
	Dataset    = /PRIVATE/Dataset_b67d992fd0937b1f13a8dcfa016d5913
	FileList   = ['URL=./test_Zee_0.root NEntries=-1']
	Locations  = None
	NEntries   = -1

	>>> try_catch(lambda: create_scan('', settings={'delimeter match': '_:2', 'delimeter dataset key': '_:-1:', 'delimeter dataset modifier': 'value.split(".")[2', 'events key': 'DELIMETER_B', 'events per key value': '10'}), 'ConfigError', 'Unable to parse delimeter modifier')
	caught

	>>> try_catch(lambda: create_scan('', settings={'delimeter match': '_:2', 'delimeter dataset key': '_:-1:', 'delimeter dataset modifier': 'value.split(".")[2]', 'events key': 'DELIMETER_B', 'events per key value': '10'}), 'DatasetError', 'Unable to modifiy')
	caught

	>>> create_scan('', settings={'delimeter match': '_:2', 'delimeter dataset key': '_:-1:', 'delimeter dataset modifier': 'value.split(".")[0]', 'events key': 'DELIMETER_DS', 'events per key value': '10'})
	BlockName  = a633f97e
	Dataset    = /PRIVATE/Dataset_405f437c0cafa56f1e60e599e6628dd1
	FileList   = ['URL=./test_Zmm_1.root NEntries=10']
	Locations  = None
	NEntries   = 10
	====
	BlockName  = 0bee63df
	Dataset    = /PRIVATE/Dataset_ffb7093e61fe3e9fe9a86d7a2b74d881
	FileList   = ['URL=./test_Zee_0.root NEntries=1', 'URL=./test_Zmm_0.root NEntries=1']
	Locations  = None
	NEntries   = 2

	>>> create_scan('', settings={'dataset key select': 'a40220d55130fe8c6d25d8d87bdab3f9', 'dataset name pattern': '/PRIVATE/Dataset_@DELIMETER_DS@', 'delimeter match': '_:2', 'delimeter dataset key': '_:-2:-1'})
	BlockName  = dcbb81ca
	Dataset    = /PRIVATE/Dataset_Zmm
	FileList   = ['URL=./test_Zmm_0.root NEntries=-1', 'URL=./test_Zmm_1.root NEntries=-1']
	Locations  = None
	NEntries   = -2

	>>> create_scan('', settings={'delimeter match': '_:2'})
	BlockName  = 747fbe96
	Dataset    = /PRIVATE/Dataset_866f8d4511b34fccc92d2600378ee899
	FileList   = ['URL=./test_Zee_0.root NEntries=-1', 'URL=./test_Zmm_0.root NEntries=-1', 'URL=./test_Zmm_1.root NEntries=-1']
	Locations  = None
	NEntries   = -3

	>>> create_scan('', settings={'events command': 'cat'})
	BlockName  = 747fbe96
	Dataset    = /PRIVATE/Dataset_866f8d4511b34fccc92d2600378ee899
	FileList   = ['URL=./test_Zee_0.root NEntries=10', 'URL=./test_Zmm_0.root NEntries=10', 'URL=./test_Zmm_1.root NEntries=20', 'URL=./test_gg.root NEntries=20']
	Locations  = None
	NEntries   = 60

	>>> create_scan('', settings={'events command': 'md5sum'})
	BlockName  = 747fbe96
	Dataset    = /PRIVATE/Dataset_866f8d4511b34fccc92d2600378ee899
	FileList   = ['URL=./test_Zee_0.root NEntries=-1', 'URL=./test_Zmm_0.root NEntries=-1', 'URL=./test_Zmm_1.root NEntries=-1', 'URL=./test_gg.root NEntries=-1']
	Locations  = None
	NEntries   = -4

	>>> create_scan('', settings={'filename filter': '*.dbs -dataA* -data_* -data-* -short*', 'filename filter matcher': 'blackwhite', 'filename filter matcher mode': 'shell'})
	BlockName  = 747fbe96
	Dataset    = /PRIVATE/Dataset_866f8d4511b34fccc92d2600378ee899
	FileList   = ['URL=./dataB.dbs NEntries=-1', 'URL=./dataC.dbs NEntries=-1', 'URL=./dataD.dbs NEntries=-1', 'URL=./dataE.dbs NEntries=-1', 'URL=./dataF.dbs NEntries=-1', 'URL=./dataG.dbs NEntries=-1', 'URL=./dataH.dbs NEntries=-1', 'URL=./dataI.dbs NEntries=-1', 'URL=./dataJ.dbs NEntries=-1', 'URL=./dataK.dbs NEntries=-1', 'URL=./dataL.dbs NEntries=-1', 'URL=./dataRL.dbs NEntries=-1']
	Locations  = None
	NEntries   = -12

	>>> create_scan('dataC.dbs', settings={'filename filter': '*'})
	BlockName  = e646cf46
	Dataset    = /PRIVATE/Dataset_be6020afca047f6c6c3b506b04c62944
	FileList   = ['URL=/path/to//file1 NEntries=5', 'URL=/path/to//file2 NEntries=15']
	Locations  = ['SE1', 'SE2']
	NEntries   = 20

	>>> create_scan('', scanners='FilesFromLS MatchOnFilename MatchDelimeter ParentLookup', settings={'parent source': 'parents.info', 'parent keys': 'DELIMETER_DS', 'delimeter match': '_:2', 'delimeter dataset key': '_:-2:-1'}, restrict_keys=False)
	BlockName  = 41f37bf4
	Dataset    = /PRIVATE/Dataset_a40220d55130fe8c6d25d8d87bdab3f9
	FileList   = ["URL=./test_Zmm_0.root NEntries=-1 Metadata={BLOCK_KEY = '41f37bf435e326c4689455c6c9b34100', DELIMETER_DS = 'Zmm', DS_KEY = 'a40220d55130fe8c6d25d8d87bdab3f9', GC_SOURCE_DIR = '.', PARENT_PATH = ['/PRIVATE/Zmm']}", "URL=./test_Zmm_1.root NEntries=-1 Metadata={BLOCK_KEY = '41f37bf435e326c4689455c6c9b34100', DELIMETER_DS = 'Zmm', DS_KEY = 'a40220d55130fe8c6d25d8d87bdab3f9', GC_SOURCE_DIR = '.', PARENT_PATH = ['/PRIVATE/Zmm']}"]
	Locations  = None
	Metadata   = {BLOCK_KEY, DELIMETER_DS, DS_KEY, GC_SOURCE_DIR, PARENT_PATH}
	NEntries   = -2
	Nickname   = Dataset_a40220d55130fe8c6d25d8d87bdab3f9
	Provider   = ScanProvider
	====
	BlockName  = f9696770
	Dataset    = /PRIVATE/Dataset_b67d992fd0937b1f13a8dcfa016d5913
	FileList   = ["URL=./test_Zee_0.root NEntries=-1 Metadata={BLOCK_KEY = 'f969677035fe8232d5a4a08b8e8f505a', DELIMETER_DS = 'Zee', DS_KEY = 'b67d992fd0937b1f13a8dcfa016d5913', GC_SOURCE_DIR = '.', PARENT_PATH = ['/PRIVATE/Zee']}"]
	Locations  = None
	Metadata   = {BLOCK_KEY, DELIMETER_DS, DS_KEY, GC_SOURCE_DIR, PARENT_PATH}
	NEntries   = -1
	Nickname   = Dataset_b67d992fd0937b1f13a8dcfa016d5913
	Provider   = ScanProvider

	>>> create_scan('', settings={'dataset name pattern': '/PRIVATE/Dataset_@DELIMETER_DS@', 'delimeter dataset key': '_:1:2', 'delimeter dataset modifier': 'value.split(".")[0]'})
	BlockName  = 05b97743
	Dataset    = /PRIVATE/Dataset_Zee
	FileList   = ['URL=./test_Zee_0.root NEntries=-1']
	Locations  = None
	NEntries   = -1
	====
	BlockName  = dcbb81ca
	Dataset    = /PRIVATE/Dataset_Zmm
	FileList   = ['URL=./test_Zmm_0.root NEntries=-1', 'URL=./test_Zmm_1.root NEntries=-1']
	Locations  = None
	NEntries   = -2
	====
	BlockName  = 02cffd2d
	Dataset    = /PRIVATE/Dataset_gg
	FileList   = ['URL=./test_gg.root NEntries=-1']
	Locations  = None
	NEntries   = -1

	>>> UserInputInterface.prompt_bool = function_factory(True, display_first=False)
	>>> create_scan('', settings={'dataset name pattern': '/PRIVATE', 'delimeter dataset key': '_:1:2', 'delimeter dataset modifier': 'value.split(".")[0]'})
	Multiple dataset keys are mapped to the name '/PRIVATE'!
	  Candidate #1 with key 'a40220d55130fe8c6d25d8d87bdab3f9':
	    DELIMETER_DS = Zmm
	  Candidate #2 with key 'a707561b2da6c70aa1cdd5516a5fb7b4':
	    DELIMETER_DS = gg
	  Candidate #3 with key 'b67d992fd0937b1f13a8dcfa016d5913':
	    DELIMETER_DS = Zee
	(('Do you want to continue?', False), {})
	BlockName  = 02cffd2d
	Dataset    = /PRIVATE
	FileList   = ['URL=./test_gg.root NEntries=-1']
	Locations  = None
	NEntries   = -1
	====
	BlockName  = 05b97743
	Dataset    = /PRIVATE
	FileList   = ['URL=./test_Zee_0.root NEntries=-1']
	Locations  = None
	NEntries   = -1
	====
	BlockName  = dcbb81ca
	Dataset    = /PRIVATE
	FileList   = ['URL=./test_Zmm_0.root NEntries=-1', 'URL=./test_Zmm_1.root NEntries=-1']
	Locations  = None
	NEntries   = -2
	"""

class Test_GCProvider:
	"""
	>>> try_catch(lambda: create_scan('test', provider='GCProvider'), 'DatasetError', 'Unable to find task output directory')
	caught
	>>> try_catch(lambda: create_scan('work.test', provider='GCProvider'), 'DatasetError', 'Unable to read file stageout information')
	caught

	>>> create_scan('work.test', provider='GCProvider', settings={'source job selector': '1-14'})
	BlockName  = 4d3b1c3e
	Dataset    = /PRIVATE/QCD_Pt_30_TuneZ2star_8TeV_pythia6_cff_py_GEN
	FileList   = ['URL=dir:///tmp/CMS/QCD_2_2/MC_2.root NEntries=-1', 'URL=dir:///tmp/CMS/QCD_2_2/MC_8.root NEntries=-1']
	Locations  = None
	NEntries   = -2
	"""

testfwk_remove_files(['data-ori.dbs', 'data.dbs'])
run_test()
