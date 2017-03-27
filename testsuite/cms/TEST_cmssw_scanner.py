#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import run_test
from testDS import create_scan


class Test_ScanProvider:
	"""
	>>> create_scan('work', provider='GCProvider', settings={'include parent infos': 'True', 'include config infos': 'True'})
	BlockName  = 0776e221
	Dataset    = /PRIVATE/output
	FileList   = ['URL=/tmp/CMS/Run1_job_3_output.root NEntries=-1']
	Locations  = ['localhost']
	NEntries   = -1
	====
	BlockName  = 101ed0e9
	Dataset    = /PRIVATE/output
	FileList   = ['URL=/store/user/stober/TEST/QCD_0.5_0.5_RunX_USER_job_0_output.root NEntries=-1']
	Locations  = ['dcache-se-cms.desy.de']
	NEntries   = -1
	====
	BlockName  = 5e79a612
	Dataset    = /PRIVATE/output
	FileList   = ['URL=protocol:/tmp/CMS/Run1_job_4_output.root NEntries=-1']
	Locations  = None
	NEntries   = -1
	====
	BlockName  = 9151fe90
	Dataset    = /PRIVATE/output
	FileList   = ['URL=/store/QCD_2_2_RunX_USER_job_2_output.root NEntries=-1', 'URL=/store/QCD_2_2_RunX_USER_job_5_output.root NEntries=-1']
	Locations  = ['castor-desy.de']
	NEntries   = -2
	====
	BlockName  = c93002bf
	Dataset    = /PRIVATE/output
	FileList   = ['URL=/store/relval/QCD_1_1_RunX_USER_job_1_output.root NEntries=-1']
	Locations  = ['caf.cern.ch']
	NEntries   = -1

	>>> create_scan('work', scanners='OutputDirsFromWork ObjectsFromCMSSW JobInfoFromOutputDir FilesFromJobInfo MatchOnFilename MatchDelimeter MetadataFromCMSSW SEListFromPath LFNFromPath DetermineEvents AddFilePrefix FilterEDMFiles', provider='GCProvider')
	BlockName  = 101ed0e9
	Dataset    = /PRIVATE/output
	FileList   = ['URL=/store/user/stober/TEST/QCD_0.5_0.5_RunX_USER_job_0_output.root NEntries=-1']
	Locations  = ['dcache-se-cms.desy.de']
	NEntries   = -1
	====
	BlockName  = 9151fe90
	Dataset    = /PRIVATE/output
	FileList   = ['URL=/store/QCD_2_2_RunX_USER_job_2_output.root NEntries=-1', 'URL=/store/QCD_2_2_RunX_USER_job_5_output.root NEntries=-1']
	Locations  = ['castor-desy.de']
	NEntries   = -2
	====
	BlockName  = c93002bf
	Dataset    = /PRIVATE/output
	FileList   = ['URL=/store/relval/QCD_1_1_RunX_USER_job_1_output.root NEntries=-1']
	Locations  = ['caf.cern.ch']
	NEntries   = -1
	"""

run_test()
