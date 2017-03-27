#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import os
from testfwk import TestsuiteStream, create_config, run_test
from grid_control.datasets import DataProvider
from web_replay import replay_start


os.environ['X509_USER_PROXY'] = 'web_replay.py'

req_return = {}
def request_modifier(key, value): # test DAS request loop
	if req_return.get(key):
		return value
	req_return[key] = True
	return 'x'*32
replay_start('cms_web_responses', request_modifier)

def getDAS(ds, cfg={}):
	cfg['dataset processor +'] = 'sort'
	cfg['dataset sort'] = 'True'
	cfg['dataset block sort'] = 'True'
	cfg['dataset files sort'] = 'True'
	cfg['dataset location sort'] = 'True'
	config = create_config(config_dict={'dataset': cfg})
	ds = DataProvider.create_instance('DASProvider', config, 'dataset', ds)
	blocks = ds.get_block_list_cached(show_stats=False)
	stream = TestsuiteStream()
	for block in DataProvider.save_to_stream(stream, ds.get_block_list_cached(show_stats=False), strip_metadata=False):
		pass

class Test_DBS3:
	"""
	>>> getDAS('/SingleMuon/Run2015E-PromptReco-v1/MINIAOD#17df3a8a-8e3d-11e5-9687-001e67abf228')
	[/SingleMuon/Run2015E-PromptReco-v1/MINIAOD#17df3a8a-8e3d-11e5-9687-001e67abf228]
	nickname = SingleMuon_Run2015E-PromptReco-v1_MINIAOD
	events = 267049
	se list = cmsdcadisk01.fnal.gov,srm-cms.jinr-t1.ru,srm.unl.edu,storage01.lcg.cscs.ch
	prefix = /store/data/Run2015E/SingleMuon/MINIAOD/PromptReco-v1/000/261
	397/00000/A8DA65A7-3B8E-E511-8C33-02163E013745.root = 56118
	397/00000/CAAB76A3-3B8E-E511-9482-02163E0134F1.root = 95082
	398/00000/4C3D439E-3D8E-E511-A01D-02163E0119C2.root = 16788
	399/00000/00AC9572-3F8E-E511-AE17-02163E014602.root = 78409
	401/00000/3E4C8AD5-408E-E511-B6E5-02163E0134DB.root = 13073
	402/00000/44A8DF36-418E-E511-AECD-02163E013522.root = 7576
	422/00000/A0C10611-438E-E511-93BE-02163E013522.root = 3
	"""

run_test()
