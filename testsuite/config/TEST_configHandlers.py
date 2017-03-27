#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, function_factory, run_test, testfwk_remove_files, try_catch
from grid_control.config import NoVarCheck, TriggerInit, TriggerResync
from grid_control.utils.user_interface import UserInputInterface


complex_string = '--disable-repo-versions --log-level info --log-files /net/scratch_cms/institut_3b/tmuller/artus/2016-04-13_10-51_analysis/output/${DATASETNICK}/${DATASETNICK}_job_${MY_JOBID}_log.txt --print-envvars ROOTSYS CMSSW_BASE DATASETNICK FILE_NAMES LD_LIBRARY_PATH -c artus_07761e3ef891bb2850d05339376aea03.json --nick $DATASETNICK -i $FILE_NAMES --ld-library-paths /.automount/net_rw/net__scratch_cms/institut_3b/tmuller/cms/htt/analysis/CMSSW_7_1_5/src/HiggsAnalysis/HiggsToTauTau/CombineHarvester/CombineTools/lib/ /.automount/net_rw/net__scratch_cms/institut_3b/tmuller/cms/htt/analysis/CMSSW_7_1_5/src/Kappa/lib/ /.automount/net_rw/net__scratch_cms/institut_3b/tmuller/cms/htt/analysis/CMSSW_7_1_5/src/KappaTools/lib/'

class Test_ConfigView:
	"""
	>>> config = create_config()
	>>> valid = NoVarCheck(config)
	>>> valid.check('TEST')
	False
	>>> valid.check('TEST @XYZ@')
	True
	>>> valid.check('TEST @XYZ@\\nTEST @ABC@')
	True
	>>> valid.check('TEST @XYZ\\nTEST @ABC')
	False
	>>> valid.check('TEST @XYZ\\nTEST @ABC __XYZ__')
	True
	>>> valid.check('TEST @XYZ @XXXX@\\nTEST @ABC __XYZ__')
	True

	>>> config = create_config(config_dict={'global': {'variable markers': '@'}})
	>>> valid1 = NoVarCheck(config)
	>>> valid1.check(complex_string)
	False
	>>> config = create_config(config_dict={'global': {'variable markers': '@ __'}})
	>>> valid1 = NoVarCheck(config)
	>>> valid1.check(complex_string)
	True
	"""

def mk_cfg(value):
	return create_config(config_dict={'test': {'key': value}, 'global': {'workdir': '.'}})

def store_cfg(config):
	fp = open('work.conf', 'w')
	config.write(fp)
	fp.close()

class Test_ConfigValidator:
	"""
	>>> testfwk_remove_files(['work.conf'])

	>>> config_tmp = create_config()
	>>> mk_cfg('value').get('key', on_valid=NoVarCheck(config_tmp))
	'value'
	>>> try_catch(lambda: mk_cfg('__value__').get('key', on_valid=NoVarCheck(config_tmp)), 'ConfigError', 'may not contain variables')
	caught
	>>> try_catch(lambda: mk_cfg('@value@').get('key', on_valid=NoVarCheck(config_tmp)), 'ConfigError', 'may not contain variables')
	caught

	>>> config_tmp = create_config(config_dict={'global': {'variable markers': '!'}})
	>>> try_catch(lambda: mk_cfg('value').get('key', on_valid=NoVarCheck(config_tmp)), 'ConfigError', 'is not supported')
	caught

	>>> config_tmp = create_config(config_dict={'global': {'variable markers': '@'}})
	>>> mk_cfg('value').get('key', on_valid=NoVarCheck(config_tmp))
	'value'
	>>> mk_cfg('__value__').get('key', on_valid=NoVarCheck(config_tmp))
	'__value__'
	>>> try_catch(lambda: mk_cfg('@value@').get('key', on_valid=NoVarCheck(config_tmp)), 'ConfigError', 'may not contain variables')
	caught

	>>> mk_cfg('@val\\nue@').get('key', on_valid=NoVarCheck(config_tmp))
	'@val\\nue@'
	>>> try_catch(lambda: mk_cfg('@val\\nue@ @').get('key', on_valid=NoVarCheck(config_tmp)), 'ConfigError', 'may not contain variables')
	caught
	"""

def test_cin(test_fun, answer_fun):
	config = mk_cfg('value1')
	state_init = config.get_state('init')
	state_parameters = config.get_state('init', detail='parameters')
	state_config = config.get_state('init', detail='config')
	print(('init', state_init, 'parameters', state_parameters, 'config', state_config))
	UserInputInterface.prompt_bool = answer_fun
	print(test_fun(config))
	state_init = config.get_state('init')
	state_parameters = config.get_state('init', detail='parameters')
	state_config = config.get_state('init', detail='config')
	print(('init', state_init, 'parameters', state_parameters, 'config', state_config))

class Test_ConfigChangeHandler:
	"""
	>>> testfwk_remove_files(['work.conf'])

	>>> config = mk_cfg('value')
	>>> store_cfg(config)
	>>> config.get('key')
	'value'

	>>> try_catch(lambda: mk_cfg('x' * 10).get('key'), 'ConfigError', 'It is *not* possible to change')
	caught
	>>> try_catch(lambda: mk_cfg('x' * 90).get('key'), 'ConfigError', 'It is *not* possible to change')
	caught

	>>> config = mk_cfg('value1')
	>>> config.get('key', on_change=None)
	'value1'

	>>> (config.get_state('resync'), config.get_state('resync', detail='parameters'))
	(False, False)
	>>> config.get('key', on_change=TriggerResync(['parameters']))
	The config option '[test] key' was changed
	Triggering resync of parameters
	The configuration was changed - triggering storage of new config options
	'value1'
	>>> (config.get_state('resync'), config.get_state('resync', detail='parameters'))
	(False, True)
	>>> config.get('key', on_change=TriggerResync(['parameters']))
	The config option '[test] key' was changed
	'value1'
	>>> (config.get_state('resync'), config.get_state('resync', detail='parameters'))
	(False, True)

	>>> test_cin(
	... lambda config: try_catch(lambda: config.get('key', on_change=TriggerInit('parameters')), 'ConfigError', 'Abort due to unintentional config change'),
	... function_factory(True, display=False))
	('init', False, 'parameters', False, 'config', False)
	caught
	None
	('init', False, 'parameters', False, 'config', False)

	>>> test_cin(
	... lambda config: config.get('key', on_change=TriggerInit('parameters')),
	... function_factory(False, True, display=False))
	('init', False, 'parameters', False, 'config', False)
	value1
	('init', False, 'parameters', True, 'config', True)

	>>> test_cin(
	... lambda config: config.get('key', on_change=TriggerInit('parameters')),
	... function_factory(False, False, display=False))
	('init', False, 'parameters', False, 'config', False)
	value
	('init', False, 'parameters', False, 'config', False)
	"""

run_test(exit_fun=lambda: testfwk_remove_files(['work.conf']))
