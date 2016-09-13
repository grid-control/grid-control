#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import run_test, str_dict_testsuite, try_catch
from grid_control.logging_setup import StdoutStreamHandler
from grid_control.output_processor import JobResult, OutputProcessor, TaskOutputProcessor


def display(value):
	raw = None
	for key in list(value):
		if key == JobResult.RAW:
			raw = str_dict_testsuite(value.pop(key))
		elif key in JobResult.enum_value_list:
			value[JobResult.enum2str(key)] = value.pop(key)
	print('result: ' + str_dict_testsuite(value) + '\nraw: ' + str(raw))

def modify_log(lines):
	sstr = 'Not a gzipped'
	for line in lines:
		if sstr in line:
			line = line[:line.find(sstr) + len(sstr)] + '...'
		yield line

class Test_OutputProcessor:
	"""
	>>> StdoutStreamHandler.testsuite_stream.set_modify(modify_log)

	>>> try_catch(lambda: OutputProcessor().process('.'), 'AbstractError', 'process is an abstract function')
	caught

	>>> jip = OutputProcessor.create_instance('JobInfoProcessor')
	>>> try_catch(lambda: jip.process('work.jobdb/output.job_F1'), 'JobResultError', 'does not exist')
	caught
	>>> try_catch(lambda: jip.process('work.jobdb/output.job_F2'), 'JobResultError', 'is empty')
	caught
	>>> try_catch(lambda: jip.process('work.jobdb/output.job_F3'), 'JobResultError', 'Unable to read job result file')
	caught
	>>> try_catch(lambda: jip.process('work.jobdb/output.job_F4'), 'JobResultError', 'is incomplete')
	caught

	>>> display(jip.process('work.jobdb/output.job_0'))
	result: {'EXITCODE': 0, 'JOBNUM': 0}
	raw: {'FILE': '"f9ed86240f138b0814fbb6bbce98c516  skim.root  kappa_2011-11-28_FS_442_Jet_2011A_RR_Nov08_0.root  srm://dgridsrm-fzk.gridka.de:8443/srm/managerv2?SFN=/pnfs/gridka.de/dcms/disk-only/users/stober"', 'TIME': 27417}

	>>> display(jip.process('work.jobdb/output.job_1'))
	result: {'EXITCODE': 123, 'JOBNUM': 155}
	raw: {'FILE': '"6a24adfdc1a710b23ba7e5744bb69bc7  skim.root  kappa_2011-11-28_FS_442_Jet_2011A_RR_Nov08_155.root  srm://dgridsrm-fzk.gridka.de:8443/srm/managerv2?SFN=/pnfs/gridka.de/dcms/disk-only/users/stober"', 'TIME': 32571}

	>>> display(jip.process('work.jobdb/output.job_2'))
	result: {'EXITCODE': 0, 'JOBNUM': 0}
	raw: {'FILE': '"dc5310668534534957283c8d07be51e5  output.exp  Dataset1_job_0_output.exp  dir:///home/fred/grid-control/docs/XXX"', 'FILE1': '"6e63ef4f3c68d0fe35c3ead00a990c13  output.hallo  Dataset1_job_0_output.hallo  dir:///home/fred/grid-control/docs/XXX"', 'TIME': 17}

	>>> display(jip.process('work.jobdb/output.job_3'))
	result: {'EXITCODE': 0, 'JOBNUM': 0}
	raw: {'FILE': '"cd4ce73f52940d6daffbbb82d29bcac8  histo_reconstruction.root  pfembTauTau_data_embedding_SingleMu_2011A_PR_v1_0_0_pt0_0_histo_reconstruction.root  srm://dcache-se-cms.desy.de:8443/srm/managerv2?SFN=/pnfs/desy.de/cms/tier2/store/user/aburgmei/embedding/20110420-emb2/"', 'FILE1': '"e41fa44c6595595428c79db135930bc6  histo_simulation.root  pfembTauTau_data_embedding_SingleMu_2011A_PR_v1_0_0_pt0_0_histo_simulation.root  srm://dcache-se-cms.desy.de:8443/srm/managerv2?SFN=/pnfs/desy.de/cms/tier2/store/user/aburgmei/embedding/20110420-emb2/"', 'FILE2': '"acac0de54ef6c683ce479b227ba1fc85  embedded_RECO.root  pfembTauTau_data_embedding_SingleMu_2011A_PR_v1_0_0_pt0_0_embedded_RECO.root  srm://dcache-se-cms.desy.de:8443/srm/managerv2?SFN=/pnfs/desy.de/cms/tier2/store/user/aburgmei/embedding/20110420-emb2/"', 'TIME': 496}

	>>> display(jip.process('work.jobdb/output.job_4'))
	result: {'EXITCODE': 0, 'JOBNUM': 0}
	raw: {'FILE': '"efa0f7787043f4eda6ef289e2d44f0bb  log.txt  DYJetsToLLM50_RunIIFall15MiniAODv2_PU25nsData2015v1_13TeV_MINIAOD_madgraph-pythia8/DYJetsToLLM50_RunIIFall15MiniAODv2_PU25nsData2015v1_13TeV_MINIAOD_madgraph-pythia8_job_0_log.txt  srm://dcache-se-cms.desy.de:8443/srm/managerv2?SFN=/pnfs/desy.de/cms/tier2/store/user/tmuller/artus/2016-04-22_13-29_analysis/output"', 'FILE1': '"43a198d3a7796d2c89f17238bec2d5dc  output.root  DYJetsToLLM50_RunIIFall15MiniAODv2_PU25nsData2015v1_13TeV_MINIAOD_madgraph-pythia8/DYJetsToLLM50_RunIIFall15MiniAODv2_PU25nsData2015v1_13TeV_MINIAOD_madgraph-pythia8_job_0_output.root  srm://dcache-se-cms.desy.de:8443/srm/managerv2?SFN=/pnfs/desy.de/cms/tier2/store/user/tmuller/artus/2016-04-22_13-29_analysis/output"', 'OUTPUT_FILE_0_DEST': '"DYJetsToLLM50_RunIIFall15MiniAODv2_PU25nsData2015v1_13TeV_MINIAOD_madgraph-pythia8/DYJetsToLLM50_RunIIFall15MiniAODv2_PU25nsData2015v1_13TeV_MINIAOD_madgraph-pythia8_job_0_log.txt"', 'OUTPUT_FILE_0_HASH': 'efa0f7787043f4eda6ef289e2d44f0bb', 'OUTPUT_FILE_0_LOCAL': '"log.txt"', 'OUTPUT_FILE_0_PATH': '"srm://dcache-se-cms.desy.de:8443/srm/managerv2?SFN=/pnfs/desy.de/cms/tier2/store/user/tmuller/artus/2016-04-22_13-29_analysis/output"', 'OUTPUT_FILE_0_SIZE': 61407, 'OUTPUT_FILE_1_DEST': '"DYJetsToLLM50_RunIIFall15MiniAODv2_PU25nsData2015v1_13TeV_MINIAOD_madgraph-pythia8/DYJetsToLLM50_RunIIFall15MiniAODv2_PU25nsData2015v1_13TeV_MINIAOD_madgraph-pythia8_job_0_output.root"', 'OUTPUT_FILE_1_HASH': '43a198d3a7796d2c89f17238bec2d5dc', 'OUTPUT_FILE_1_LOCAL': '"output.root"', 'OUTPUT_FILE_1_PATH': '"srm://dcache-se-cms.desy.de:8443/srm/managerv2?SFN=/pnfs/desy.de/cms/tier2/store/user/tmuller/artus/2016-04-22_13-29_analysis/output"', 'OUTPUT_FILE_1_SIZE': 1619503, 'TIME': 387, 'TIMESTAMP_CMSSW_EPILOG1_DONE': 1461325248, 'TIMESTAMP_CMSSW_EPILOG1_START': 1461324879, 'TIMESTAMP_CMSSW_STARTUP_DONE': 1461324879, 'TIMESTAMP_CMSSW_STARTUP_START': 1461324873, 'TIMESTAMP_DEPLOYMENT_DONE': 1461324868, 'TIMESTAMP_DEPLOYMENT_START': 1461324867, 'TIMESTAMP_EXECUTION_DONE': 1461325248, 'TIMESTAMP_EXECUTION_START': 1461324872, 'TIMESTAMP_SE_IN_DONE': 1461324872, 'TIMESTAMP_SE_IN_START': 1461324868, 'TIMESTAMP_SE_OUT_DONE': 1461325254, 'TIMESTAMP_SE_OUT_START': 1461325248, 'TIMESTAMP_WRAPPER_DONE': 1461325255, 'TIMESTAMP_WRAPPER_START': 1461324867}

	>>> djip = OutputProcessor.create_instance('DebugJobInfoProcessor')
	>>> djip.process('work.jobdb/output.job_0') == jip.process('work.jobdb/output.job_0')
	True
	>>> djip._display_files.append('gc.test.gz')
	>>> djip._display_files.append('gc.fail.gz')

	>>> display(djip.process('work.jobdb/output.job_1'))
	0000-00-00 00:00:00 - jobs.output:ERROR - gc.stdout
	STDOUT message1
	STDOUT message2
	STDOUT message3
	--------------------------------------------------
	0000-00-00 00:00:00 - jobs.output:ERROR - Log file does not exist: gc.stderr
	0000-00-00 00:00:00 - jobs.output:ERROR - gc.test.gz
	TEST message1
	TEST message2
	TEST message3
	--------------------------------------------------
	0000-00-00 00:00:00 - jobs.output:ERROR - Unable to display gc.fail.gz: XXError: Not a gzipped...
	result: {'EXITCODE': 123, 'JOBNUM': 155}
	raw: {'FILE': '"6a24adfdc1a710b23ba7e5744bb69bc7  skim.root  kappa_2011-11-28_FS_442_Jet_2011A_RR_Nov08_155.root  srm://dgridsrm-fzk.gridka.de:8443/srm/managerv2?SFN=/pnfs/gridka.de/dcms/disk-only/users/stober"', 'TIME': 32571}

	>>> fip = OutputProcessor.create_instance('FileInfoProcessor')
	>>> fip.process('work.jobdb/output.job_F1')
	Unable to process job information: JobResultError: Unable to process output directory 'work.jobdb/output.job_F1' - JobResultError: Job result file 'work.jobdb/output.job_F1/job.info' does not exist
	"""

class Test_TaskOutputProcessor:
	"""
	>>> try_catch(lambda: TaskOutputProcessor().process(dn = None, task = None), 'AbstractError', 'process is an abstract function')
	caught
	>>> TaskOutputProcessor.create_instance('SandboxProcessor').process(dn = None, task = None)
	True
	"""

run_test()
