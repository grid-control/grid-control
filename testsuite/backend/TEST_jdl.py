#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import run_test, str_dict_testsuite, try_catch
from grid_control.backends import WMS
from grid_control.backends.jdl_writer import JDLWriter
from grid_control.backends.wms_edg import EDGJDL


jdl = JDLWriter()

def test_jdl(req_list):
	print(str_dict_testsuite(jdl.prepare(req_list)))

class Test_JDL:
	"""
	>>> test_jdl([(WMS.WALLTIME, 59)])
	{'Requirements': 'other.GlueHostNetworkAdapterOutboundIP && (other.GlueCEPolicyMaxWallClockTime >= 1)'}
	>>> test_jdl([(WMS.WALLTIME, 60)])
	{'Requirements': 'other.GlueHostNetworkAdapterOutboundIP && (other.GlueCEPolicyMaxWallClockTime >= 1)'}
	>>> test_jdl([(WMS.WALLTIME, 61)])
	{'Requirements': 'other.GlueHostNetworkAdapterOutboundIP && (other.GlueCEPolicyMaxWallClockTime >= 2)'}
	>>> test_jdl([(WMS.CPUS, 2), (WMS.MEMORY, 2000), (WMS.SOFTWARE, 'VO-cms-CMSSW_9_9_9'), (WMS.CPUTIME, 59)])
	{'CpuNumber': 2, 'Requirements': 'other.GlueHostNetworkAdapterOutboundIP && (other.GlueHostMainMemoryRAMSize >= 2000) && Member("VO-cms-CMSSW_9_9_9", other.GlueHostApplicationSoftwareRunTimeEnvironment) && (other.GlueCEPolicyMaxCPUTime >= 1)'}
	>>> test_jdl([(WMS.SITES, ['-siteA', 'siteB'])])
	{'Requirements': 'other.GlueHostNetworkAdapterOutboundIP && ( !RegExp("siteA", other.GlueCEUniqueID) && (RegExp("siteB", other.GlueCEUniqueID)) )'}
	>>> test_jdl([(WMS.STORAGE, ['SE1'])])
	{'Requirements': 'other.GlueHostNetworkAdapterOutboundIP && ( Member("SE1", other.GlueCESEBindGroupSEUniqueID) )'}
	>>> try_catch(lambda: test_jdl([(0, None)]), 'APIError', 'Unknown requirement type')
	caught

	>>> jdl.format([(WMS.WALLTIME, 59)], {'RetryCount': 2})
	['Requirements = other.GlueHostNetworkAdapterOutboundIP && (other.GlueCEPolicyMaxWallClockTime >= 1);\\n', 'RetryCount = 2;\\n']
	"""

class Test_Other:
	"""
	>>> JDLWriter().prepare([(WMS.STORAGE, ['SE1'])])
	{'Requirements': 'other.GlueHostNetworkAdapterOutboundIP && ( Member("SE1", other.GlueCESEBindGroupSEUniqueID) )'}
	>>> EDGJDL().prepare([(WMS.STORAGE, ['SE1'])])
	{'Requirements': 'other.GlueHostNetworkAdapterOutboundIP && anyMatch(other.storage.CloseSEs, (target.GlueSEUniqueID == "SE1"))'}
	"""

run_test()
