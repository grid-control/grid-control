from wms import *
from wms_factory import WMSFactory
from local_wms import LocalWMS
from broker import *
from broker_basic import *

wms.WMS.registerObject(searchPath = ['grid_control.backends.local_api'])
wms.WMS.moduleMap['MultiWMS'] = 'wms_multi.MultiWMS'
wms.WMS.moduleMap['ThreadedMultiWMS'] = 'wms_thread.ThreadedMultiWMS'
wms.WMS.moduleMap['GliteWMS'] = 'glite_wms.GliteWMS'
wms.WMS.moduleMap['localhost'] = 'host.Localhost'
wms.WMS.moduleMap['Condor'] = 'condor_wms.Condor'
wms.WMS.moduleMap['JMS'] = 'slurm.JMS'
wms.WMS.moduleMap['OGE'] = 'sge.OGE'
wms.WMS.moduleMap['grid'] = 'GliteWMS'
wms.WMS.moduleMap['inactive'] = 'InactiveWMS'

def findLocalWMS(self, clsName):
	for wms, cmd in [('OGE', 'sgepasswd'), ('PBS', 'pbs-config'), ('OGE', 'qsub'), ('LSF', 'bsub'), ('SLURM', 'job_slurm')]:
		try:
			utils.resolveInstallPath(cmd)
			return wms
		except:
			pass
	return 'PBS'

wms.WMS.moduleMapDynamic['local'] = findLocalWMS
