from wms import *
from wms_factory import WMSFactory
from local_wms import LocalWMS
from broker import *
from broker_basic import *

wms.WMS.dynamicLoaderPath(['grid_control.backends.local_api'])
wms.WMS.moduleMap['MultiWMS'] = 'wms_multi.MultiWMS'
wms.WMS.moduleMap['ThreadedMultiWMS'] = 'wms_thread.ThreadedMultiWMS'
wms.WMS.moduleMap['GliteWMS'] = 'glite_wms.GliteWMS'
wms.WMS.moduleMap['localhost'] = 'host.Localhost'
wms.WMS.moduleMap['Condor'] = 'condor_wms.Condor'
wms.WMS.moduleMap['JMS'] = 'slurm.JMS'
wms.WMS.moduleMap['OGE'] = 'sge.OGE'
