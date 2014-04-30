#-#  Copyright 2010-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from wms import *
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

def findLocalWMS(clsName):
	for wms, cmd in [('OGE', 'sgepasswd'), ('PBS', 'pbs-config'), ('OGE', 'qsub'), ('LSF', 'bsub'), ('SLURM', 'job_slurm')]:
		try:
			utils.resolveInstallPath(cmd)
			return wms
		except:
			pass
	return 'PBS'

wms.WMS.moduleMapDynamic['local'] = findLocalWMS
