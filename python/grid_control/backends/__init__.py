from wms import *
from broker import *
from local_api import *
wms.WMS.moduleMap["LocalWMS"] = "local_wms.LocalWMS"
wms.WMS.moduleMap["GliteWMS"] = "glite_wms.GliteWMS"
from local_wms import *

