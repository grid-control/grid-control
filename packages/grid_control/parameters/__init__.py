from config_param import *

from psource_base import *
from psource_basic import *
#from psource_fnlo import *
from psource_meta import *
from psource_file import *
from psource_data import *
from psource_lookup import *

from pfactory_base import *
from pfactory_modular import *
from pfactory_simple import SimpleParameterFactory

from padapter import *

ParameterFactory.moduleMap['EasyParameterFactory'] = 'pfactory_easy.EasyParameterFactory'
ParameterFactory.moduleMap['SimpleParameterFactory'] = 'pfactory_simple.SimpleParameterFactory'
