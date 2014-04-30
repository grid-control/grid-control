#-#  Copyright 2010-2013 Karlsruhe Institute of Technology
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
