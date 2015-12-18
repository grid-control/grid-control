#-#  Copyright 2013-2015 Karlsruhe Institute of Technology
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

from grid_control.config.chandlers_base import changeImpossible, changeInitNeeded, validNoVar
from grid_control.config.config_entry import ConfigEntry, ConfigError, noDefault
from grid_control.config.config_factory import createConfigFactory
from grid_control.config.cview_base import SimpleConfigView
from grid_control.config.cview_tagged import TaggedConfigView

# At the lowest level, all config option values are represented by strings
# which are encapsulated in the ConfigEntry class, which holds access and source
# information in addition to the value.

# These config entries are stored in an ConfigContainer, which makes the
# entries accessible via "section" and "option" specifiers.

# The type parsing config interface (getInt, getBool, getClass, ...) is defined
# in the TypedConfigInterface class
