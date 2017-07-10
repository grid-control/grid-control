# | Copyright 2013-2017 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

from grid_control.config.chandlers_base import NoVarCheck, TriggerInit, TriggerResync
from grid_control.config.config_entry import ConfigError, join_config_locations
from grid_control.config.config_factory import create_config
from grid_control.config.matcher_base import ListOrder, Matcher


__all__ = ['ConfigError', 'create_config', 'join_config_locations', 'ListOrder',
	'Matcher', 'NoVarCheck', 'TriggerInit', 'TriggerResync']

# At the lowest level, all config option values are represented by strings
# which are encapsulated in the ConfigEntry class, which holds access and source
# information in addition to the value.

# These config entries are stored in an ConfigContainer, which makes the
# entries accessible via "section" and "option" specifiers.

# The type parsing config interface (get_int, get_bool, get_plugin, ...) is defined
# in the TypedConfigInterface class
