#-#  Copyright 2014 Karlsruhe Institute of Technology
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

import logging
from grid_control import utils, ConfigError

# Change handler to notify about impossible changes
def changeImpossible(config, old_obj, cur_obj, cur_entry, obj2str):
	raise ConfigError('It is *not* possible to change "%s" from %s to %s!' %
		(cur_entry.format_opt(), obj2str(old_obj), obj2str(cur_obj)))


# Change handler to trigger re-inits
class changeInitNeeded:
	def __init__(self, option):
		self._option = option

	def __call__(self, config, old_obj, cur_obj, cur_entry, obj2str):
		log = logging.getLogger('config.onChange.%s' % self._option)
		raw_config = config.clone()
		interaction_def = raw_config.getBool('interactive', 'default', True, onChange = None)
		interaction_opt = raw_config.getBool('interactive', self._option, interaction_def, onChange = None)
		if interaction_opt:
			if utils.getUserBool('The option "%s" was changed from the old value:' % cur_entry.format_opt() +
				'\n\t%s\nto the new value:\n\t%s\nDo you want to abort?' % (obj2str(old_obj), obj2str(cur_obj)), False):
				raise ConfigError('Abort due to unintentional config change!')
			if not utils.getUserBool('A partial reinitialization (same as --reinit %s) is needed to apply this change! Do you want to continue?' % self._option, True):
				log.log(logging.INFO1, 'Using stored value %s for option %s' % (obj2str(cur_obj), cur_entry.format_opt()))
				return old_obj
		config.setState(True, 'init', self._option) # This will trigger a write of the new options
		config.setState(True, 'init', 'config')
		return cur_obj


# Validation handler to check for variables in string
def validNoVar(section, option, obj):
	return utils.checkVar(obj, '[%s] "%s" may not contain variables.' % (section, option))
