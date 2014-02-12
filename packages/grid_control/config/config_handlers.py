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
		raw_config = config.getScoped(None)
		interaction_def = raw_config.getBool('interactive', 'default', True, onChange = None)
		interaction_opt = raw_config.getBool('interactive', self._option, interaction_def, onChange = None)
		if interaction_opt:
			if utils.getUserBool('The option "%s" was changed from the old value:' % cur_entry.format_opt() +
				'\n\t%s\nto the new value:\n\t%s\nDo you want to abort?' % (obj2str(old_obj), obj2str(cur_obj)), False):
				raise ConfigError('Abort due to unintentional config change!')
			if not utils.getUserBool('A partial reinitialization (same as --reinit %s) is needed to apply this change! Do you want to continue?' % self._option, True):
				log.log(logging.INFO1, 'Using stored value %s for option %s' % (obj2str(cur_obj), cur_entry.format_opt()))
				return old_obj
		config.set('init %s' % self._option, 'True')
		config.set('init config', 'True', section = 'global') # This will trigger a write of the new options
		return cur_obj


# Validation handler to check for variables in string
def validNoVar(section, option, obj):
	return utils.checkVar(obj, '[%s] "%s" may not contain variables.' % (section, option))
