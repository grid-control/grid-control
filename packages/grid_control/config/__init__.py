from config_basic import Config, CompatConfig, noDefault
from config_handlers import validNoVar, changeImpossible, changeInitNeeded

# At the lowest level, all config option values are represented by strings
# which are encapsulated in the ConfigEntry class, which holds access and source
# information in addition to the value.

# These config entries are stored in an ConfigContainer, which makes the
# entries accessible via "section" and "option" specifiers.

# The type parsing config interface (getInt, getBool, getClass, ...) is defined
# in the ConfigBase class
