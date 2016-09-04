# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

def init_hpf_logging():
	import logging

	logLevelDict = {'DEFAULT': logging.INFO - 1,
		'INFO1': logging.DEBUG + 3, 'INFO2': logging.DEBUG + 2, 'INFO3': logging.DEBUG + 1,
		'DEBUG1': logging.DEBUG - 1, 'DEBUG2': logging.DEBUG - 2, 'DEBUG3': logging.DEBUG - 3}

	# Register new log levels
	for name in logLevelDict:
		setattr(logging, name.upper(), logLevelDict[name]) # Add numerical constant
		logging.addLevelName(logLevelDict[name], name)     # Register with logging module
