#!/usr/bin/env python
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

import os, sys, time, fcntl, logging

# add python subdirectory from where exec was started to search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'packages')))

from grid_control import utils
from grid_control.backends import storage
from grid_control.config import createConfig
from grid_control.job_db import Job, JobClass, JobDB
from grid_control.job_selector import ClassSelector, JobSelector
from grid_control.output_processor import FileInfoProcessor, JobInfoProcessor
from grid_control.utils.cmd_options import Options
from hpfwk import Plugin
from python_compat import ifilter, imap, tarfile

def scriptOptions(parser, args = None, arg_keys = None):
	parser.addFlag(None, 'parseable', default = False, help = 'Output tabular data in parseable format')
	parser.addFlag(None, 'pivot',     default = False, help = 'Output pivoted tabular data')
	parser.addText(None, 'textwidth', default = 100,   help = 'Output tabular data with selected width')
	parser.addAccu(None, 'verbose',   short = '-v',    help = 'Increase verbosity')
	(opts, args, config_dict) = parser.parse(args, arg_keys)
	logging.getLogger().setLevel(logging.DEFAULT - opts.verbose)
	utils.verbosity(opts.verbose)
	if opts.parseable:
		utils.printTabular.mode = 'parseable'
	elif opts.pivot:
		utils.printTabular.mode = 'longlist'
	utils.printTabular.wraplen = int(opts.textwidth)
	return utils.Result(opts = opts, args = args, config_dict = config_dict)


def getConfig(configFile = None, configDict = None, section = None, additional = None):
	if configDict and section:
		configDict = {section: configDict}
	config = createConfig(configFile, configDict, additional = additional)
	if section:
		return config.changeView(addSections = [section])
	return config


class FileMutex:
	def __init__(self, lockfile):
		first = time.time()
		self.lockfile = lockfile
		while os.path.exists(self.lockfile):
			if first and (time.time() - first > 10):
				logging.info('Trying to aquire lock file %s ...', lockfile)
				first = False
			time.sleep(0.2)
		self.fd = open(self.lockfile, 'w')
		fcntl.flock(self.fd, fcntl.LOCK_EX)

	def __del__(self):
		fcntl.flock(self.fd, fcntl.LOCK_UN)
		try:
			if os.path.exists(self.lockfile):
				os.unlink(self.lockfile)
		except Exception:
			pass


def initGC(args):
	if len(args) > 0:
		config = getConfig(args[0])
		userSelector = None
		if len(args) != 1:
			userSelector = JobSelector.create(args[1])
		return (config.getWorkPath(), config, JobDB(config, jobSelector = userSelector))
	sys.stderr.write('Syntax: %s <config file> [<job id>, ...]\n\n' % sys.argv[0])
	sys.exit(os.EX_USAGE)


def getCMSSWInfo(tarPath):
	import xml.dom.minidom
	# Read framework report files to get number of events
	tarFile = tarfile.open(tarPath, 'r:gz')
	fwkReports = ifilter(lambda x: os.path.basename(x.name) == 'report.xml', tarFile.getmembers())
	for fwkReport in imap(tarFile.extractfile, fwkReports):
		try:
			yield xml.dom.minidom.parse(fwkReport)
		except Exception:
			logging.exception('Error while parsing %s', tarPath)
			raise


def prettySize(size):
	suffixes = [('B', 2**10), ('K', 2**20), ('M', 2**30), ('G', 2**40), ('T', 2**50)]
	for suf, lim in suffixes:
		if size > lim:
			continue
		else:
			return str(round(size / float(lim / 2**10), 2)) + suf
