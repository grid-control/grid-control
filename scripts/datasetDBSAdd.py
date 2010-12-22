#!/usr/bin/env python
import base64, xml.dom.minidom, optparse, locale, re, time, os, operator
from gcSupport import *
from grid_control.gui import Console
from grid_control_cms import provider_dbsv2
from grid_control_cms.provider_dbsv2 import *
from grid_control_cms.DBSAPI.dbsMigrateApi import DbsMigrateApi
from grid_control_cms.DBSAPI.dbsApiException import DbsException, DbsBadRequest

class DBSInfoProvider(datasets.GCProvider):
	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		# Override scan modules, other settings setup by GCProvider
		tmp = QM(os.path.isdir(datasetExpr), ['OutputDirsFromWork'], ['OutputDirsFromConfig', 'MetadataFromModule'])
		config.set(section, 'scanner', str.join(' ', tmp + ['ObjectsFromCMSSW', 'FilesFromJobInfo',
			'MetadataFromCMSSW', 'ParentLookup', 'SEListFromPath', 'LFNFromPath', 'DetermineEvents']))
		config.set(section, 'include config infos', 'True')
		config.set(section, 'parent keys', 'CMSSW_PARENT_LFN CMSSW_PARENT_PFN')
		config.set(section, 'events key', 'CMSSW_EVENTS_WRITE')
		datasets.GCProvider.__init__(self, config, section, datasetExpr, datasetNick, datasetID)

	def generateDatasetName(self, key, data):
		if 'CMSSW_DATATIER' not in data:
			raise RuntimeError('Incompatible data tiers in dataset: %s' % data)
		getPathComponents = lambda path: QM(path, tuple(path.strip('/').split('/')), ())
		userPath = getPathComponents(self.nameDS)

		(primary, processed, tier) = (None, None, None)
		# In case of a child dataset, use the parent infos to construct new path
		for parent in data.get('PARENT_PATH', []):
			if len(userPath) == 3:
				(primary, processed, tier) = userPath
			else:
				try:
					(primary, processed, tier) = getPathComponents(parent)
				except:
					pass
		if (primary == None) and (len(userPath) > 0):
			primary = userPath[0]
			userPath = userPath[1:]

		if len(userPath) == 2:
			(processed, tier) = userPath
		elif len(userPath) == 1:
			(processed, tier) = (userPath[0], data['CMSSW_DATATIER'])
		elif len(userPath) == 0:
			(processed, tier) = ('Dataset_%s' % key, data['CMSSW_DATATIER'])

		if None in (primary, processed, tier):
			raise RuntimeError('Invalid dataset name supplied: %s' % repr(self.nameDS))
		return utils.replaceDict('/%s/%s/%s' % (primary, processed, tier), data)

	def generateBlockName(self, key, data):
		return utils.strGuid(key)


class XMLWriter: # xml.dom.minidom needs a lot of memory - could be replaced by something simpler...
	def __init__(self):
		self.doc = xml.dom.minidom.Document()
		self.doc.appendChild(self.doc.createComment(' DBS Version 1 '))

	def newElement(self, root, name, att = {}):
		new = self.doc.createElement(name)
		for key, value in att.items():
			new.setAttribute(key, str(value))
		QM(root, root, self.doc).appendChild(new)
		return new

	def finish(self):
		return self.doc.toprettyxml(indent='\t').replace('<?xml version="1.0" ?>', "<?xml version='1.0' standalone='yes'?>")


def getDBSXML(opts, block, dsBlocks):
	getM = lambda idx, fi: fi[DataProvider.Metadata][idx]
	getAllM = lambda idx, fiList = block[DataProvider.FileList]: filter(lambda x: x, map(lambda fi: getM(idx, fi), fiList))
	getOutsideM = lambda key, outside: getAllM(outside[DataProvider.Metadata].index(key), outside[DataProvider.FileList])

	# Check validity of input dataset
	def getKey(k):
		try:
			return block[DataProvider.Metadata].index(k)
		except:
			raise RethrowError('Could not find metadata %s' % k)
	DBS = type('EnumDBS', (), dict(map(lambda (n, k): (n, getKey(k)), {'CONFIG_FILE': 'CMSSW_CONFIG_FILE',
		'CONFIG_HASH': 'CMSSW_CONFIG_HASH', 'TYPE': 'CMSSW_DATATYPE', 'ANNOTATION': 'CMSSW_ANNOTATION',
		'VERSION': 'CMSSW_VERSION', 'CONFIG_CONTENT': 'CMSSW_CONFIG_CONTENT', 'SIZE': 'SE_OUTPUT_SIZE',
		'HASH_CRC32': 'SE_OUTPUT_HASH_CRC32', 'HASH_MD5': 'SE_OUTPUT_HASH_MD5'}.items())))
	if opts.importParents:
		for (n, k) in {'PARENT_PATH': 'PARENT_PATH', 'PARENT_LFN': 'CMSSW_PARENT_LFN'}.items():
			setattr(DBS, n, getKey(k))
	if opts.importLumi:
		DBS.LUMIS = getKey('CMSSW_LUMIS')

	if block[DataProvider.Dataset].count('/') != 3:
		raise RuntimeError('Invalid dataset name: %s' % block[DataProvider.Dataset])
	if block[DataProvider.BlockName] != utils.strGuid(block[DataProvider.BlockName].replace('-', '')):
		raise RuntimeError('Invalid block name: %s' % block[DataProvider.BlockName])

	dataType = set(getAllM(DBS.TYPE))
	if len(dataType) == 0: # Try to recover type from other blocks
		for t in map(lambda b: getOutsideM('CMSSW_DATATYPE', b), dsBlocks):
			dataType.update(t)
	if len(dataType) > 1:
		raise RuntimeException('Data and MC files are mixed!')
	elif len(dataType) == 0:
		if not opts.datatype:
			raise RuntimeException('Please supply dataset type via --datatype!')
		dataType = opts.datatype
	else:
		dataType = dataType.pop()

	# Create DBS dump file
	writer = XMLWriter()
	nodeDBS = writer.newElement(None, 'dbs')

	# Dataset / Block identifier
	fqBlock = '%s#%s' % (block[DataProvider.Dataset], block[DataProvider.BlockName])
	writer.newElement(nodeDBS, 'dataset', {'block_name': fqBlock, 'path': block[DataProvider.Dataset]})

	# Primary dataset information - get datatype from other blocks in dataset in case of eg. 0 event block
	primary = block[DataProvider.Dataset].split('/')[1]
	writer.newElement(nodeDBS, 'primary_dataset', {'primary_name': primary,
		'annotation': 'NO ANNOTATION PROVIDED', 'type': dataType.lower(),
		'start_date': 'NO_END_DATE PROVIDED', 'end_date': ''})

	# Describes the processed data
	nodeProc = writer.newElement(nodeDBS, 'processed_dataset', {'primary_datatset_name': primary,
		'processed_datatset_name': block[DataProvider.Dataset].split('/')[2],
		'status': 'VALID', 'physics_group_name': 'NoGroup', 'physics_group_convener': 'NO_CONVENOR',
	})
	writer.newElement(nodeProc, 'path', {'dataset_path': block[DataProvider.Dataset]})
	for tier in block[DataProvider.Dataset].split('/')[3].split('-'):
		writer.newElement(nodeProc, 'data_tier', {'name': tier})

	# Harvest config infos and put them into a dictionary (+b64 encode config file content)
	configInfos = {}
	for fi in block[DataProvider.FileList]:
		configInfos[getM(DBS.CONFIG_HASH, fi)] = dict(map(lambda key: (key, getM(key, fi)),
				[DBS.CONFIG_FILE, DBS.CONFIG_HASH, DBS.ANNOTATION, DBS.VERSION]) +
			[(DBS.CONFIG_CONTENT, base64.encodestring(getM(DBS.CONFIG_CONTENT, fi)))])

	for cfgDict in configInfos.values():
		writer.newElement(nodeProc, 'algorithm', {'app_version': cfgDict[DBS.VERSION],
			'app_family_name': 'cmsRun', 'app_executable_name': 'cmsRun', 'ps_hash': cfgDict[DBS.CONFIG_HASH]})

	# List dataset parents
	if opts.importParents:
		for path in set(reduce(operator.add, getAllM(DBS.PARENT_PATH), [])):
			writer.newElement(nodeDBS, 'processed_dataset_parent', {'path': path})

	# Algorithm describing the job configuration
	for cfgDict in configInfos.values():
		writer.newElement(nodeDBS, 'processed_dataset_algorithm', {'app_version': cfgDict[DBS.VERSION],
			'app_family_name': 'cmsRun', 'app_executable_name': 'cmsRun', 'ps_content': cfgDict[DBS.CONFIG_CONTENT],
			'ps_hash': cfgDict[DBS.CONFIG_HASH], 'ps_name': cfgDict[DBS.CONFIG_FILE],
			'ps_version': 'private version', 'ps_type': 'user', 'ps_annotation': base64.encodestring('user cfg')
		})

	# Give information about List files in block
	nodeBlock = writer.newElement(nodeDBS, 'block', {'name': fqBlock, 'path': block[DataProvider.Dataset],
		'number_of_events': block[DataProvider.NEvents], 'number_of_files': len(block[DataProvider.FileList]),
		'size': sum(getAllM(DBS.SIZE)), 'open_for_writing': QM(opts.closeBlock, '0', '1')})
	for se in block[DataProvider.SEList]:
		writer.newElement(nodeBlock, 'storage_element', {'storage_element_name': se})

	# List files in block
	for fi in block[DataProvider.FileList]:
		nodeFile = writer.newElement(nodeDBS, 'file', {'lfn': fi[DataProvider.lfn],
			'queryable_meta_data': 'NOTSET', 'validation_status': 'VALID', 'status': 'VALID', 'type': 'EDM',
			'checksum': getM(DBS.HASH_CRC32, fi), 'adler32': 'NOTSET', 'md5': getM(DBS.HASH_MD5, fi),
			'size': getM(DBS.SIZE, fi), 'number_of_events': fi[DataProvider.NEvents], 'block_name': fqBlock})

		# List algos in file
		writer.newElement(nodeFile, 'file_algorithm', {'app_version': getM(DBS.VERSION, fi),
			'app_family_name': 'cmsRun', 'app_executable_name': 'cmsRun', 'ps_hash': getM(DBS.CONFIG_HASH, fi)})

		# List parents of file
		if opts.importParents:
			for parentLFN in getM(DBS.PARENT_LFN, fi):
				writer.newElement(nodeFile, 'file_parent', {'lfn': parentLFN})

		# List lumi sections
		if opts.importLumi:
			for (run, lumi) in getM(DBS.LUMIS, fi):
				writer.newElement(nodeFile, 'file_lumi_section', {'run_number': run, 'lumi_section_number': lumi,
					'start_event_number': '0', 'end_event_number': '0', 'lumi_start_time': '0', 'lumi_end_time': '0'
				})

	# Finish xml file
	writer.newElement(nodeDBS, 'SUCCESS')
	return (fqBlock, writer.finish())


# Create dbs dump files for all blocks
def createDBSXMLDumps(opts, allBlocks):
	produced = []
	for idx, block in enumerate(allBlocks):
		log = utils.ActivityLog(' * Creating DBS dump files - [%d / %d]' % (idx, len(allBlocks)))
		dsBlocks = filter(lambda b: block[DataProvider.Dataset] == b[DataProvider.Dataset], allBlocks)
		xmlDSPath = os.path.join(opts.tmpDir, utils.strGuid(md5(block[DataProvider.Dataset]).hexdigest()))
		if not os.path.exists(xmlDSPath):
			os.mkdir(xmlDSPath)
		for block in allBlocks:
			fqBlock, dump = getDBSXML(opts, block, allBlocks)
			xmlBPath = os.path.join(xmlDSPath, block[DataProvider.BlockName])
			open(xmlBPath, 'w').write(dump)
			produced.append((fqBlock, xmlBPath))
		del log
	utils.vprint(' * Creating DBS dump files - done', -1)
	return produced


def makeDBSAPI(url):
	if not 'X509_USER_PROXY' in os.environ:
		raise RuntimeError('Environment variable X509_USER_PROXY not set!')
	return createDBSAPI(url)


# Check the existence of a dataset
def hasDataset(url, dataset):
	try:
		api = makeDBSAPI(url)
		return len(api.listBlocks(dataset, nosite = True)) > 0
	except DbsBadRequest:
		return False


# Currently depends on the whole dataset being registered at the DBS instance
def registerParent(opts, parentPath):
	if not hasDataset(opts.dbsTarget, parentPath):
		# Parent dataset has to be moved to target dbs instance
		text = ' * Migrating dataset parent... %s (This can take a lot of time!)' % parentPath
		log = utils.ActivityLog(text)
		try:
			quiet = Silencer()
			for dbsSourceSelected in map(str.strip, opts.dbsSource.split(',')):
				if hasDataset(dbsSourceSelected, parentPath):
					DbsMigrateApi(dbsSourceSelected, opts.dbsTarget).migrateDataset(parentPath)
			del quiet
		except:
			del quiet
			raise RuntimeError('Could not migrate dataset %s to target!' % parentPath)
		del log
		utils.vprint(' * Migrating dataset parent %s - done' % parentPath, -1)


# Register datasets at dbs instance
def registerDataset(opts, fqBlock, xmlFile):
	try:
		utils.vprint(' * %s' % fqBlock, -1)
		log = utils.ActivityLog('   * Importing dataset file... %s' % os.path.basename(xmlFile))
		makeDBSAPI(opts.dbsTarget).insertDatasetContents(open(xmlFile).read())
		del log
		return True
	except DbsException, e:
		utils.eprint('   ! Could not import %s' % fqBlock)
		errorMsg = e.getErrorMessage()
		errorPath = xmlFile.replace('.xml', '.log')
		for msg in errorMsg.splitlines():
			if str(msg) == '':
				break
			utils.eprint('   ! %s' % msg)
		utils.eprint('   ! The complete error log can be found in:\n   ! %s' % errorPath)
		open(errorPath, 'w').write(errorMsg)


try:
	try:
		locale.setlocale(locale.LC_ALL, '')
	except:
		pass

	usage = '%s [OPTIONS] <config file / work directory>' % sys.argv[0]
	parser = optparse.OptionParser(usage=usage)
	parser.add_option('-F', '--input',           dest='inputFile',          default=None,
		help='Specify dbs input file to use instead of scanning job output')
	parser.add_option('-k', '--key-select',      dest='dataset key select', default='',
		help='Specify dataset keys to process')

	parser.add_option('-L', '--no-lumi',         dest='importLumi',    default=True,   action='store_false',
		help='Do not include lumi section information [Default: Include Lumi information]')
	parser.add_option('-p', '--no-parents',      dest='importParents', default=True,   action='store_false',
		help='Disable import of parent datasets into target DBS instance - ' +
			'Warning: this will disconnect the dataset from it\'s parents [Default: Import parents]')

	ogDiscover = optparse.OptionGroup(parser, 'Discovery options - ignored in case dbs input file is specified', '')
	ogDiscover.add_option('-n', '--name',        dest='dataset name pattern', default='',
		help='Specify dbs path name - Example: DataSet_@NICK@_@VAR@')
	ogDiscover.add_option('-T', '--datatype',    dest='datatype',      default=None,
		help='Supply dataset type in case cmssw report did not specify it - valid values: "mc" or "data"')
	ogDiscover.add_option('-m', '--merge',       dest='merge parents', default=False,  action='store_true',
		help='Merge output files from different parent blocks into a single block [Default: Keep boundaries]')
	ogDiscover.add_option('-j', '--jobhash',     dest='useJobHash',    default=False,  action='store_true',
		help='Use hash of all config files in job for dataset key calculation')
	ogDiscover.add_option('-P', '--parent',      dest='parent source', default='',
		help='Override parent information source - to bootstrap a reprocessing on local files')
	parser.add_option_group(ogDiscover)

	ogDiscover2 = optparse.OptionGroup(parser, 'Discovery options II - only available when config file is used', '')
	ogDiscover2.add_option('-J', '--job-selector',    dest='selected',      default=None,
		help='Specify dataset(s) to process')
	parser.add_option_group(ogDiscover2)

	ogMode = optparse.OptionGroup(parser, 'Processing mode', '')
	ogMode.add_option('-b', '--batch',           dest='batch',         default=False, action='store_true',
		help='Enable non-interactive batch mode [Default: Interactive mode]')
	ogMode.add_option('-i', '--no-import',       dest='doImport',      default=True,  action='store_false',
		help='Disable import of new datasets into target DBS instance - only temporary xml files are created, ' +
			'which can be added later via datasetDBSTool.py [Default: Import datasets]')
	parser.add_option_group(ogMode)

	ogInc = optparse.OptionGroup(parser, 'Incremental adding of files to DBS', '')
	ogInc.add_option('-I', '--incremental',     dest='incremental',   default=False,  action='store_true',
		help='Skip import of existing files - Warning: this destroys coherent block structure!')
	ogInc.add_option('-o', '--open-blocks',     dest='closeBlock',    default=True,   action='store_false',
		help='Keep blocks open for addition of further files [Default: Close blocks]')
	parser.add_option_group(ogInc)

	ogInst = optparse.OptionGroup(parser, 'DBS instance handling', '')
	ogInst.add_option('-t', '--target-instance', dest='dbsTarget',
		default='https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_02_writer/servlet/DBSServlet',
		help='Specify target dbs instance url')
	ogInst.add_option('-s', '--source-instance', dest='dbsSource',
		default='http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet',
		help='Specify source dbs instance url(s), where parent datasets are taken from')
	parser.add_option_group(ogInst)

	ogDbg = optparse.OptionGroup(parser, 'Display options', '')
	ogDbg.add_option('-D', '--display-dataset', dest='display_data',  default=None,
		help='Display information associated with dataset key(s) (accepts "all")')
	ogDbg.add_option('-C', '--display-config',  dest='display_cfg',   default=None,
		help='Display information associated with config hash(es) (accepts "all")')
	parser.add_option_group(ogDbg)

	(opts, args) = parser.parse_args()
	if opts.useJobHash:
		setattr(opts, 'dataset hash keys', getattr(opts, 'dataset hash keys') + ' CMSSW_CONFIG_JOBHASH')

	# 0) Get work directory, create dbs dump directory
	if len(args) != 1:
		utils.exitWithUsage(usage, 'Neither work directory nor config file specified!')
	if os.path.isdir(args[0]):
		opts.workDir = os.path.abspath(os.path.normpath(args[0]))
	else:
		config = Config(args[0])
		opts.workDir = config.getPath('global', 'workdir', config.workDirDefault)
	opts.tmpDir = os.path.join(opts.workDir, 'dbs')
	if not os.path.exists(opts.tmpDir):
		os.mkdir(opts.tmpDir)
	# Lock file in case several instances of this program are running
	mutex = FileMutex(os.path.join(opts.tmpDir, 'datasetDBSAdd.lock'))

	# 1) Get dataset information
	if opts.inputFile:
		provider = ListProvider(Config(), None, opts.inputFile, None)
	else:
		provider = DBSInfoProvider(Config(configDict = {None: dict(parser.values.__dict__)}), None, args[0], None)
	provider.saveState(opts.tmpDir)
	blocks = provider.getBlocks()

	# 2) Filter datasets
	if opts.incremental:
		dNames = set(map(lambda b: b[DataProvider.Dataset], blocks))
		dNames = filter(lambda ds: hasDataset(opts.dbsTarget, ds), dNames)
		config = Config(configDict = {None: {'dbs instance': opts.dbsTarget}})
		oldBlocks = reduce(operator.add, map(lambda ds: DBSApiv2(config, None, ds, None).getBlocks(), dNames), [])
		(blocksAdded, blocksMissing, blocksChanged) = DataProvider.resyncSources(oldBlocks, blocks)
		if len(blocksMissing) or len(blocksChanged):
			if utils.getUserBool(' * WARNING: Block structure has changed! Continue?', False):
				sys.exit(0)
		setOldBlocks = set(map(lambda x: x[DataProvider.Blockname], oldBlocks))
		setAddedBlocks = set(map(lambda x: x[DataProvider.Blockname], blocksAdded))
		blockCollision = set.intersection(setOldBlocks, setAddedBlocks)
		if blockCollision and opts.closeBlock: # Block are closed and contents have changed
			for block in blocksAdded:
				if block[DataProvider.Blockname] in blockCollision:
					block[DataProvider.Blockname] = utils.strGuid(md5(str(time.time())).hexdigest())
		blocks = blocksAdded

	# 3) Display dataset properties
	if opts.display_data:
		displayDatasetInfos(opts.display_data, blocks)
	if opts.display_cfg:
		displayConfigInfos(opts.display_cfg, blocks)

	# 4) Translate into DBSXML
	xmlFiles = createDBSXMLDumps(opts, blocks)
	if not opts.doImport or opts.display_cfg or opts.display_data:
		sys.exit(0)

	# 5) Migrate parent datasets
	if opts.importParents:
		os.chdir(opts.tmpDir)
		def getBlockM(block):
			getFileM = lambda fi: fi[DataProvider.Metadata][block[DataProvider.Metadata].index('PARENT_PATH')]
			getFileListM = lambda fl: reduce(operator.add, map(getFileM, fl))
			return reduce(operator.add, map(getFileListM, block[DataProvider.FileList]))
		parents = set(reduce(operator.add, map(getBlockM, blocks), []))
		if len(parents) > 0:
			utils.vprint(' * The following parents will be needed at the target dbs instance:', -1)
			utils.vprint(str.join('', map(lambda x: '   * %s\n' % x, parents)), -1)
			if not (opts.batch or utils.getUserBool(' * Register these parents?', True)):
				sys.exit(0)
			for parent in parents:
				registerParent(opts, parent)

	# 6) Insert blocks into DBS
	if opts.batch or utils.getUserBool(" * Start dataset import?", True):
		fail = False
		for (fqBlock, xmlBPath) in produced:
			if not registerDataset(fqBlock, xmlBPath):
				fail = True
		utils.vprint(" * Importing datasets - %s\n" % QM(fail, 'failed', 'done'), -1)
	del mutex
except GCError:
	sys.stderr.write(GCError.message)
	del mutex
