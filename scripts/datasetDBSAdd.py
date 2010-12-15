#!/usr/bin/env python
import base64, xml.dom.minidom, optparse, locale, re, time, os, tarfile
from xml.dom.minidom import parseString
from gcSupport import *
from grid_control_cms import provider_dbsv2
from grid_control_cms.provider_dbsv2 import *
from grid_control_cms.DBSAPI.dbsMigrateApi import DbsMigrateApi
from grid_control_cms.DBSAPI.dbsApiException import DbsException, DbsBadRequest

class DBS(object):
	enum = ('CRC32', 'MD5', 'SIZE', 'LFN', 'SE', 'LUMI', 'TYPE', 'EVENTS', 'JOBHASH',
		'PARENT_FILES', 'PARENT_INFO', 'CONFIGHASH', 'CONFIGNAME', 'CONFIG', 'CMSSW_VER')
	for id, state in enumerate(enum):
		locals()[state] = id


def MakeDBSApi(url):
	if not "X509_USER_PROXY" in os.environ:
		raise RuntimeError("Environment variable X509_USER_PROXY not set!")
	return createDBSAPI(url)


class DBSInfoProvider(datasets.GCProvider):
	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		# Override scan modules, other settings setup by GCProvider
		tmp = QM(os.path.isdir(datasetExpr), ['OutputDirsFromWork'], ['OutputDirsFromConfig', 'MetadataFromModule'])
		config.set(section, 'scanner', str.join(tmp + ['ObjectsFromCMSSW', 'FilesFromJobInfo',
			'MetadataFromCMSSW', 'SEListFromPath', 'LFNFromPath', 'DetermineEvents']))
		config.set(section, 'events ignore empty', 'False')
		config.set(section, 'include parent infos', 'True')
		config.set(section, 'include config infos', 'True')
		GCProvider.__init__(self, config, section, dbsScanner, datasetNick, datasetID)


class XMLWriter: # xml.dom.minidom needs a lot of memory - could be replaced by something simpler...
	def __init__(self):
		self.doc = xml.dom.minidom.Document()
		self.doc.appendChild(self.doc.createComment(" DBS Version 1 "))

	def newElement(self, root, name, att = {}):
		new = self.doc.createElement(name)
		for key, value in att.items():
			new.setAttribute(key, str(value))
		QM(root, root, self.doc).appendChild(new)
		return new

	def finish(self):
		return self.doc.toprettyxml(indent="\t").replace('<?xml version="1.0" ?>', "<?xml version='1.0' standalone='yes'?>")


def getDBSXML(opts, block, allBlocks):
	def collectVar(key, src):
		idx = block[DataProvider.Metadata].index(key)
		return filter(lambda x: x, map(lambda fi: fi[DataProvider.Metadata][idx], src))
	def collectDict(key, valueKeys, src):
		idxKey = block[DataProvider.Metadata].index(key)
		idxList = map(lambda k: block[DataProvider.Metadata].index(k), valueKeys)
		result = {}
		for fi in src:
			result[fi[DataProvider.Metadata][idxKey]] = map(lambda idx: fi[DataProvider.Metadata][idx], idxList)
		return result

	# Create DBS dump file
	writer = XMLWriter()
	nodeDBS = writer.newElement(None, "dbs")

	# Dataset / Block identifier
	fqBlock = "%s#%s" % (block[DataProvider.Dataset], block[DataProvider.BlockName])
	writer.newElement(nodeDBS, "dataset", {"block_name": fqBlock, "path": block[DataProvider.Dataset]})

	# Primary dataset information - get datatype from other blocks in dataset in case of eg. 0 event block
	primary = block[DataProvider.Dataset].split("/")[1]
	dataType = set(collectVar('CMSSW_DATATYPE', block[DataProvider.FileList]))
	if len(dataType) == 0: # Try to recover type from all blocks
		allDataTypes = map(lambda b: set(collectVar('CMSSW_DATATYPE', b[DataProvider.FileList])), allBlocks)
		for t in allDataTypes:
			dataType.update(t)
	if len(dataType) > 1:
		raise RuntimeException("Data and MC files are mixed!")
	elif len(dataType) == 0:
		if opts.datatype:
			dataType.add(opts.datatype)
		else:
			raise RuntimeException("Please supply dataset type via --datatype!")
	writer.newElement(nodeDBS, "primary_dataset", {"primary_name": primary,
		"annotation": 'NO ANNOTATION PROVIDED', "type": dataType.pop().lower(),
		"start_date": 'NO_END_DATE PROVIDED', "end_date": ''})

	# Describes the processed data
	nodeProc = writer.newElement(nodeDBS, "processed_dataset", {"primary_datatset_name": primary,
		"processed_datatset_name": block[DataProvider.Dataset].split("/")[2],
		"status": 'VALID', "physics_group_name": 'NoGroup', "physics_group_convener": 'NO_CONVENOR',
	})
	writer.newElement(nodeProc, "path", {"dataset_path": block[DataProvider.Dataset]})
	for tier in block[DataProvider.Dataset].split("/")[3].split("-"):
		writer.newElement(nodeProc, "data_tier", {"name": tier})

	# Harvest config infos
	configInfos = {}
	for fi in block[DataProvider.FileList]:
		tmp = {}
		def setVar(key, format = lambda x: x):
			tmp[key] = format(fi[DataProvider.Metadata][block[DataProvider.Metadata].index(key)])
		setVar('CMSSW_CONFIG_CONTENT', base64.encodestring)
		for key in ['CMSSW_CONFIG_FILE', 'CMSSW_CONFIG_HASH', 'CMSSW_ANNOTATION', 'CMSSW_VERSION']:
			setVar(key)
		configInfos[fi[DataProvider.Metadata][block[DataProvider.Metadata].index('CMSSW_CONFIG_HASH')]] = tmp

	for cfgIdx in configInfos:
		writer.newElement(nodeProc, "algorithm", {"app_version": configInfos[cfgIdx]['CMSSW_VERSION'],
			"app_family_name": 'cmsRun', "app_executable_name": 'cmsRun',
			"ps_hash": configInfos[cfgIdx]['CMSSW_CONFIG_HASH']})

	# List dataset parents
	if opts.importParents:
		for path in set(collectVar('PARENT_DATASET', block[DataProvider.FileList])):
			writer.newElement(nodeDBS, "processed_dataset_parent", {"path": path})

	# Algorithm describing the job configuration
	for cfgIdx in configInfos:
		writer.newElement(nodeDBS, "processed_dataset_algorithm", {
			"app_version": configInfos[cfgIdx]['CMSSW_VERSION'], "app_family_name": "cmsRun",
			"app_executable_name": "cmsRun", "ps_content": configInfos[cfgIdx]['CMSSW_CONFIG_CONTENT'],
			"ps_hash": configInfos[cfgIdx]['CMSSW_CONFIG_HASH'], "ps_name": configInfos[cfgIdx]['CMSSW_CONFIG_HASH'],
			"ps_version": "private version", "ps_type": "user", "ps_annotation": base64.encodestring("user cfg")
		})

	# Give information about List files in block
	nodeBlock = writer.newElement(nodeDBS, "block", {"name": fqBlock, "path": block[DataProvider.Dataset],
		"size": sum(collectVar('SE_OUTPUT_SIZE', block[DataProvider.FileList])),
		"number_of_events": block[DataProvider.NEvents], "number_of_files": len(block[DataProvider.FileList]),
		"open_for_writing": QM(opts.doClose, '0', "1")})
	for se in block[DataProvider.SEList]:
		writer.newElement(nodeBlock, "storage_element", {"storage_element_name": se})

	# List files in block
	for fi in block[DataProvider.FileList]:
		getM = lambda key: fi[DataProvider.Metadata][block[DataProvider.Metadata].index(key)]

		nodeFile = writer.newElement(nodeDBS, "file", {"lfn": fi[DataProvider.lfn],
			"queryable_meta_data": 'NOTSET', "validation_status": 'VALID', "status": 'VALID', "type": 'EDM',
			"checksum": getM('SE_OUTPUT_HASH_CRC32'), "adler32": 'NOTSET', "md5": getM('SE_OUTPUT_HASH_MD5'),
			"size": getM('SE_OUTPUT_SIZE'), "number_of_events": fi[DataProvider.NEvents], "block_name": fqBlock})

		# List algos in file
		writer.newElement(nodeFile, "file_algorithm", {"app_version": getM('CMSSW_VERSION'),
			"app_family_name": 'cmsRun', "app_executable_name": 'cmsRun', "ps_hash": getM('CMSSW_CONFIG_HASH')})

		# List parents of file
		if opts.importParents:
			for parentLFN in getM('CMSSW_PARENT_LFN'):
				writer.newElement(nodeFile, "file_parent", {"lfn": parentLFN})

		# List lumi sections
		if opts.doLumi:
			for (run, lumi) in getM('CMSSW_LUMIS'):
				writer.newElement(nodeFile, "file_lumi_section", {"run_number": run, "lumi_section_number": lumi,
					"start_event_number": '0', "end_event_number": '0', "lumi_start_time": '0', "lumi_end_time": '0'
				})

	# Finish xml file
	writer.newElement(nodeDBS, "SUCCESS")
	return writer.finish()


try:
	try:
		locale.setlocale(locale.LC_ALL, "")
	except:
		pass

	usage = "%s [OPTIONS] <work directory>" % sys.argv[0]
	parser = optparse.OptionParser(usage=usage)
	parser.add_option("-n", "--name",            dest="dbsPath",       default=None,
		help="Specify dbs path name(s) - Example: DataSetA,DataSetB,DataSetC")
	parser.add_option("-d", "--dataset",         dest="dataset",       default=None,
		help="Specify dataset(s) to process")

	parser.add_option("-L", "--no-lumi",         dest="doLumi",        default=True,   action="store_false",
		help="Do not include lumi section information [Default: Include Lumi information]")
	parser.add_option("-m", "--merge",           dest="doMerge",       default=False,  action="store_true",
		help="Merge output files from different parent blocks into a single block [Default: Keep boundaries]")
	parser.add_option("-p", "--no-parents",      dest="importParents", default=True,   action="store_false",
		help="Disable import of parent datasets into target DBS instance - Warning: this will disconnect the " +
			"dataset from it's parents [Default: Import parents]")
	parser.add_option("-P", "--use-pfn",         dest="usePFN",        default=False,  action="store_true",
		help="Use the pfn instead of the lfn in dataset structrures [Default: Use LFN]")
	parser.add_option("-T", "--datatype",        dest="datatype",      default=None,
		help="Supply dataset type in case cmssw report did not specify it - valid values: 'mc' or 'data'")
	parser.add_option("-J", "--jobhash",         dest="useJobHash",    default=False,  action="store_true",
		help="Use hash of all config files in job for dataset key calculation")

	ogMode = optparse.OptionGroup(parser, "Processing mode", "")
	ogMode.add_option("-b", "--batch",           dest="batch",         default=False, action="store_true",
		help="Enable non-interactive batch mode [Default: Interactive mode]")
	ogMode.add_option("-i", "--no-import",       dest="doImport",      default=True,  action="store_false",
		help="Disable import of new datasets into target DBS instance - only temporary xml files are created, " +
			"which can be added later via datasetDBSTool.py [Default: Import datasets]")
	parser.add_option_group(ogMode)

	ogInc = optparse.OptionGroup(parser, "Incremental adding of files to DBS", "")
	ogInc.add_option("-r", "--incremental",     dest="incremental",   default=False,  action="store_true",
		help="Skip import of existing files - Warning: this destroys coherent block structure!")
	ogInc.add_option("-o", "--open-blocks",     dest="doClose",       default=True,  action="store_false",
		help="Keep blocks open for addition of further files [Default: Close blocks]")
	parser.add_option_group(ogInc)

	ogInst = optparse.OptionGroup(parser, "DBS instance handling", "")
	ogInst.add_option("-t", "--target-instance", dest="dbsTarget",
		default="https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_02_writer/servlet/DBSServlet",
		help="Specify target dbs instance url")
	ogInst.add_option("-s", "--source-instance", dest="dbsSource",
		default="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet",
		help="Specify source dbs instance url(s), where parent datasets are taken from")
	parser.add_option_group(ogInst)

	ogDbg = optparse.OptionGroup(parser, "Debug options - enables also dryrun", "")
	ogDbg.add_option("-D", "--display-dataset", dest="display_data",  default=None,
		help="Display information associated with dataset key(s) (accepts 'all')")
	ogDbg.add_option("-C", "--display-config",  dest="display_cfg",   default=None,
		help="Display information associated with config hash(es) (accepts 'all')")
	parser.add_option_group(ogDbg)

	(opts, args) = parser.parse_args()

	allBlocks = DataProvider.loadState(Config(), '.', args[0]).getBlocks()
	for block in allBlocks:
		open(block[DataProvider.BlockName], 'w').write(getDBSXML(opts, block, allBlocks))
except GCError:
	sys.stderr.write(GCError.message)
