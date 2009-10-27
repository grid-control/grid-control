#!/usr/bin/env python
import gcSupport, base64, xml.dom.minidom, optparse, locale, re, time
from xml.dom.minidom import parseString
from grid_control import *
from DBSAPI_v2.dbsMigrateApi import DbsMigrateApi

class DBS(object):
	enum = ('CRC32', 'MD5', 'SIZE', 'LFN', 'SE', 'LUMI', 'TYPE', 'EVENTS',
		'PARENT_FILES', 'PARENT_INFO', 'CONFIGHASH', 'CONFIGNAME', 'CONFIG', 'CMSSW_VER')
	for id, state in enumerate(enum):
		locals()[state] = id


def MakeDBSApi(url):
	proxy = VomsProxy(gcSupport.ConfigDummy({"proxy": {"ignore warnings": True}}))
	return DBSAPI_v2.dbsApi.DbsApi({'version': 'DBS_2_0_6', 'level': 'CRITICAL',
		'url': url, 'userID': proxy._getInfo()['identity']})


def readDBSJobInfo(opts, workDir, jobNum):
	# Read general grid-control file infos
	fileDictGC = {}
	try:
		files = gcSupport.getFileInfo(workDir, jobNum, lambda retCode: retCode == 0, rejected = None)
		for (hash, name_local, name_dest, pathSE) in files:
			seUrl = os.path.join(pathSE, name_dest)
			se = pathSE.split(":")[1].lstrip("/").split("/")[0]
			lfn = os.path.join('/store', seUrl.split("/store/",1)[-1])
			fileDictGC[name_local] = zip((DBS.MD5, DBS.LFN, DBS.SE), (hash, lfn, se))
	except:
		raise RuntimeError("Could not read grid-control file infos for job %d!" % jobNum)

	# Open CMSSW overview file
	try:
		tar = tarfile.open(os.path.join(workDir, 'output', 'job_%d' % jobNum, 'cmssw.dbs.tar.gz'), 'r')
	except:
		raise RuntimeError("Could not open CMSSW file infos for job %d!" % jobNum)

	# Read CMSSW file infos
	fileDictCMSSW = {}
	try:
		for rawdata in map(str.split, tar.extractfile('files').readlines()):
			fileDictCMSSW[rawdata[2]] = zip((DBS.CRC32, DBS.SIZE), rawdata[:-1])
	except:
		raise RuntimeError("Could not read CMSSW file infos for job %d!" % jobNum)

	# Read CMSSW config infos
	configData = {}
	configReports = {}
	cmsswVersion = tar.extractfile("version").read().strip()
	for cfg in filter(lambda x: not '/' in x and x not in ['version', 'files'], tar.getnames()):
		try:
			cfgHash = tar.extractfile("%s/hash" % cfg).readlines()[-1].strip()
			configData[cfgHash] = { DBS.CONFIGNAME: cfg, DBS.CMSSW_VER: cmsswVersion,
				DBS.CONFIG: tar.extractfile("%s/config" % cfg).read()
			}
			configReports[cfgHash] = parseString(tar.extractfile("%s/report.xml" % cfg).read())
		except:
			raise
			raise RuntimeError("Could not read config infos about %s in job %d" % (cfg, jobNum))

	# Parse CMSSW framework infos
	fileDictReport = {}
	for cfgHash in configData.keys():
		dom = configReports[cfgHash]
		def readTag(base, tag):
			return str(base.getElementsByTagName(tag)[0].childNodes[0].data)
		fileDictReport = {}
		for outputFile in dom.getElementsByTagName('File'):
			toRead = [("DataType", DBS.TYPE), ("TotalEvents", DBS.EVENTS)]
			tmp = map(lambda (tag, key): (key, readTag(outputFile, tag)), toRead)

			if opts.importParents:
				try:
					inputs = outputFile.getElementsByTagName("Inputs")[0].getElementsByTagName("Input")
					tmp.append((DBS.PARENT_FILES, map(lambda x: readTag(x, "LFN"), inputs)))
				except:
					raise RuntimeError("Could not parse lfn of parent!")
			else:
				tmp.append((DBS.PARENT_FILES, []))

			lumis = []
			runs = outputFile.getElementsByTagName("Runs")[0]
			for run in runs.getElementsByTagName("Run"):
				runId = int(run.getAttribute("ID"))
				for lumi in run.getElementsByTagName("LumiSection"):
					lumis.append((runId, int(lumi.getAttribute("ID"))))
			tmp.append((DBS.LUMI, lumis))

			tmp.append((DBS.CONFIGHASH, cfgHash))
			fileDictReport[readTag(outputFile, "PFN")] = tmp

	# Cleanup & checks
	tar.close()

	# Combine file infos, include only framework files, other transferred files are ignored
	outputData = {}
	for key in fileDictReport.keys():
		tmp = dict(fileDictGC[key])
		tmp.update(dict(fileDictCMSSW[key]))
		tmp.update(dict(fileDictReport[key]))
		outputData[tmp[DBS.LFN]] = tmp

	return (outputData, configData)


def getOutputDatasets(opts):
	# Get job numbers, task id, ...
	log = utils.ActivityLog(' * Reading task info...')
	jobList = utils.sorted(gcSupport.getJobs(opts.workDir))
	taskInfo = utils.PersistentDict(os.path.join(opts.workDir, 'task.dat'), ' = ')
	del log
	print " * Reading task info - done"

	# Get all config and output data
	log = None
	configData = {}
	outputData = {}
	dbsLog = utils.PersistentDict(os.path.join(opts.workDir, 'dbs.log'), ' = ', False)
	for jobNum in jobList:
		if jobNum % 10 == 0:
			del log
			log = utils.ActivityLog(' * Reading job logs - [%d / %d]' % (jobNum, jobList[-1]))
		(output, config) = readDBSJobInfo(opts, opts.workDir, jobNum)
		# ignore already registed files in incremental mode
		for lfn in filter(lambda x: not (opts.incremental and x in dbsLog), output):
			outputData.update({lfn: output[lfn]})
		configData.update(config)
	print " * Reading job logs - done"

	# Merge parent infos into output file data
	if os.path.exists(os.path.join(opts.workDir, 'datacache.dat')):
		# Get parent infos
		provider = DataProvider.loadState(gcSupport.ConfigDummy(), opts.workDir, 'datacache.dat')
		log = utils.ActivityLog(' * Processing parent infos...')
		blocks = provider.getBlocks()
		parentMap = {}
		for block in blocks:
			blockInfo = (block[DataProvider.Dataset], block[DataProvider.BlockName])
			lfns = map(lambda x: (x[DataProvider.lfn], blockInfo), block[DataProvider.FileList])
			parentMap.update(dict(lfns))
		# Insert parentage infos
		for lfn in outputData.keys():
			for parentLFN in outputData[lfn][DBS.PARENT_FILES]:
				if not DBS.PARENT_INFO in outputData[lfn]:
					outputData[lfn][DBS.PARENT_INFO] = []
				if not parentMap[parentLFN] in outputData[lfn][DBS.PARENT_INFO]:
					outputData[lfn][DBS.PARENT_INFO].append(parentMap[parentLFN])
		del log
		print " * Processing parent infos - done"

	# Sort output files into blocks
	log = None
	metadata = {}
	datasets = {}
	datasetInfos = {}
	for idx, lfn in enumerate(outputData):
		if idx % 10 == 0:
			del log
			log = utils.ActivityLog(' * Dividing output into blocks - [%d / %d]' % (idx, len(outputData)))

		# Define dataset split criteria
		def generateDatasetKey(fileInfo):
			# Split by dataset parent and config hash
			parentDS = map(lambda (ds,b): ds, fileInfo.get(DBS.PARENT_INFO, []))
			dsKey = utils.md5(str((fileInfo[DBS.CONFIGHASH], parentDS))).hexdigest()
			# Write summary information:
			if not dsKey in datasetInfos:
				if parentDS == []: parentDS = ['None']
				datasetInfos[dsKey] = ("%15s: %s\n%15s: %s\n" % ("Config hash", fileInfo[DBS.CONFIGHASH],
					"Parent datasets", str.join("\n" + 17*" ", parentDS)))
			return dsKey

		# Define block split criteria
		def generateBlockKey(fileInfo):
			# Split by SE and block parent (parent is left out in case of merging)
			key = utils.md5(str(fileInfo[DBS.SE]) + generateDatasetKey(fileInfo))
			if not opts.doMerge:
				key.update(str(map(lambda (ds,b): b, fileInfo.get(DBS.PARENT_INFO, []))))
			return key.hexdigest()

		dsKey = generateDatasetKey(outputData[lfn])
		blockKey = generateBlockKey(outputData[lfn])
		if not dsKey in datasets:
			datasets[dsKey] = {}
			metadata[dsKey] = {DBS.SIZE: 0, DBS.EVENTS: 0}
		if not blockKey in datasets[dsKey]:
			datasets[dsKey][blockKey] = []
			metadata[blockKey] = {DBS.SIZE: 0, DBS.EVENTS: 0}

		# Calculate 
		def incStats(x, info):
			x[DBS.SIZE] += int(info[DBS.SIZE])
			x[DBS.EVENTS] += int(info[DBS.EVENTS])
		incStats(metadata[dsKey], outputData[lfn])
		incStats(metadata[blockKey], outputData[lfn])

		datasets[dsKey][blockKey].append(lfn)
	print " * Dividing output into blocks - done"

	# Display dataset information
	print
	print " => Identified the following output datasets:"
	for ds in datasets.keys():
		print "%4s * Key %s [%d block(s), %d file(s)]" % ("", ds, len(datasets[ds]), sum(map(len, datasets[ds].values())))
		print 7*" " + datasetInfos[ds].replace("\n", "\n" + 7*" ")

	return (taskInfo['task id'], datasets, metadata, outputData, configData)


def getBlockParents(lfns, outputData):
	return utils.unique(reduce(lambda x,y: x+y, map(lambda x: outputData[x].get(DBS.PARENT_INFO, []), lfns)))


def createDbsBlockDump(opts, newPath, blockInfo, metadata, outputData, configData):
	(blockKey, lfns) = blockInfo

	# Helper function to create xml elements
	def newElement(root, name = None, att = {}, text = ""):
		if name == None:
			new = doc.createTextNode(str(text))
		else:
			new = doc.createElement(name)
			for key, value in att.items():
				new.setAttribute(key, str(value))
		root.appendChild(new)
		return new

	# Create DBS dump file
	doc = xml.dom.minidom.Document()
	doc.appendChild(doc.createComment(" DBS Version 1 "))
	nodeDBS = newElement(doc, "dbs")

	# Dataset / Block identifier
	if opts.incremental:
		hex = str.join("", map(lambda x: "%02x" % random.randrange(256), range(16)))
	else:
		hex = blockKey
	blockName = '%s-%s-%s-%s-%s' % (hex[:8], hex[8:12], hex[12:16], hex[16:20], hex[20:])
	fqBlock = "%s#%s" % (newPath, blockName)
	newElement(nodeDBS, "dataset", {"block_name": fqBlock, "path": newPath})

	# Primary dataset information
	dataType = utils.unique(map(lambda x: outputData[x][DBS.TYPE], lfns))
	primary = newPath.split("/")[1]
	if len(dataType) > 1:
		raise RuntimeException("Data and MC files are mixed!")
	newElement(nodeDBS, "primary_dataset", {"primary_name": primary,
		"annotation": 'NO ANNOTATION PROVIDED', "type": dataType[0].lower(),
		"start_date": 'NO_END_DATE PROVIDED', "end_date": ''})

	# Describes the processed data
	nodeProc = newElement(nodeDBS, "processed_dataset", {"primary_datatset_name": primary,
		"processed_datatset_name": newPath.split("/")[2],
		"status": 'VALID', "physics_group_name": 'NoGroup', "physics_group_convener": 'NO_CONVENOR',
	})
	newElement(nodeProc, "path", {"dataset_path": newPath})
	for tier in newPath.split("/")[3].split("-"):
		newElement(nodeProc, "data_tier", {"name": tier})
	for cfgHash in utils.unique(map(lambda x: outputData[x][DBS.CONFIGHASH], lfns)):
		configData[cfgHash]
		newElement(nodeProc, "algorithm", {"app_version": configData[cfgHash][DBS.CMSSW_VER],
			"app_family_name": 'cmsRun', "app_executable_name": 'cmsRun', "ps_hash": cfgHash})

	# List dataset parents
	if opts.importParents:
		parents = getBlockParents(lfns, outputData)
		for (path, block) in parents:
			newElement(nodeDBS, "processed_dataset_parent", {"path": path})

	# Algorithm describing the job configuration
	for cfgHash in utils.unique(map(lambda x: outputData[x][DBS.CONFIGHASH], lfns)):
		cfg = configData[cfgHash]
		cfgContent = base64.encodestring(cfg[DBS.CONFIG])
		newElement(nodeDBS, "processed_dataset_algorithm", {
			"app_version": cfg[DBS.CMSSW_VER], "app_family_name": "cmsRun", "app_executable_name": "cmsRun",
			"ps_content": cfgContent, "ps_hash": cfgHash, "ps_name": cfg[DBS.CONFIGNAME],
			"ps_version": "private version", "ps_type": "user", "ps_annotation": base64.encodestring("user cfg")
		})

	# Give information about List files in block
	nodeBlock = newElement(nodeDBS, "block", {"name": fqBlock, "path": newPath,
		"size": metadata[blockKey][DBS.SIZE], "number_of_events": metadata[blockKey][DBS.EVENTS],
		"number_of_files": len(lfns), "open_for_writing": ("1", "0")[opts.doClose]})
	for se in utils.unique(map(lambda x: outputData[x][DBS.SE], lfns)):
		newElement(nodeBlock, "storage_element", {"storage_element_name": se})

	# List files in block
	for lfn in lfns:
		fileInfo = outputData[lfn]
		nodeFile = newElement(nodeDBS, "file", {"lfn": lfn,
			"queryable_meta_data": 'NOTSET', "validation_status": 'VALID', "status": 'VALID', "type": 'EDM',
			"checksum": fileInfo[DBS.CRC32], "adler32": 'NOTSET', "md5": fileInfo[DBS.MD5],
			"size": fileInfo[DBS.SIZE], "number_of_events": fileInfo[DBS.EVENTS], "block_name": fqBlock})

		# List algos in file
		cfgHash = fileInfo[DBS.CONFIGHASH]
		newElement(nodeFile, "file_algorithm", {"app_version": configData[cfgHash][DBS.CMSSW_VER],
			"app_family_name": 'cmsRun', "app_executable_name": 'cmsRun', "ps_hash": cfgHash})

		# List parents of file
		if opts.importParents:
			for parentLFN in fileInfo[DBS.PARENT_FILES]:
				newElement(nodeFile, "file_parent", {"lfn": parentLFN})

		# List lumi sections
		if not opts.doLumi:
			continue
		for (run, lumi) in fileInfo[DBS.LUMI]:
			newElement(nodeFile, "file_lumi_section", {"run_number": run, "lumi_section_number": lumi,
				"start_event_number": '0', "end_event_number": '0', "lumi_start_time": '0', "lumi_end_time": '0'
			})

	# Finish xml file
	newElement(nodeDBS, "SUCCESS")
	data = doc.toprettyxml(indent="\t").replace('<?xml version="1.0" ?>', "<?xml version='1.0' standalone='yes'?>")
	return (blockName, data)


def createDbsBlockDumps(opts, datasets, metadata, datasetPaths, outputData, configData):
	produced = []
	selectedDS = filter(lambda x: not opts.dataset or x in opts.dataset, datasets)
	todo = sum(map(lambda x: len(datasets[x]), selectedDS))
	for datasetKey in selectedDS:
		xmlDSPath = os.path.join(opts.xmlPath, datasetKey)
		if not os.path.exists(xmlDSPath):
			os.mkdir(xmlDSPath)

		idx, log = (0, None)
		newPath = datasetPaths[datasetKey]
		for blockInfo in datasets[datasetKey].items():
			del log
			log = utils.ActivityLog(' * Creating DBS dump files - [%d / %d]' % (idx, todo))
			(name, data) = createDbsBlockDump(opts, newPath, blockInfo, metadata, outputData, configData)
			dumpFile =  os.path.join(xmlDSPath, "%s.xml" % name)
			fp = open(dumpFile, "w")
			produced.append((newPath, blockInfo[0], dumpFile, blockInfo[1]))
			fp.write(data)
			fp.close()
		del log
	print " * Creating DBS dump files - done"
	return produced


def displayDatasetInfos(keys, datasets, metadata, paths):
	def fmtNum(x):
		return locale.format("%d", x, True)
	print 53*"=", "\n"
	if keys.lower() == "all":
		keys = str.join(",", datasets.keys())
	for dataKey in map(str.strip, keys.split(",")):
		if dataKey in datasets:
			# Display dataset overview
			print " * %s: %s" % ("Dataset key", dataKey)
			print "   %s: %s" % ("Dataset path", paths[dataKey])
			print "   %s events - %s files - %s bytes " % tuple(map(fmtNum,
				[metadata[dataKey][DBS.EVENTS],
				sum(map(len, datasets[dataKey].values())), metadata[dataKey][DBS.SIZE]]))
			print

			# Display block infos
			for bKey in datasets[dataKey]:
				lfns = datasets[dataKey][bKey]
				uuid = '%s-%s-%s-%s-%s' % (bKey[:8], bKey[8:12], bKey[12:16], bKey[16:20], bKey[20:])
				print "    * Block:", uuid
				print "      %s events - %s files - %s bytes " % tuple(map(fmtNum,
					[metadata[bKey][DBS.EVENTS], len(lfns), metadata[bKey][DBS.SIZE]]))
				print "      Location:",
				print str.join(", ", utils.unique(map(lambda x: outputData[x][DBS.SE], lfns)))
			print
		else:
			print "Config hash %s not found!" % dataKey


def displayConfigInfos(keys, configData):
	if keys.lower() == "all":
		keys = str.join(",", configData.keys())
	for cfgHash in map(str.strip, keys.split(",")):
		cfg = configData.get(cfgHash, None)
		if cfg:
			print 53*"="
			print "%15s: %s" % ("Config hash", cfgHash)
			print "%15s: %s" % ("File name", cfg[DBS.CONFIGNAME])
			print "%15s: %s" % ("CMSSW version", cfg[DBS.CMSSW_VER])
			print 53*"-"
			sys.stdout.write(cfg[DBS.CONFIG])
			print
		else:
			print "Config hash %s not found!" % cfgHash


def determineDatasetPaths(opts, taskId, datasets, outputData, configData):
	if opts.dbsPath:
		dbsUserPath = map(str.strip, opts.dbsPath.split(","))
		if len(datasets) == len(dbsUserPath):
			dbsUserPath = dict(zip(datasets.keys(), dbsUserPath))
		elif len(datasets) < len(dbsUserPath):
			raise RuntimeError("Too many dataset name(s) supplied")
		elif len(dbsUserPath) > 1:
			raise RuntimeError("Invalid dataset name(s) supplied")
		else:
			if dbsUserPath[0].strip("/").count("/") == 1:
				# Increment processed dataset number to avoid collisions
				tmp = map(lambda x: dbsUserPath[0] + "_%02x" % x, range(len(datasets.keys())))
				dbsUserPath = dict(zip(datasets.keys(), tmp))
			else:
				dbsUserPath = dict.fromkeys(datasets.keys(), dbsUserPath[0])
	else:
		dbsUserPath = dict.fromkeys(datasets.keys(), None)
	# dbsUserPath contains mapping between datasetKeys and user input

	def getPathComponents(path):
		if path:
			return tuple(path.strip("/").split("/"))
		else:
			return ()

	datasetPaths = {}
	for (dataKey, userPath) in dbsUserPath.items():
		def getTier():
			# Look into first file of block to determine data tier
			try:
				lfn = datasets[dataKey].items()[0][1][0]
				cfgData = configData[outputData[lfn][DBS.CONFIGHASH]][DBS.CONFIG]
				regex = re.compile('.*dataTier.*=.*cms.untracked.string.*\((.*)\)')
				result = regex.search(cfgData).group(1).strip('\"\' ')
				if result == "":
					raise
				return result
			except:
				pass
			return "USER"

		def rndProcName():
			# Create a new processed dataset name
			return "Dataset_%s_%s" % (taskId, dataKey[:16])

		# In case of a child dataset, use the parent infos to construct new path
		parents = getBlockParents(datasets[dataKey].items()[0][1], outputData)
		userPath = getPathComponents(userPath)
		(primary, processed, tier) = (None, None, None)

		if len(parents) > 0:
			(primary, processed, tier) = getPathComponents(parents[0][0])
			if len(userPath) == 3:
				(primary, processed, tier) = userPath
		elif len(userPath) > 0:
			primary = userPath[0]
			userPath = userPath[1:]

		if len(userPath) == 2:
			(processed, tier) = userPath
		elif len(userPath) == 1:
			(processed, tier) = (userPath[0], getTier())
		elif len(userPath) == 0:
			(processed, tier) = (rndProcName(), getTier())

		if None in (primary, processed, tier):
			raise RuntimeError("Invalid dataset name(s) supplied")
		datasetPaths[dataKey] = "/%s/%s/%s" % (primary, processed, tier)
	return datasetPaths


# Currently depends on the whole dataset being registered at the DBS instance
def registerParent(opts, parentPath):
	# Check the existance of an dataset
	def hasDataset(url, dataset):
		try:
			api = MakeDBSApi(url)
			return len(api.listBlocks(dataset)) > 0
		except DbsBadRequest:
			return False

	if not hasDataset(opts.dbsTarget, parentPath):
		# Parent dataset has to be moved to target dbs instance
		text = ' * Migrating dataset parents... %s (This can take a lot of time!)' % parentPath
		log = utils.ActivityLog(text)
		try:
			quiet = gcSupport.Silencer()
			for dbsSourceSelected in map(str.strip, opts.dbsSource.split(",")):
				if hasDataset(dbsSourceSelected, parentPath):
					(sApi, tApi) = map(MakeDBSApi, (dbsSourceSelected, opts.dbsTarget))
					DbsMigrateApi(sApi, tApi, force=True, pBranches=True).migratePath(parentPath)
			del quiet
		except:
			del quiet
			raise RuntimeError("Could not migrate dataset to target!")
		del log
		print " * Migrating parents of dataset - done"


# Check whether to insert block into DBS or not
def xmlChanged(xmlDumpInfo):
	(dsName, blockName, xmlFile, lfns) = xmlDumpInfo
	return True


# Register datasets at dbs instance
def registerDataset(opts, dsName, blockName, xmlFile, lfns):
	print "   * %s#%s" % (dsName, blockName)
	log = utils.ActivityLog(" * Importing dataset file... %s" % os.path.basename(xmlFile))
	fp = open(xmlFile)
	try:
		MakeDBSApi(opts.dbsTarget).insertDatasetContents(fp.read())
		# Mark registered files
		dbsLog = utils.PersistentDict(os.path.join(opts.workDir, 'dbs.log'), ' = ', False)
		dbsLog.write(dict.fromkeys(lfns, int(time.time())))
		del log
		fp.close()
		return True
	except DbsException, e:
		print "   ! Could not import %s/%s" % (dsName, blockName)
		errorMsg = e.getErrorMessage()
		errorPath = xmlFile.replace(".xml", ".log")
		for msg in errorMsg.split("\n"):
			if str(msg) == "":
				break
			print "   ! %s" % msg
		print "   ! The complete error log can be found in:\n   ! %s" % errorPath
		open(errorPath, "w").write(errorMsg)
	fp.close()
	return False


def print_help(*args):
	sys.stderr.write("Syntax: %s [OPTIONS] <work directory>\n" % sys.argv[0])
	sys.exit(0)


try:
	locale.setlocale(locale.LC_ALL, "")

	parser = optparse.OptionParser()#add_help_option=False)
#	parser.add_option("-h", "--help",            action="callback",    callback=print_help)
	parser.add_option("-b", "--batch",           dest="batch",         default=False, action="store_true",
		help="Enable non-interactive batch mode [Default: Interactive mode]")
	parser.add_option("-o", "--open-blocks",     dest="doClose",       default=True,  action="store_false",
		help="Keep blocks open for addition of further files [Default: Close blocks]")
	parser.add_option("-l", "--lumi",            dest="doLumi",        default=False, action="store_true",
		help="Include lumi section information [Default: False]")
	parser.add_option("-p", "--no-parents",      dest="importParents", default=True,  action="store_false",
		help="Disable import of parent datasets into target DBS instance [Default: Import parents]")
	parser.add_option("-i", "--no-import",       dest="doImport",      default=True,  action="store_false",
		help="Disable import of new datasets into target DBS instance [Default: Import datasets]")
	parser.add_option("-m", "--merge",           dest="doMerge",       default=False,  action="store_true",
		help="Merge output files from different blocks into a single block [Default: Keep boundaries]")
	parser.add_option("-r", "--incremental",     dest="incremental",   default=False,  action="store_true",
		help="Disable import of new datasets into target DBS instance [Default: Import datasets]")
	parser.add_option("-t", "--target-instance", dest="dbsTarget",
#		default="http://grid-dcms1.physik.rwth-aachen.de:8081/cms_dbs_prod_local/servlet/DBSServlet"
#		default="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet",
		default="http://ekpcms2.physik.uni-karlsruhe.de:8080/DBS/servlet/DBSServlet",
		help="Specify target dbs instance url")
	parser.add_option("-s", "--source-instance", dest="dbsSource",
		default="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet",
		help="Specify source dbs instance url(s), where parent datasets are taken from")
	parser.add_option("-n", "--name",            dest="dbsPath",       default=None,
		help="Specify dbs path name(s)")
	parser.add_option("-d", "--dataset",         dest="dataset",       default=None,
		help="Specify dataset(s) to process")
	parser.add_option("-D", "--display-dataset", dest="display_data",  default=None,
		help="Display information associated with dataset key(s) (accepts 'all')")
	parser.add_option("-C", "--display-config",  dest="display_cfg",   default=None,
		help="Display information associated with config hash(es) (accepts 'all')")
	(opts, args) = parser.parse_args()

	# Get work directory, create dbs dump directory
	if len(args) != 1:
		sys.stderr.write("Work directory not specified!\n")
		sys.stderr.write("Syntax: %s [OPTIONS] <work directory>\n" % sys.argv[0])
		sys.stderr.write("Use --help to get a list of options!\n")
		sys.exit(0)
	opts.workDir = os.path.abspath(os.path.normpath(args[0]))
	opts.xmlPath = os.path.join(opts.workDir, "dbs")
	if not os.path.exists(opts.xmlPath):
		os.mkdir(opts.xmlPath)

	# Lock file in case several instances of this program are running
	mutex = gcSupport.FileMutex(os.path.join(opts.xmlPath, 'datasetDBSAdd.lock'))
	# Read comprehensive output information
	(tid, datasets, metadata, outputData, configData) = getOutputDatasets(opts)
	if len(datasets) == 0:
		raise RuntimeError("There aren't any datasets left to process")

	# Display config hash information
	if opts.display_cfg:
		displayConfigInfos(opts.display_cfg, configData)
		sys.exit(0)

	# Determine dataset names
	datasetPaths = determineDatasetPaths(opts, tid, datasets, outputData, configData)

	# Display dataset information
	if opts.display_data:
		displayDatasetInfos(opts.display_data, datasets, metadata, datasetPaths)
		sys.exit(0)

	if len(datasetPaths) != len(map(lambda (x,y): (y,x), datasetPaths.items())):
		raise RuntimeError("The same dataset path was assigned to several datasets.")

	# Go over the selected datasets and write out the xml dump
	xmlDumps = createDbsBlockDumps(opts, datasets, metadata, datasetPaths, outputData, configData)

	# Import any parent datasets needed by the new datasets
	if opts.doImport and opts.importParents:
		parents = {}
		os.chdir(opts.xmlPath)
		for dataKey in datasets:
			for (blockKey, lfns) in datasets[dataKey].items():
				parents.update(dict(getBlockParents(lfns, outputData)))
		if len(parents) > 0:
			print " * The following parents will be needed at the target dbs instance:"
			print str.join("", map(lambda x: "   * %s\n" % x, parents.keys())),
			if not (opts.batch or utils.boolUserInput(" * Register needed parents?", True)):
				sys.exit(0)
			for parent in parents.keys():
				registerParent(opts, parent)

	# Insert blocks into DBS
	if opts.doImport:
		todoList = filter(xmlChanged, xmlDumps)
		if len(todoList) == 0:
			print " * Nothing to do..."
			sys.exit(0)
		print
		print " => The following datasets will be imported into the target dbs instance:"
		for dsName in utils.unique(map(lambda x: x[0], todoList)):
			print "     * \33[0;91m%s\33[0m" % dsName
			for (d1, blockName, d2, d3) in filter(lambda x: x[0] == dsName, todoList):
				print "       Block \33[0;94m%s\33[0m" % blockName
		if not (opts.batch or utils.boolUserInput(" * Start dataset import?", True)):
			sys.exit(0)
		fail = False
		for (dsName, blockName, xmlFile, lfns) in todoList:
			if not registerDataset(opts, dsName, blockName, xmlFile, lfns):
				fail = True
		if fail:
			print " * Importing datasets - failed"
		else:
			print " * Importing datasets - done"

	del mutex
except GridError, e:
	e.showMessage()
	del mutex
