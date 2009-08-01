import FWCore.ParameterSet.Config as cms
from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper

def customise_for_gc(process):

	try:
		maxevents = __MAX_EVENTS__
	except:
		maxevents = -1

	process.maxEvents = cms.untracked.PSet(
		input = cms.untracked.int32(maxevents)
	)

	try:
		tmp = __SKIP_EVENTS__
		process.source = cms.Source("PoolSource",
			skipEvents = cms.untracked.uint32(__SKIP_EVENTS__),
			fileNames = cms.untracked.vstring(__FILE_NAMES__)
		)
	except:
		pass

	if hasattr(process, "RandomNumberGeneratorService"):
		randSvc = RandomNumberServiceHelper(process.RandomNumberGeneratorService)
		randSvc.populate()

	return (process)

process = customise_for_gc(process)
