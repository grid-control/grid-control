import FWCore.ParameterSet.Config as cms
import random

def customise(process):

	try:
		maxevents = __MAX_EVENTS__
	except:
		maxevents = -1
		print "__MAX_EVENTS__ not specified!"

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
		process.RandomNumberGeneratorService.generator = cms.PSet(
			initialSeed = cms.untracked.uint32(random.randint(0, 900000000 - 1)),
			engineName = cms.untracked.string('HepJamesRandom')
		)

	return (process)

process = customise(process)
