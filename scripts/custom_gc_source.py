import FWCore.ParameterSet.Config as cms

def customise(process):

	process.maxEvents = cms.untracked.PSet(
		input = cms.untracked.int32(__MAX_EVENTS__)
	)

	process.source = cms.Source("PoolSource",
		skipEvents = cms.untracked.uint32(__SKIP_EVENTS__),
		fileNames = cms.untracked.vstring(__FILE_NAMES__)
	)

	process.RandomNumberGeneratorService.generator = cms.PSet(
		initialSeed = cms.untracked.uint32(__SEED_ALT__),
		engineName = cms.untracked.string('HepJamesRandom')
	)

	return (process)

process = customize(process)
