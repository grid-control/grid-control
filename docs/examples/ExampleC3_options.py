import FWCore.ParameterSet.Config as cms

process = cms.Process('minimal')
process.load('FWCore.MessageService.MessageLogger_cfi')
process.load('Configuration/StandardSequences/Services_cff')
process.source = cms.Source('PoolSource',
	fileNames = cms.untracked.vstring('/store/mc/Phys14DR/TT_Tune4C_13TeV-pythia8-tauola/MINIAODSIM/PU20bx25_tsg_PHYS14_25_V1-v1/00000/18EAB3D4-B470-E411-9F8A-0025905A609E.root')
)

# Check command line options
from FWCore.ParameterSet.VarParsing import VarParsing
options = VarParsing ('analysis')
options.register('events', '', VarParsing.multiplicity.singleton, VarParsing.varType.int, 'Events')
options.parseArguments()
print(options.events)
