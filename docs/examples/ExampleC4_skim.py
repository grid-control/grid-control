import FWCore.ParameterSet.Config as cms
process = cms.Process('minimal')
process.TFileService = cms.Service('TFileService', fileName = cms.string('output.root'))
process.GlobalTag.globaltag = '@GLOBALTAG@'
