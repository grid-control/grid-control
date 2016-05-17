import FWCore.ParameterSet.Config as cms

process = cms.Process('Analysis')
process.source = cms.Source('PoolSource',
	fileNames = cms.untracked.vstring('/store/data/Run2012B/JetHT/AOD/13Jul2012-v1/00000/0035A62D-BED2-E111-9F65-0018F3D096C8.root')
)
@IFEQ@SERVICE@enabled@process.TFileService = cms.Service('TFileService', fileName = cms.string('service.root'))
process.analysis = cms.EDAnalyzer('PFJetPlotsExample',
	JetAlgorithm  = cms.string('ak5PFJets'),
	HistoFileName = cms.string('ak5PFJets.root'),
	NJets         = cms.int32(2)
)
@IF@POOLOUTPUT@process.output = cms.OutputModule('PoolOutputModule', fileName = cms.untracked.string('output.root'))
@IF@GLOBALTAG@process.GlobalTag.globaltag = '@GLOBALTAG@'
process.p = cms.Path(process.analysis)
