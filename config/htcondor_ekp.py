{
	# Identifier when reporting about this pool
	'NiceName' : 'EKP HTCondor',
	# URIs for contacting User Pool schedulers ("submit nodes")
	'ScheddURIs' : [
		'ssh://ekpcms6.ekpplus.cluster.de:24',
		'ssh://ekpcms6.physik.uni-karlsruhe.de:24'
	],
	# Pool configuration for issuing job requests
	'jobFeatureMap' : {
		'denySite'     : '+REFUSED_Sites',
		'wantSite'     : '+DESIRED_Sites',
		'wantWallTime' : '+DESIRED_Walltime',
		'wantSE'       : '+DESIRED_SEs',
	},
	# Pool configuration for information in the queue
	'queueQueryMap' : {
		'Queue' : [ 'MATCH_EXP_JOB_GLIDEIN_Entry_Name' ],
	},
}
