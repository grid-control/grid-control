grid-control options
====================

global options
--------------

  * ``config id`` = <text> (default: <config file name w/o extension> or 'unnamed')
    Identifier for the current configuration
  * ``delete`` = <job selector> (default: '')
    The unfinished jobs selected by this expression are cancelled.
  * ``plugin paths`` = <list of paths> (default: ['<current directory>'])
    Specifies paths that are used to search for plugins
  * ``reset`` = <job selector> (default: '')
    The jobs selected by this expression are reset to the INIT state
  * ``workdir`` = <path> (default: <workdir base>/work.<config file name>)
    Location of the grid-control work directory. Usually based on the name of the config file
  * ``workdir base`` = <path> (default: <config file path>)
    Directory where the default workdir is created
  * ``workdir create`` = <boolean> (default: True)
    Skip interactive question about workdir creation
  * ``workdir space`` = <integer> (default: 10)
    Lower space limit in the work directory. Monitoring can be deactived with 0
  * ``workflow`` = <plugin[:name]> (default: 'Workflow:global')
    Specifies the workflow that is being run

Workflow options
----------------

  * ``task / module`` = <plugin[:name]>
    Select the task module to run
  * ``action`` = <list of values> (default: 'check retrieve submit')
    Specify the actions and the order in which grid-control should perform them
  * ``backend`` = <list of plugin[:name] ...> (default: 'grid')
    Select the backend to use for job submission
  * backend manager = <plugin> (Default: 'MultiWMS')
    Specifiy compositor class to merge the different plugins given in ``backend``
  * ``continuous`` = <boolean> (default: False)
    Enable continuous running mode
  * ``duration`` = <duration hh[:mm[:ss]]> (default: <continuous mode on: infinite (-1), off: exit immediately (0)>)
    Maximal duration of the job processing pass. The default depends on the value of the 'continuous' option.
  * ``gui`` = <plugin> (default: 'SimpleConsole')
    Specify GUI plugin to handle the user interaction
  * ``job manager`` = <plugin[:name]> (default: 'SimpleJobManager')
    Specify the job management plugin to handle the job cycle
  * ``monitor`` = <list of plugin[:name] ...> (default: 'scripts')
    Specify monitor plugins to track the task / job progress
  * monitor manager = <plugin> (Default: 'MultiMonitor')
    Specifiy compositor class to merge the different plugins given in ``monitor``
  * ``submission`` = <boolean> (default: True)
    Toggle to control the submission of jobs

SimpleJobManager options
------------------------

  * ``abort report`` = <text> (default: 'LocationReport')
    Specify report plugin to display in case of job cancellations
  * ``chunks check`` = <integer> (default: 100)
    Specify maximal number of jobs to check in each job cycle
  * ``chunks enabled`` = <boolean> (default: True)
    Toggle to control if only a chunk of jobs are processed each job cycle
  * ``chunks retrieve`` = <integer> (default: 100)
    Specify maximal number of jobs to retrieve in each job cycle
  * ``chunks submit`` = <integer> (default: 100)
    Specify maximal number of jobs to submit in each job cycle
  * ``defect tries / kick offender`` = <integer> (default: 10)
    Threshold for dropping jobs causing status retrieval errors
  * ``in flight`` = <integer> (default: no limit (-1))
    Maximum number of concurrently submitted jobs
  * ``in queue`` = <integer> (default: no limit (-1))
    Maximum number of queued jobs
  * ``job database`` = <plugin> (default: 'JobDB')
    Specify job database plugin that is used to store job information
  * ``jobs`` = <integer> (default: no limit (-1))
    Maximum number of jobs (truncated to task maximum)
  * ``max retry`` = <integer> (default: no limit (-1))
    Number of resubmission attempts for failed jobs
  * ``output processor`` = <plugin> (default: 'SandboxProcessor')
    Specify plugin that processes the output sandbox of successful jobs
  * ``queue timeout`` = <duration hh[:mm[:ss]]> (default: disabled (-1))
    Resubmit jobs after staying some time in initial state
  * ``selected`` = <text> (default: '')
    Apply general job selector
  * ``shuffle`` = <boolean> (default: False)
    Submit jobs in random order
  * ``verify chunks`` = <list of values> (default: '-1')
    List of job chunk sizes that are enabled after passing the configured verification thresholds
  * ``verify threshold / verify reqs`` = <list of values> (default: '0.5')
    List of job verification thresholds that enable the configured job chunk sizes

UserTask options
----------------

  * ``wall time`` = <duration hh[:mm[:ss]]>
    Requested wall time also used for checking the proxy lifetime
  * ``cpu time`` = <duration hh[:mm[:ss]]> (default: <wall time>)
    Requested cpu time
  * ``cpus`` = <integer> (default: 1)
    Requested number of cpus per node
  * ``depends`` = <list of values> (default: '')
    List of environment setup scripts that the jobs depend on
  * ``gzip output`` = <boolean> (default: True)
    Toggle the compression of the job log files for stdout and stderr
  * ``input files`` = <list of paths> (default: [])
    List of files that should be transferred to the landing zone of the job on the worker node
  * ``job name generator`` = <plugin> (default: 'DefaultJobName')
    Specify the job name plugin that generates the job name that is given to the backend
  * ``landing zone space left`` = <integer> (default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the landing zone directory while running
  * ``landing zone space used`` = <integer> (default: 100)
    Maximum amount of disk space (in MB) that the job is allowed to use in the landing zone directory while running
  * ``memory`` = <integer> (default: unspecified (-1))
    Requested memory in MB. Some batch farms have very low default memory limits in which case it is necessary to specify this option!
  * ``node timeout`` = <duration hh[:mm[:ss]]> (default: disabled (-1))
    Cancel job after some time on worker node
  * ``output files`` = <list of values> (default: '')
    List of files that should be transferred to the job output directory on the submission machine
  * ``parameter factory`` = <plugin[:name]> (default: 'SimpleParameterFactory')
    Specify the parameter factory plugin that is used to generate the parameter space of the task
  * ``scratch space left`` = <integer> (default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply
  * ``scratch space used`` = <integer> (default: 5000)
    Maximum amount of disk space (in MB) that the job is allowed to use in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply
  * ``se min size`` = <integer> (default: disabled (-1))
    SE output files below this file size trigger a job failure
  * ``subst files`` = <list of values> (default: '')
    List of files that will be subjected to variable substituion
  * ``task date`` = <text> (default: current date: YYYY-MM-DD)
    Persistent date when the task was started.
  * ``task id`` = <text> (default: GCxxxxxxxxxxxx)
    Persistent task identifier that is generated at the start of the task

BasicWMS options
----------------

  * ``access token / proxy`` = <list of plugin[:name] ...> (default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission
  * access token manager = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``
  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``sb input manager`` = <plugin[:name]> (default: 'LocalSBStorageManager')
    Specify transfer manager plugin to transfer sandbox input files
  * ``se input manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE input files
  * ``se output manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE output files
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle

CMSSW options
-------------

  * ``wall time`` = <duration hh[:mm[:ss]]>
    Requested wall time also used for checking the proxy lifetime
  * ``area files`` = <list of values> (default: '-.* -config bin lib python module */data *.xml *.sql *.db *.cf[if] *.py -*/.git -*/.svn -*/CVS -*/work.*')
    List of files that should be taken from the CMSSW project area for running the job
  * ``arguments`` = <text> (default: '')
    Arguments that will be passed to the *cmsRun* call
  * ``config file`` = <list of paths> (default: <name:cfgDefault>)
    List of config files that will be sequentially processed by *cmsRun* calls
  * ``cpu time`` = <duration hh[:mm[:ss]]> (default: <wall time>)
    Requested cpu time
  * ``cpus`` = <integer> (default: 1)
    Requested number of cpus per node
  * ``depends`` = <list of values> (default: '')
    List of environment setup scripts that the jobs depend on
  * ``events per job`` = <text> (default: '0')
    This sets the variable MAX_EVENTS if no datasets are present
  * ``gzip output`` = <boolean> (default: True)
    Toggle the compression of the job log files for stdout and stderr
  * ``input files`` = <list of paths> (default: [])
    List of files that should be transferred to the landing zone of the job on the worker node
  * ``instrumentation`` = <boolean> (default: True)
    Toggle to control the instrumentation of CMSSW config files for running over data / initializing the RNG for MC production
  * ``instrumentation fragment`` = <path> (default: <grid-control cms package>/share/fragmentForCMSSW.py)
    Path to the instrumentation fragment that is appended to the CMSSW config file if instrumentation is enabled
  * ``job name generator`` = <plugin> (default: 'DefaultJobName')
    Specify the job name plugin that generates the job name that is given to the backend
  * ``landing zone space left`` = <integer> (default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the landing zone directory while running
  * ``landing zone space used`` = <integer> (default: 100)
    Maximum amount of disk space (in MB) that the job is allowed to use in the landing zone directory while running
  * ``memory`` = <integer> (default: unspecified (-1))
    Requested memory in MB. Some batch farms have very low default memory limits in which case it is necessary to specify this option!
  * ``node timeout`` = <duration hh[:mm[:ss]]> (default: disabled (-1))
    Cancel job after some time on worker node
  * ``output files`` = <list of values> (default: '')
    List of files that should be transferred to the job output directory on the submission machine
  * ``parameter factory`` = <plugin[:name]> (default: 'SimpleParameterFactory')
    Specify the parameter factory plugin that is used to generate the parameter space of the task
  * ``project area`` = <path> (default: <depends on ``scram arch`` and ``scram project``>)
    Specify location of the CMSSW project area that should be send with the job. Instead of the CMSSW project area, it is possible to specify ``scram arch`` and ``scram project`` to use a fresh CMSSW project.
  * ``scram arch`` = <text> (default: <depends on ``project area``>)
    Specify scram architecture that should be used by the job (eg. 'slc7_amd64_gcc777'). When using an existing CMSSW project area with ``project area``, this option uses the default value taken from the project area.
  * ``scram project`` = <list of values> (default: '')
    Specify scram project that should be used by the job (eg. 'CMSSW CMSSW_9_9_9')
  * ``scram version`` = <text> (default: 'scramv1')
    Specify scram version that should be used by the job.
  * ``scratch space left`` = <integer> (default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply
  * ``scratch space used`` = <integer> (default: 5000)
    Maximum amount of disk space (in MB) that the job is allowed to use in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply
  * ``se min size`` = <integer> (default: disabled (-1))
    SE output files below this file size trigger a job failure
  * ``se project area / se runtime`` = <boolean> (default: True)
    Toggle to specify how the CMSSW project area should be transferred to the worker node
  * ``software requirements`` = <boolean> (default: True)
    Toggle the inclusion of scram software tags into the job requirements
  * ``subst files`` = <list of values> (default: '')
    List of files that will be subjected to variable substituion
  * ``task date`` = <text> (default: current date: YYYY-MM-DD)
    Persistent date when the task was started.
  * ``task id`` = <text> (default: GCxxxxxxxxxxxx)
    Persistent task identifier that is generated at the start of the task
  * ``vo software dir / cmssw dir`` = <text> (default: '')
    This option allows to override of the VO_CMS_SW_DIR environment variable

CMSSW_Advanced options
----------------------

  * ``wall time`` = <duration hh[:mm[:ss]]>
    Requested wall time also used for checking the proxy lifetime
  * ``area files`` = <list of values> (default: '-.* -config bin lib python module */data *.xml *.sql *.db *.cf[if] *.py -*/.git -*/.svn -*/CVS -*/work.*')
    List of files that should be taken from the CMSSW project area for running the job
  * ``arguments`` = <text> (default: '')
    Arguments that will be passed to the *cmsRun* call
  * ``config file`` = <list of paths> (default: <name:cfgDefault>)
    List of config files that will be sequentially processed by *cmsRun* calls
  * ``cpu time`` = <duration hh[:mm[:ss]]> (default: <wall time>)
    Requested cpu time
  * ``cpus`` = <integer> (default: 1)
    Requested number of cpus per node
  * ``depends`` = <list of values> (default: '')
    List of environment setup scripts that the jobs depend on
  * ``events per job`` = <text> (default: '0')
    This sets the variable MAX_EVENTS if no datasets are present
  * ``gzip output`` = <boolean> (default: True)
    Toggle the compression of the job log files for stdout and stderr
  * ``input files`` = <list of paths> (default: [])
    List of files that should be transferred to the landing zone of the job on the worker node
  * ``instrumentation`` = <boolean> (default: True)
    Toggle to control the instrumentation of CMSSW config files for running over data / initializing the RNG for MC production
  * ``instrumentation fragment`` = <path> (default: <grid-control cms package>/share/fragmentForCMSSW.py)
    Path to the instrumentation fragment that is appended to the CMSSW config file if instrumentation is enabled
  * ``job name generator`` = <plugin> (default: 'DefaultJobName')
    Specify the job name plugin that generates the job name that is given to the backend
  * ``landing zone space left`` = <integer> (default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the landing zone directory while running
  * ``landing zone space used`` = <integer> (default: 100)
    Maximum amount of disk space (in MB) that the job is allowed to use in the landing zone directory while running
  * ``memory`` = <integer> (default: unspecified (-1))
    Requested memory in MB. Some batch farms have very low default memory limits in which case it is necessary to specify this option!
  * ``nickname config`` = <lookup specifier> (default: {})
    Allows to specify a dictionary with list of config files that will be sequentially processed by *cmsRun* calls. The dictionary key is the job dependent dataset nickname
  * ``nickname constants`` = <list of values> (default: '')
    Allows to specify a list of nickname dependent variables. The value of the variables is specified separately in the form of a dictionary. (This option is deprecated, since *all* variables support this functionality now!)
  * ``nickname lumi filter`` = <dictionary> (default: {})
    Allows to specify a dictionary with nickname dependent lumi filter expressions. (This option is deprecated, since the normal option ``lumi filter`` already supports this!)
  * ``node timeout`` = <duration hh[:mm[:ss]]> (default: disabled (-1))
    Cancel job after some time on worker node
  * ``output files`` = <list of values> (default: '')
    List of files that should be transferred to the job output directory on the submission machine
  * ``parameter factory`` = <plugin[:name]> (default: 'SimpleParameterFactory')
    Specify the parameter factory plugin that is used to generate the parameter space of the task
  * ``project area`` = <path> (default: <depends on ``scram arch`` and ``scram project``>)
    Specify location of the CMSSW project area that should be send with the job. Instead of the CMSSW project area, it is possible to specify ``scram arch`` and ``scram project`` to use a fresh CMSSW project.
  * ``scram arch`` = <text> (default: <depends on ``project area``>)
    Specify scram architecture that should be used by the job (eg. 'slc7_amd64_gcc777'). When using an existing CMSSW project area with ``project area``, this option uses the default value taken from the project area.
  * ``scram project`` = <list of values> (default: '')
    Specify scram project that should be used by the job (eg. 'CMSSW CMSSW_9_9_9')
  * ``scram version`` = <text> (default: 'scramv1')
    Specify scram version that should be used by the job.
  * ``scratch space left`` = <integer> (default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply
  * ``scratch space used`` = <integer> (default: 5000)
    Maximum amount of disk space (in MB) that the job is allowed to use in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply
  * ``se min size`` = <integer> (default: disabled (-1))
    SE output files below this file size trigger a job failure
  * ``se project area / se runtime`` = <boolean> (default: True)
    Toggle to specify how the CMSSW project area should be transferred to the worker node
  * ``software requirements`` = <boolean> (default: True)
    Toggle the inclusion of scram software tags into the job requirements
  * ``subst files`` = <list of values> (default: '')
    List of files that will be subjected to variable substituion
  * ``task date`` = <text> (default: current date: YYYY-MM-DD)
    Persistent date when the task was started.
  * ``task id`` = <text> (default: GCxxxxxxxxxxxx)
    Persistent task identifier that is generated at the start of the task
  * ``vo software dir / cmssw dir`` = <text> (default: '')
    This option allows to override of the VO_CMS_SW_DIR environment variable

logging options
---------------

  * ``<name:logger_name> file`` = <text>
  * ``<logger name> <handler> code context / <logger name> code context`` = <integer> (default: 2)
  * ``<logger name> <handler> file stack / <logger name> file stack`` = <integer> (default: 1)
  * ``<logger name> <handler> format / <logger name> format`` = <text> (default: '$(message)s')
  * ``<logger name> <handler> variables / <logger name> variables`` = <integer> (default: 1)
  * ``<name:logger_name> debug file`` = <text> (default: '')
  * ``<name:logger_name> handler`` = <list of values> (default: '')
  * ``<name:logger_name> level`` = <enum> (default: <attr:level>)
  * ``<name:logger_name> propagate`` = <boolean> (default: <call:bool(<attr:propagate>)>)
  * ``debug mode`` = <boolean> (default: False)
  * ``display logger`` = <boolean> (default: False)

DataProvider.bind options
-------------------------

  * ``dataset provider`` = <text> (default: 'ListProvider')

ParameterConfig.get options
---------------------------

  * ``<call:self.getopt(<name:var>, <name:opt>)>`` = <text> (default: <name:default>)

changeInitNeeded.__call__ options
---------------------------------

  * ``<attr:option>`` = <boolean> (default: <name:interaction_def>)
  * ``default`` = <boolean> (default: True)

ConfigDataProvider._readFileFromConfig options
----------------------------------------------

  * ``<name:url>`` = <text>

ConfigDataProvider._readBlockFromConfig options
-----------------------------------------------

  * ``events`` = <integer> (default: -1)
  * ``id`` = <integer> (default: <name:datasetID>)
  * ``metadata`` = <text> (default: '[]')
  * ``metadata common`` = <text> (default: '[]')
  * ``nickname`` = <text> (default: <name:datasetNick>)
  * ``prefix`` = <text> (default: '')
  * ``se list`` = <text> (default: '')

TaskExecutableWrapper options
-----------------------------

  * ``<name:prefix> arguments`` = <text> (default: '')
  * ``<name:prefix> executable`` = <text> (default: <name:exeDefault>)
  * ``<name:prefix> send executable`` = <boolean> (default: True)

validNoVar options
------------------

  * ``variable markers`` = <list of values> (default: '@ __')
    Specifies how variables are marked

DataTask.setupJobParameters options
-----------------------------------

  * ``dataset`` = <list of plugins> (default: '')
  * dataset manager = <plugin> (Default: ':MultiDatasetProvider:')
    Specifiy compositor class to merge the different plugins given in ``dataset``
  * ``dataset refresh`` = <duration hh[:mm[:ss]]> (default: -1)
  * ``dataset splitter`` = <text> (default: 'FileBoundarySplitter')
  * ``partition processor`` = <list of plugins> (default: 'TFCPartitionProcessor LocationPartitionProcessor MetaPartitionProcessor BasicPartitionProcessor')
  * partition processor manager = <plugin> (Default: 'MultiPartitionProcessor')
    Specifiy compositor class to merge the different plugins given in ``partition processor``

DataProvider options
--------------------

  * ``dataset processor`` = <list of plugins> (default: 'EntriesConsistencyDataProcessor URLDataProcessor URLCountDataProcessor ' 'EntriesCountDataProcessor EmptyDataProcessor UniqueDataProcessor LocationDataProcessor')
  * dataset processor manager = <plugin> (Default: 'MultiDataProcessor')
    Specifiy compositor class to merge the different plugins given in ``dataset processor``
  * ``nickname source`` = <plugin> (default: 'SimpleNickNameProducer')

GUI options
-----------

  * ``report`` = <list of plugins> (default: 'BasicReport')
    Type of report to display during operations
  * report manager = <plugin> (Default: 'MultiReport')
    Specifiy compositor class to merge the different plugins given in ``report``
  * ``report options`` = <text> (default: '')

Matcher options
---------------

  * ``<prefix> case sensitive`` = <boolean>

DataSplitter options
--------------------

  * ``resync interactive`` = <boolean> (default: False)
  * ``resync jobs`` = <enum> (default: <attr:append>)
  * ``resync metadata`` = <list of values> (default: '')
  * ``resync mode <name:meta>`` = <enum> (default: <attr:complete>)
  * ``resync mode expand`` = <enum> (default: <attr:changed>)
    Sets the resync mode for expanded files
  * ``resync mode new`` = <enum> (default: <attr:complete>)
    Sets the resync mode for new files
  * ``resync mode removed`` = <enum> (default: <attr:complete>)
    Sets the resync mode for removed files
  * ``resync mode shrink`` = <enum> (default: <attr:changed>)
    Sets the resync mode for shrunken files

EmptyDataProcessor options
--------------------------

  * ``dataset remove empty blocks`` = <boolean> (default: True)
  * ``dataset remove empty files`` = <boolean> (default: True)

URLDataProcessor options
------------------------

  * ``dataset ignore urls / dataset ignore files`` = <filter option> (default: '')

UniqueDataProcessor options
---------------------------

  * ``dataset check unique block`` = <enum> (default: <attr:abort>)
  * ``dataset check unique url`` = <enum> (default: <attr:abort>)

NickNameProducer options
------------------------

  * ``nickname check collision`` = <boolean> (default: True)
  * ``nickname check consistency`` = <boolean> (default: True)

PartitionEstimator options
--------------------------

  * ``target partitions`` = <integer> (default: -1)
  * ``target partitions per nickname`` = <integer> (default: -1)

SortingDataProcessor options
----------------------------

  * ``dataset block sort`` = <boolean> (default: False)
  * ``dataset files sort`` = <boolean> (default: False)
  * ``dataset sort`` = <boolean> (default: False)

LumiDataProcessor options
-------------------------

  * ``lumi filter`` = <lookup specifier> (default: {})
  * ``lumi keep`` = <enum> (default: <name:lumi_keep_default>)
  * ``strict lumi filter`` = <boolean> (default: True)

URLCountDataProcessor options
-----------------------------

  * ``dataset limit urls / dataset limit files`` = <integer> (default: -1)

EntriesCountDataProcessor options
---------------------------------

  * ``dataset limit entries / dataset limit events`` = <integer> (default: -1)

LocationDataProcessor options
-----------------------------

  * ``dataset location filter`` = <filter option> (default: '')

SimpleNickNameProducer options
------------------------------

  * ``nickname check collision`` = <boolean> (default: True)
  * ``nickname check consistency`` = <boolean> (default: True)
  * ``nickname full name`` = <boolean> (default: True)

InlineNickNameProducer options
------------------------------

  * ``nickname check collision`` = <boolean> (default: True)
  * ``nickname check consistency`` = <boolean> (default: True)
  * ``nickname expr`` = <text> (default: 'oldnick')

ScanProviderBase options
------------------------

  * ``dataset key select`` = <list of values> (default: '')
  * ``dataset processor`` = <list of plugins> (default: 'EntriesConsistencyDataProcessor URLDataProcessor URLCountDataProcessor ' 'EntriesCountDataProcessor EmptyDataProcessor UniqueDataProcessor LocationDataProcessor')
  * dataset processor manager = <plugin> (Default: 'MultiDataProcessor')
    Specifiy compositor class to merge the different plugins given in ``dataset processor``
  * ``nickname source`` = <plugin> (default: 'SimpleNickNameProducer')
  * ``scanner`` = <list of values> (default: <name:datasetExpr>)

CMSBaseProvider options
-----------------------

  * ``dataset processor`` = <list of plugins> (default: 'EntriesConsistencyDataProcessor URLDataProcessor URLCountDataProcessor ' 'EntriesCountDataProcessor EmptyDataProcessor UniqueDataProcessor LocationDataProcessor')
  * dataset processor manager = <plugin> (Default: 'MultiDataProcessor')
    Specifiy compositor class to merge the different plugins given in ``dataset processor``
  * ``dbs instance`` = <text> (default: '')
  * ``location format`` = <enum> (default: <attr:hostname>)
  * ``lumi filter`` = <lookup specifier> (default: {})
  * ``lumi metadata`` = <boolean> (default: <manual>)
  * ``nickname source`` = <plugin> (default: 'SimpleNickNameProducer')
  * ``only complete sites`` = <boolean> (default: True)
  * ``only valid`` = <boolean> (default: True)
  * ``phedex sites`` = <filter option> (default: '-T3_US_FNALLPC')
  * ``phedex t1 accept`` = <filter option> (default: 'T1_DE_KIT T1_US_FNAL')
  * ``phedex t1 mode`` = <enum> (default: <attr:disk>)

ConfigDataProvider options
--------------------------

  * ``dataset hash`` = <text> (default: <name:dataset_hash_new>)
  * ``dataset processor`` = <list of plugins> (default: 'EntriesConsistencyDataProcessor URLDataProcessor URLCountDataProcessor ' 'EntriesCountDataProcessor EmptyDataProcessor UniqueDataProcessor LocationDataProcessor')
  * dataset processor manager = <plugin> (Default: 'MultiDataProcessor')
    Specifiy compositor class to merge the different plugins given in ``dataset processor``
  * ``nickname source`` = <plugin> (default: 'SimpleNickNameProducer')

DBSInfoProvider options
-----------------------

  * ``dataset key select`` = <list of values> (default: '')
  * ``dataset processor`` = <list of plugins> (default: 'EntriesConsistencyDataProcessor URLDataProcessor URLCountDataProcessor ' 'EntriesCountDataProcessor EmptyDataProcessor UniqueDataProcessor LocationDataProcessor')
  * dataset processor manager = <plugin> (Default: 'MultiDataProcessor')
    Specifiy compositor class to merge the different plugins given in ``dataset processor``
  * ``discovery`` = <boolean> (default: False)
  * ``nickname source`` = <plugin> (default: 'SimpleNickNameProducer')
  * ``scanner`` = <list of values> (default: <name:datasetExpr>)

FilesFromLS options
-------------------

  * ``source directory`` = <text> (default: '.')

MatchOnFilename options
-----------------------

  * ``filename filter`` = <list of values> (default: '*.root')

FilesFromDataProvider options
-----------------------------

  * ``source dataset path`` = <text>

ParentLookup options
--------------------

  * ``merge parents`` = <boolean> (default: False)
  * ``parent keys`` = <list of values> (default: '')
  * ``parent match level`` = <integer> (default: 1)
  * ``parent source`` = <text> (default: '')

DetermineEvents options
-----------------------

  * ``events command`` = <text> (default: '')
  * ``events default`` = <integer> (default: -1)
  * ``events key`` = <text> (default: '')
  * ``events per key value`` = <text> (default: '')
  * ``key value per events`` = <text> (default: '')

MetadataFromTask options
------------------------

  * ``ignore task vars`` = <list of values> (default: <name:ignoreDef>)

AddFilePrefix options
---------------------

  * ``filename prefix`` = <text> (default: '')

MatchDelimeter options
----------------------

  * ``delimeter block key`` = <text> (default: '')
  * ``delimeter dataset key`` = <text> (default: '')
  * ``delimeter match`` = <text> (default: '')

LFNFromPath options
-------------------

  * ``lfn marker`` = <text> (default: '/store/')

OutputDirsFromConfig options
----------------------------

  * ``source config`` = <path>
  * ``source job selector`` = <text> (default: '')
  * ``workflow`` = <plugin[:name]> (default: 'Workflow:global')
    Specifies the workflow that is read from the config file

ObjectsFromCMSSW options
------------------------

  * ``include parent infos`` = <boolean> (default: False)
  * ``merge config infos`` = <boolean> (default: True)

OutputDirsFromWork options
--------------------------

  * ``source directory`` = <path>
  * ``source job selector`` = <text> (default: '')

MetadataFromCMSSW options
-------------------------

  * ``include config infos`` = <boolean> (default: False)

ConfigurableJobName options
---------------------------

  * ``job name`` = <text> (default: '@GC_TASK_ID@.@GC_JOB_ID@')

BlackWhiteMatcher options
-------------------------

  * ``<prefix> case sensitive`` = <boolean>
  * ``<prefix> mode`` = <plugin> (default: 'start')

JobManager options
------------------

  * ``abort report`` = <text> (default: 'LocationReport')
    Specify report plugin to display in case of job cancellations
  * ``chunks check`` = <integer> (default: 100)
    Specify maximal number of jobs to check in each job cycle
  * ``chunks enabled`` = <boolean> (default: True)
    Toggle to control if only a chunk of jobs are processed each job cycle
  * ``chunks retrieve`` = <integer> (default: 100)
    Specify maximal number of jobs to retrieve in each job cycle
  * ``chunks submit`` = <integer> (default: 100)
    Specify maximal number of jobs to submit in each job cycle
  * ``in flight`` = <integer> (default: no limit (-1))
    Maximum number of concurrently submitted jobs
  * ``in queue`` = <integer> (default: no limit (-1))
    Maximum number of queued jobs
  * ``job database`` = <plugin> (default: 'JobDB')
    Specify job database plugin that is used to store job information
  * ``jobs`` = <integer> (default: no limit (-1))
    Maximum number of jobs (truncated to task maximum)
  * ``max retry`` = <integer> (default: no limit (-1))
    Number of resubmission attempts for failed jobs
  * ``output processor`` = <plugin> (default: 'SandboxProcessor')
    Specify plugin that processes the output sandbox of successful jobs
  * ``queue timeout`` = <duration hh[:mm[:ss]]> (default: disabled (-1))
    Resubmit jobs after staying some time in initial state
  * ``selected`` = <text> (default: '')
    Apply general job selector
  * ``shuffle`` = <boolean> (default: False)
    Submit jobs in random order

ParameterFactory options
------------------------

  * ``parameter adapter`` = <text> (default: 'TrackedParameterAdapter')

VomsAccessToken options
-----------------------

  * ``ignore walltime`` = <boolean> (default: False)
  * ``ignore warnings`` = <boolean> (default: False)
  * ``max query time`` = <duration hh[:mm[:ss]]> (default: 300)
  * ``min lifetime`` = <duration hh[:mm[:ss]]> (default: 300)
  * ``min query time`` = <duration hh[:mm[:ss]]> (default: 1800)
  * ``proxy path`` = <text> (default: '')

AFSAccessToken options
----------------------

  * ``access refresh`` = <duration hh[:mm[:ss]]> (default: 3600)
  * ``ignore walltime`` = <boolean> (default: False)
  * ``max query time`` = <duration hh[:mm[:ss]]> (default: 300)
  * ``min lifetime`` = <duration hh[:mm[:ss]]> (default: 300)
  * ``min query time`` = <duration hh[:mm[:ss]]> (default: 1800)
  * ``tickets`` = <list of values> (default: '')

UserBroker options
------------------

  * ``<name:useropt>`` = <list of values> (default: '')
  * ``<name:useropt> entries`` = <integer> (default: 0)
  * ``<name:useropt> randomize`` = <boolean> (default: False)

FilterBroker options
--------------------

  * ``<name:useropt>`` = <filter option> (default: '')
  * ``<name:useropt> entries`` = <integer> (default: 0)
  * ``<name:useropt> randomize`` = <boolean> (default: False)

CoverageBroker options
----------------------

  * ``<name:useropt>`` = <filter option> (default: '')
  * ``<name:useropt> entries`` = <integer> (default: 0)
  * ``<name:useropt> randomize`` = <boolean> (default: False)

StorageBroker options
---------------------

  * ``<name:useropt> entries`` = <integer> (default: 0)
  * ``<name:useropt> randomize`` = <boolean> (default: False)
  * ``<name:useropt> storage access`` = <lookup specifier> (default: {})

JabberAlarm options
-------------------

  * ``source jid`` = <text>
    source account of the jabber messages
  * ``source password file`` = <path>
    path to password file of the source account
  * ``target jid`` = <text>
    target account of the jabber messages

ScriptMonitoring options
------------------------

  * ``on finish`` = <command or path> (default: '')
  * ``on output`` = <command or path> (default: '')
  * ``on status`` = <command or path> (default: '')
  * ``on submit`` = <command or path> (default: '')
  * ``script timeout`` = <duration hh[:mm[:ss]]> (default: 5)
  * ``silent`` = <boolean> (default: True)
    Do not show output of event scripts

DashBoard options
-----------------

  * ``application`` = <text> (default: 'shellscript')
  * ``dashboard timeout`` = <integer> (default: 5)
  * ``task`` = <text> (default: <manual>)
  * ``task name`` = <text> (default: '@GC_TASK_ID@_@DATASETNICK@')

BasicParameterFactory options
-----------------------------

  * ``constants`` = <list of values> (default: '')
  * ``nseeds`` = <integer> (default: 10)
    Number of random seeds to generate
  * ``parameter adapter`` = <text> (default: 'TrackedParameterAdapter')
  * ``repeat`` = <integer> (default: 1)
  * ``seeds`` = <list of values> (default: Generate <nseeds> random seeds)
    Random seeds used in the job via @SEED_j@
	@SEED_0@ = 32, 33, 34, ... for first, second, third job
	@SEED_1@ = 51, 52, 53, ... for first, second, third job

LocalSBStorageManager options
-----------------------------

  * ``<name:optdefault> path`` = <path> (default: <call:config.getWorkPath('sandbox')>)

SEStorageManager options
------------------------

  * ``<name:optdefault> path`` = <list of values> (default: '')
  * ``<name:optprefix> files`` = <list of values> (default: '')
  * ``<name:optprefix> force`` = <boolean> (default: True)
  * ``<name:optprefix> path`` = <list of values> (default: <attr:defPaths>)
  * ``<name:optprefix> pattern`` = <text> (default: '@X@')
  * ``<name:optprefix> timeout`` = <duration hh[:mm[:ss]]> (default: 7200)

ROOTTask options
----------------

  * ``executable`` = <text>
  * ``wall time`` = <duration hh[:mm[:ss]]>
    Requested wall time also used for checking the proxy lifetime
  * ``cpu time`` = <duration hh[:mm[:ss]]> (default: <wall time>)
    Requested cpu time
  * ``cpus`` = <integer> (default: 1)
    Requested number of cpus per node
  * ``depends`` = <list of values> (default: '')
    List of environment setup scripts that the jobs depend on
  * ``gzip output`` = <boolean> (default: True)
    Toggle the compression of the job log files for stdout and stderr
  * ``input files`` = <list of paths> (default: [])
    List of files that should be transferred to the landing zone of the job on the worker node
  * ``job name generator`` = <plugin> (default: 'DefaultJobName')
    Specify the job name plugin that generates the job name that is given to the backend
  * ``landing zone space left`` = <integer> (default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the landing zone directory while running
  * ``landing zone space used`` = <integer> (default: 100)
    Maximum amount of disk space (in MB) that the job is allowed to use in the landing zone directory while running
  * ``memory`` = <integer> (default: unspecified (-1))
    Requested memory in MB. Some batch farms have very low default memory limits in which case it is necessary to specify this option!
  * ``node timeout`` = <duration hh[:mm[:ss]]> (default: disabled (-1))
    Cancel job after some time on worker node
  * ``output files`` = <list of values> (default: '')
    List of files that should be transferred to the job output directory on the submission machine
  * ``parameter factory`` = <plugin[:name]> (default: 'SimpleParameterFactory')
    Specify the parameter factory plugin that is used to generate the parameter space of the task
  * ``root path`` = <text> (default: <call:os.environ.get('ROOTSYS', '')>)
  * ``scratch space left`` = <integer> (default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply
  * ``scratch space used`` = <integer> (default: 5000)
    Maximum amount of disk space (in MB) that the job is allowed to use in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply
  * ``se min size`` = <integer> (default: disabled (-1))
    SE output files below this file size trigger a job failure
  * ``subst files`` = <list of values> (default: '')
    List of files that will be subjected to variable substituion
  * ``task date`` = <text> (default: current date: YYYY-MM-DD)
    Persistent date when the task was started.
  * ``task id`` = <text> (default: GCxxxxxxxxxxxx)
    Persistent task identifier that is generated at the start of the task

Local options
-------------

  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle
  * ``wms`` = <text> (default: '')

MultiWMS options
----------------

  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle
  * ``wms broker`` = <plugin[:name]> (default: 'RandomBroker')

InactiveWMS options
-------------------

  * ``access token / proxy`` = <list of plugin[:name] ...> (default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission
  * access token manager = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``
  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle

HTCondor options
----------------

  * ``access token / proxy`` = <list of plugin[:name] ...> (default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission
  * access token manager = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``
  * ``append info`` = <list of values> (default: '')
  * ``append opts`` = <list of values> (default: '')
  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``poolconfig`` = <list of values> (default: '')
  * ``sandbox path`` = <path> (default: <call:config.getWorkPath('sandbox.<name:wmsName>')>)
  * ``sb input manager`` = <plugin[:name]> (default: 'LocalSBStorageManager')
    Specify transfer manager plugin to transfer sandbox input files
  * ``schedduri`` = <text> (default: '')
  * ``se input manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE input files
  * ``se output manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE output files
  * ``universe`` = <text> (default: 'vanilla')
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle

GridWMS options
---------------

  * ``access token / proxy`` = <list of plugin[:name] ...> (default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission
  * access token manager = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``
  * ``ce`` = <text> (default: '')
  * ``config`` = <path> (default: '')
  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``sb input manager`` = <plugin[:name]> (default: 'LocalSBStorageManager')
    Specify transfer manager plugin to transfer sandbox input files
  * ``se input manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE input files
  * ``se output manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE output files
  * ``site broker`` = <plugin[:name]> (default: 'UserBroker')
  * ``vo`` = <text> (default: <call:self._token.getGroup()>)
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle
  * ``warn sb size`` = <integer> (default: 5242880)

Condor options
--------------

  * ``access token / proxy`` = <list of plugin[:name] ...> (default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission
  * access token manager = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``
  * ``classaddata`` = <list of values> (default: '')
  * ``debuglog`` = <text> (default: '')
  * ``jdldata`` = <list of values> (default: '')
  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``notifyemail`` = <text> (default: '')
  * ``poolargs query`` = <dictionary> (default: {})
  * ``poolargs req`` = <dictionary> (default: {})
  * ``poolhostlist`` = <list of values> (default: '')
  * ``remote dest`` = <text> (default: '@')
  * ``remote type`` = <enum> (default: <attr:LOCAL>)
  * ``remote user`` = <text> (default: '')
  * ``remote workdir`` = <text> (default: '')
  * ``sandbox path`` = <path> (default: <call:config.getWorkPath('sandbox')>)
  * ``sb input manager`` = <plugin[:name]> (default: 'LocalSBStorageManager')
    Specify transfer manager plugin to transfer sandbox input files
  * ``se input manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE input files
  * ``se output manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE output files
  * ``site broker`` = <plugin[:name]> (default: 'UserBroker')
  * ``task id`` = <text> (default: <call:md5(...).hexdigest()>)
  * ``universe`` = <text> (default: 'vanilla')
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle

GliteWMS options
----------------

  * ``access token / proxy`` = <list of plugin[:name] ...> (default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission
  * access token manager = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``
  * ``ce`` = <text> (default: '')
  * ``config`` = <path> (default: '')
  * ``discover sites`` = <boolean> (default: False)
  * ``discover wms`` = <boolean> (default: True)
  * ``force delegate`` = <boolean> (default: False)
  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``sb input manager`` = <plugin[:name]> (default: 'LocalSBStorageManager')
    Specify transfer manager plugin to transfer sandbox input files
  * ``se input manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE input files
  * ``se output manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE output files
  * ``site broker`` = <plugin[:name]> (default: 'UserBroker')
  * ``try delegate`` = <boolean> (default: True)
  * ``vo`` = <text> (default: <call:self._token.getGroup()>)
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle
  * ``warn sb size`` = <integer> (default: 5242880)
  * ``wms discover full`` = <boolean> (default: True)

CreamWMS options
----------------

  * ``access token / proxy`` = <list of plugin[:name] ...> (default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission
  * access token manager = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``
  * ``ce`` = <text> (default: '')
  * ``config`` = <path> (default: '')
  * ``job chunk size`` = <integer> (default: 10)
  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``sb input manager`` = <plugin[:name]> (default: 'LocalSBStorageManager')
    Specify transfer manager plugin to transfer sandbox input files
  * ``se input manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE input files
  * ``se output manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE output files
  * ``site broker`` = <plugin[:name]> (default: 'UserBroker')
  * ``vo`` = <text> (default: <call:self._token.getGroup()>)
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle
  * ``warn sb size`` = <integer> (default: 5242880)

PBS options
-----------

  * ``access token / proxy`` = <list of plugin[:name] ...> (default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission
  * access token manager = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``
  * ``account`` = <text> (default: '')
  * ``delay output`` = <boolean> (default: False)
  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``memory`` = <integer> (default: -1)
  * ``queue broker`` = <plugin[:name]> (default: 'UserBroker')
  * ``sandbox path`` = <path> (default: <call:config.getWorkPath('sandbox')>)
  * ``sb input manager`` = <plugin[:name]> (default: 'LocalSBStorageManager')
    Specify transfer manager plugin to transfer sandbox input files
  * ``scratch path`` = <list of values> (default: 'TMPDIR /tmp')
  * ``se input manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE input files
  * ``se output manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE output files
  * ``server`` = <text> (default: '')
  * ``shell`` = <text> (default: '')
  * ``site broker`` = <plugin[:name]> (default: 'UserBroker')
  * ``software requirement map`` = <lookup specifier> (default: {})
  * ``submit options`` = <text> (default: '')
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle

GridEngine options
------------------

  * ``access token / proxy`` = <list of plugin[:name] ...> (default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission
  * access token manager = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``
  * ``account`` = <text> (default: '')
  * ``delay output`` = <boolean> (default: False)
  * ``job parser`` = <plugin> (default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status
  * ``memory`` = <integer> (default: -1)
  * ``project name`` = <text> (default: '')
  * ``queue broker`` = <plugin[:name]> (default: 'UserBroker')
  * ``sandbox path`` = <path> (default: <call:config.getWorkPath('sandbox')>)
  * ``sb input manager`` = <plugin[:name]> (default: 'LocalSBStorageManager')
    Specify transfer manager plugin to transfer sandbox input files
  * ``scratch path`` = <list of values> (default: 'TMPDIR /tmp')
  * ``se input manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE input files
  * ``se output manager`` = <plugin[:name]> (default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE output files
  * ``shell`` = <text> (default: '')
  * ``site broker`` = <plugin[:name]> (default: 'UserBroker')
  * ``software requirement map`` = <lookup specifier> (default: {})
  * ``submit options`` = <text> (default: '')
  * ``user`` = <text> (default: <call:os.environ.get('LOGNAME', '')>)
  * ``wait idle`` = <integer> (default: 60)
    Wait for the specified duration if the job cycle was idle
  * ``wait work`` = <integer> (default: 10)
    Wait for the specified duration during the work steps of the job cycle

TrackedParameterAdapter options
-------------------------------

  * ``parameter hash`` = <text> (default: <name:pHash>)

TFCPartitionProcessor options
-----------------------------

  * ``partition tfc`` = <lookup specifier> (default: {})

MetaPartitionProcessor options
------------------------------

  * ``partition metadata`` = <list of values> (default: '')

LocationPartitionProcessor options
----------------------------------

  * ``partition location check`` = <boolean> (default: True)
  * ``partition location filter`` = <filter option> (default: '')
  * ``partition location preference`` = <list of values> (default: '')
  * ``partition location requirement`` = <boolean> (default: True)

LFNPartitionProcessor options
-----------------------------

  * ``partition lfn modifier`` = <text> (default: '')
  * ``partition lfn modifier dict`` = <dictionary> (default: {'<xrootd>': 'root://cms-xrd-global.cern.ch/', '<xrootd:eu>': 'root://xrootd-cms.infn.it/', '<xrootd:us>': 'root://cmsxrootd.fnal.gov/'})

LumiPartitionProcessor options
------------------------------

  * ``lumi filter`` = <lookup specifier> (default: {})

