grid-control options
====================

.. _global:
global options
--------------

* ``cmdargs`` = <list of values> (Default: '')
    Automatically added command line arguments

* ``config id`` = <text> (Default: <config file name w/o extension> or 'unnamed')
    Identifier for the current configuration

* ``delete`` = <job selector> (Default: '')
    The unfinished jobs selected by this expression are cancelled

* ``gui`` = <plugin> (Default: 'BasicConsoleGUI')
    Specify GUI plugin to handle the user interaction

    List of available plugins:
     * ANSIGUI_ (alias: ansi)
     * BasicConsoleGUI_ (alias: console)
     * CPWebserver_ (alias: cherrypy)

* ``include`` = <list of values> (Default: '')
    List of additional config files which provide default values. These config files are processed in addition to the files: /etc/grid-control.conf, ~/.grid-control.conf and <GCDIR>/default.conf

* ``include override`` = <list of values> (Default: '')
    List of additional config files which will override anything in the current config file.

* ``package paths`` = <list of paths> (Default: '')
    Specify paths to additional grid-control packages with user defined plugins that are outside of the base package directory

* ``plugin paths`` = <list of paths> (Default: '<current directory>')
    Specifies paths that are used to search for plugins

* ``reset`` = <job selector> (Default: '')
    The jobs selected by this expression are reset to the INIT state

* ``variable markers`` = <list of values> (Default: '@ __')
    Specifies how variables are marked

* ``workdir`` = <path> (Default: <workdir base>/work.<config file name>)
    Location of the grid-control work directory. Usually based on the name of the config file

* ``workdir base`` = <path> (Default: <config file path>)
    Directory where the default workdir is created

* ``workdir create`` = <boolean> (Default: True)
    Skip interactive question about workdir creation

* ``workdir space`` = <integer> (Default: 10)
    Lower space limit in the work directory. Monitoring can be deactived with 0

* ``workflow`` = <plugin[:name]> (Default: 'Workflow:global')
    Specifies the workflow that is being run

    List of available plugins:
     * Workflow


.. _Workflow:
Workflow options
----------------

* ``backend`` = <list of plugin[:name] ...>
    Select the backend to use for job submission

    List of available plugins:
     * CreamWMS_ (alias: cream)
     * EuropeanDataGrid_ (alias: EDG, LCG)
     * GliteWMS_ (alias: gwms)
     * GridEngine_ (alias: SGE, UGE, OGE)
     * Host_ (alias: Localhost)
     * InactiveWMS_ (alias: inactive)

* ``backend manager`` = <plugin> (Default: 'MultiWMS')
    Specifiy compositor class to merge the different plugins given in ``backend``

    List of available compositor plugins:
     * MultiWMS_

* ``task / module`` = <plugin[:name]>
    Select the task module to run

    List of available plugins:
     * CMSSWAdvanced_ (alias: CMSSW_Advanced)
     * CMSSW_
     * ROOTTask_ (alias: ROOTMod, root)
     * UserTask_ (alias: UserMod, user, script)

* ``action`` = <list of values> (Default: 'check retrieve submit')
    Specify the actions and the order in which grid-control should perform them

* ``continuous`` = <boolean> (Default: False)
    Enable continuous running mode

* ``duration`` = <duration hh[:mm[:ss]]> (Default: <continuous mode on: infinite (-1), off: exit immediately (0)>)
    Maximal duration of the job processing pass. The default depends on the value of the 'continuous' option.

* ``event handler manager`` = <plugin> (Default: 'CompatEventHandlerManager')
    Specify event handler plugin to manage dual event handlers (that are both remote and local)

    List of available plugins:
     * CompatEventHandlerManager_ (alias: compat)

* ``job manager`` = <plugin[:name]> (Default: 'SimpleJobManager')
    Specify the job management plugin to handle the job cycle

    List of available plugins:
     * SimpleJobManager_ (alias: default)

* ``submission`` = <boolean> (Default: True)
    Toggle to control the submission of jobs

* ``submission time requirement`` = <duration hh[:mm[:ss]]> (Default: <wall time>)
    Toggle to control the submission of jobs

* ``workdir space timeout`` = <duration hh[:mm[:ss]]> (Default: 00:00:05)
    Specify timeout for workdir space check


.. _SimpleJobManager:
SimpleJobManager options
------------------------

* ``abort report`` = <plugin[:name]> (Default: 'LocationReport')
    Specify report plugin to display in case of job cancellations

    List of available plugins:
     * ANSIHeaderReport_ (alias: ansiheader)
     * ANSIReport_ (alias: ansireport)
     * ANSITheme_ (alias: ansi)
     * BackendReport_ (alias: backend)
     * BarReport_ (alias: bar)
     * BasicHeaderReport_ (alias: basicheader)
     * BasicReport_ (alias: basicreport)
     * BasicTheme_ (alias: basic)
     * ColorBarReport_ (alias: cbar)
     * LeanHeaderReport_ (alias: leanheader)
     * LeanReport_ (alias: leanreport)
     * LeanTheme_ (alias: lean)
     * LocationHistoryReport_ (alias: history)
     * LocationReport_ (alias: location)
     * MapReport_ (alias: map)
     * ModernReport_ (alias: modern)
     * ModuleReport_ (alias: module)
     * NullReport_ (alias: null)
     * PlotReport_ (alias: plot)
     * PluginReport_ (alias: plugin)
     * TimeReport_ (alias: time)
     * TrivialReport_ (alias: trivial)
     * VariablesReport_ (alias: variables, vars)

* ``chunks check`` = <integer> (Default: 100)
    Specify maximal number of jobs to check in each job cycle

* ``chunks enabled`` = <boolean> (Default: True)
    Toggle to control if only a chunk of jobs are processed each job cycle

* ``chunks retrieve`` = <integer> (Default: 100)
    Specify maximal number of jobs to retrieve in each job cycle

* ``chunks submit`` = <integer> (Default: 100)
    Specify maximal number of jobs to submit in each job cycle

* ``defect tries / kick offender`` = <integer> (Default: 10)
    Threshold for dropping jobs causing status retrieval errors (disable check with 0)

* ``in flight`` = <integer> (Default: no limit (-1))
    Maximum number of concurrently submitted jobs

* ``in queue`` = <integer> (Default: no limit (-1))
    Maximum number of queued jobs

* ``job database`` = <plugin> (Default: 'TextFileJobDB')
    Specify job database plugin that is used to store job information

    List of available plugins:
     * Migrate2ZippedJobDB_ (alias: migrate)
     * TextFileJobDB_ (alias: textdb)
     * ZippedJobDB_ (alias: zipdb)

* ``jobs`` = <integer> (Default: no limit (-1))
    Maximum number of jobs (truncated to task maximum)

* ``local event handler / local monitor`` = <list of plugin[:name] ...> (Default: 'logmonitor')
    Specify local event handler plugins to track the task / job progress on the submission host

    List of available plugins:
     * BasicLogEventHandler_ (alias: logmonitor)
     * DashboardLocal_ (alias: dashboard)
     * JabberAlarm_ (alias: jabber)
     * ScriptEventHandler_ (alias: scripts)

* ``local event handler manager`` = <plugin> (Default: 'MultiLocalEventHandler')
    Specifiy compositor class to merge the different plugins given in ``local event handler``

    List of available compositor plugins:
     * MultiLocalEventHandler_ (alias: multi)

* ``max retry`` = <integer> (Default: no limit (-1))
    Number of resubmission attempts for failed jobs

* ``output processor`` = <plugin> (Default: 'SandboxProcessor')
    Specify plugin that processes the output sandbox of successful jobs

    List of available plugins:
     * SandboxProcessor_ (alias: null)

* ``queue timeout`` = <duration hh[:mm[:ss]]> (Default: disabled (-1))
    Resubmit jobs after staying some time in initial state

* ``selected`` = <text> (Default: '')
    Apply general job selector

* ``shuffle`` = <boolean> (Default: False)
    Submit jobs in random order

* ``unknown timeout`` = <duration hh[:mm[:ss]]> (Default: disabled (-1))
    Cancel jobs without status information after staying in this state for the specified time

* ``verify chunks`` = <list of values> (Default: '-1')
    Specifies how many jobs to submit initially, and use to verify the workflow. If sufficient jobs succeed, all remaining jobs are enabled for submission

* ``verify threshold / verify reqs`` = <list of values> (Default: '0.5')
    Specifies the fraction of jobs in the verification chunk that must succeed


.. _backend:
backend options
---------------

* ``<prefix> chunk interval`` = <integer> (Default: <depends on the process>)
    Specify the interval between (submit, check, ...) chunks

* ``<prefix> chunk size`` = <integer> (Default: <depends on the process>)
    Specify the size of (submit, check, ...) chunks

* ``access token / proxy`` = <list of plugin[:name] ...> (Default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission

    List of available plugins:
     * AFSAccessToken_ (alias: afs, AFSProxy, KerberosAccessToken)
     * ARCAccessToken_ (alias: arc, arcproxy)
     * TrivialAccessToken_ (alias: trivial, TrivialProxy)
     * VomsAccessToken_ (alias: voms, VomsProxy)

* ``access token manager`` = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``

    List of available compositor plugins:
     * MultiAccessToken_ (alias: multi)

* ``cancel timeout`` = <duration hh[:mm[:ss]]> (Default: 00:01:00)
    Specify timeout of the process that is used to cancel jobs

* ``sb input manager`` = <plugin[:name]> (Default: 'LocalSBStorageManager')
    Specify transfer manager plugin to transfer sandbox input files

    List of available plugins:
     * StorageManager

* ``se input manager`` = <plugin[:name]> (Default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE input files

    List of available plugins:
     * StorageManager

* ``se output manager`` = <plugin[:name]> (Default: 'SEStorageManager')
    Specify transfer manager plugin to transfer SE output files

    List of available plugins:
     * StorageManager


.. _UserTask:
UserTask options
----------------

* ``wall time`` = <duration hh[:mm[:ss]]>
    Requested wall time also used for checking the proxy lifetime

* ``cpu time`` = <duration hh[:mm[:ss]]> (Default: <wall time>)
    Requested cpu time

* ``cpus`` = <integer> (Default: 1)
    Requested number of cpus per node

* ``datasource names`` = <list of values> (Default: 'dataset')
    Specify list of data sources that will be created for use in the parameter space definition

* ``depends`` = <list of values> (Default: '')
    List of environment setup scripts that the jobs depend on

* ``gzip output`` = <boolean> (Default: True)
    Toggle the compression of the job log files for stdout and stderr

* ``input files`` = <list of paths> (Default: '')
    List of files that should be transferred to the landing zone of the job on the worker node. Only for small files - send large files via SE!

* ``internal parameter factory`` = <plugin> (Default: 'BasicParameterFactory')
    Specify the parameter factory plugin that is used to generate the basic grid-control parameters

    List of available plugins:
     * BasicParameterFactory_ (alias: basic)
     * ModularParameterFactory_ (alias: modular)
     * SimpleParameterFactory_ (alias: simple)

* ``job name generator`` = <plugin> (Default: 'DefaultJobName')
    Specify the job name plugin that generates the job name that is given to the backend

    List of available plugins:
     * ConfigurableJobName_ (alias: config)
     * DefaultJobName_ (alias: default)

* ``landing zone space left`` = <integer> (Default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the landing zone directory while running

* ``landing zone space used`` = <integer> (Default: 100)
    Maximum amount of disk space (in MB) that the job is allowed to use in the landing zone directory while running

* ``memory`` = <integer> (Default: unspecified (-1))
    Requested memory in MB. Some batch farms have very low default memory limits in which case it is necessary to specify this option!

* ``node timeout`` = <duration hh[:mm[:ss]]> (Default: disabled (-1))
    Cancel job after some time on worker node

* ``output files`` = <list of values> (Default: '')
    List of files that should be transferred to the job output directory on the submission machine. Only for small files - send large files via SE!

* ``parameter adapter`` = <plugin> (Default: 'TrackedParameterAdapter')
    Specify the parameter adapter plugin that translates parameter point to job number

    List of available plugins:
     * BasicParameterAdapter_ (alias: basic)
     * TrackedParameterAdapter_ (alias: tracked)

* ``scratch space left`` = <integer> (Default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply

* ``scratch space used`` = <integer> (Default: 5000)
    Maximum amount of disk space (in MB) that the job is allowed to use in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply

* ``se min size`` = <integer> (Default: -1)
    TODO: DELETE

* ``subst files`` = <list of values> (Default: '')
    List of files that will be subjected to variable substituion

* ``task date`` = <text> (Default: <current date: YYYY-MM-DD>)
    Persistent date when the task was started

* ``task id`` = <text> (Default: 'GCxxxxxxxxxxxx')
    Persistent task identifier that is generated at the start of the task

* ``task name generator`` = <plugin> (Default: 'DefaultTaskName')
    Specify the task name plugin that generates the task name that is given to the backend

    List of available plugins:
     * DefaultTaskName_ (alias: default)

* ``task time`` = <text> (Default: <current time: HHMMSS>)
    Persistent time when the task was started


.. _CMSSW:
CMSSW options
-------------

* ``wall time`` = <duration hh[:mm[:ss]]>
    Requested wall time also used for checking the proxy lifetime

* ``area files`` = <filter option> (Default: '-.* -config bin lib python module data *.xml *.sql *.db *.cfi *.cff *.py -CVS -work.* *.pcm')
    List of files that should be taken from the CMSSW project area for running the job

* ``area files matcher`` = <plugin> (Default: 'BlackWhiteMatcher')
    Specifiy matcher plugin that is used to match filter expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``area files basename`` = <boolean> (Default: True)
    Toggle between using the relative path or just the file base name to match area files

* ``arguments`` = <text> (Default: '')
    Arguments that will be passed to the *cmsRun* call

* ``config file`` = <list of paths> (Default: <no default> or '' if prolog / epilog script is given)
    List of config files that will be sequentially processed by *cmsRun* calls

* ``cpu time`` = <duration hh[:mm[:ss]]> (Default: <wall time>)
    Requested cpu time

* ``cpus`` = <integer> (Default: 1)
    Requested number of cpus per node

* ``datasource names`` = <list of values> (Default: 'dataset')
    Specify list of data sources that will be created for use in the parameter space definition

* ``depends`` = <list of values> (Default: '')
    List of environment setup scripts that the jobs depend on

* ``events per job`` = <text> (Default: '0')
    This sets the variable MAX_EVENTS if no datasets are present

* ``gzip output`` = <boolean> (Default: True)
    Toggle the compression of the job log files for stdout and stderr

* ``input files`` = <list of paths> (Default: '')
    List of files that should be transferred to the landing zone of the job on the worker node. Only for small files - send large files via SE!

* ``instrumentation`` = <boolean> (Default: True)
    Toggle to control the instrumentation of CMSSW config files for running over data / initializing the RNG for MC production

* ``instrumentation fragment`` = <path> (Default: <grid-control cms package>/share/fragmentForCMSSW.py)
    Path to the instrumentation fragment that is appended to the CMSSW config file if instrumentation is enabled

* ``internal parameter factory`` = <plugin> (Default: 'BasicParameterFactory')
    Specify the parameter factory plugin that is used to generate the basic grid-control parameters

    List of available plugins:
     * BasicParameterFactory_ (alias: basic)
     * ModularParameterFactory_ (alias: modular)
     * SimpleParameterFactory_ (alias: simple)

* ``job name generator`` = <plugin> (Default: 'DefaultJobName')
    Specify the job name plugin that generates the job name that is given to the backend

    List of available plugins:
     * ConfigurableJobName_ (alias: config)
     * DefaultJobName_ (alias: default)

* ``landing zone space left`` = <integer> (Default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the landing zone directory while running

* ``landing zone space used`` = <integer> (Default: 100)
    Maximum amount of disk space (in MB) that the job is allowed to use in the landing zone directory while running

* ``memory`` = <integer> (Default: unspecified (-1))
    Requested memory in MB. Some batch farms have very low default memory limits in which case it is necessary to specify this option!

* ``node timeout`` = <duration hh[:mm[:ss]]> (Default: disabled (-1))
    Cancel job after some time on worker node

* ``output files`` = <list of values> (Default: '')
    List of files that should be transferred to the job output directory on the submission machine. Only for small files - send large files via SE!

* ``parameter adapter`` = <plugin> (Default: 'TrackedParameterAdapter')
    Specify the parameter adapter plugin that translates parameter point to job number

    List of available plugins:
     * BasicParameterAdapter_ (alias: basic)
     * TrackedParameterAdapter_ (alias: tracked)

* ``project area`` = <path> (Default: <depends on ``scram arch`` and ``scram project``>)
    Specify location of the CMSSW project area that should be send with the job. Instead of the CMSSW project area, it is possible to specify ``scram arch`` and ``scram project`` to use a fresh CMSSW project

* ``scram arch`` = <text> (Default: <depends on ``project area``>)
    Specify scram architecture that should be used by the job (eg. 'slc7_amd64_gcc777'). When using an existing CMSSW project area with ``project area``, this option uses the default value taken from the project area

* ``scram arch requirements`` = <boolean> (Default: True)
    Toggle the inclusion of the scram architecture in the job requirements

* ``scram project`` = <list of values> (Default: '')
    Specify scram project that should be used by the job (eg. 'CMSSW CMSSW_9_9_9')

* ``scram project requirements`` = <boolean> (Default: False)
    Toggle the inclusion of the scram project name in the job requirements

* ``scram project version requirements`` = <boolean> (Default: False)
    Toggle the inclusion of the scram project version in the job requirements

* ``scram version`` = <text> (Default: 'scramv1')
    Specify scram version that should be used by the job

* ``scratch space left`` = <integer> (Default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply

* ``scratch space used`` = <integer> (Default: 5000)
    Maximum amount of disk space (in MB) that the job is allowed to use in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply

* ``se min size`` = <integer> (Default: -1)
    TODO: DELETE

* ``se project area / se runtime`` = <boolean> (Default: True)
    Toggle to specify how the CMSSW project area should be transferred to the worker node

* ``subst files`` = <list of values> (Default: '')
    List of files that will be subjected to variable substituion

* ``task date`` = <text> (Default: <current date: YYYY-MM-DD>)
    Persistent date when the task was started

* ``task id`` = <text> (Default: 'GCxxxxxxxxxxxx')
    Persistent task identifier that is generated at the start of the task

* ``task name generator`` = <plugin> (Default: 'DefaultTaskName')
    Specify the task name plugin that generates the task name that is given to the backend

    List of available plugins:
     * DefaultTaskName_ (alias: default)

* ``task time`` = <text> (Default: <current time: HHMMSS>)
    Persistent time when the task was started

* ``vo software dir / cmssw dir`` = <text> (Default: '')
    This option allows to override of the VO_CMS_SW_DIR environment variable


.. _CMSSWAdvanced:
CMSSWAdvanced options
---------------------

* ``wall time`` = <duration hh[:mm[:ss]]>
    Requested wall time also used for checking the proxy lifetime

* ``area files`` = <filter option> (Default: '-.* -config bin lib python module data *.xml *.sql *.db *.cfi *.cff *.py -CVS -work.* *.pcm')
    List of files that should be taken from the CMSSW project area for running the job

* ``area files matcher`` = <plugin> (Default: 'BlackWhiteMatcher')
    Specifiy matcher plugin that is used to match filter expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``area files basename`` = <boolean> (Default: True)
    Toggle between using the relative path or just the file base name to match area files

* ``arguments`` = <text> (Default: '')
    Arguments that will be passed to the *cmsRun* call

* ``config file`` = <list of paths> (Default: <no default> or '' if prolog / epilog script is given)
    List of config files that will be sequentially processed by *cmsRun* calls

* ``cpu time`` = <duration hh[:mm[:ss]]> (Default: <wall time>)
    Requested cpu time

* ``cpus`` = <integer> (Default: 1)
    Requested number of cpus per node

* ``datasource names`` = <list of values> (Default: 'dataset')
    Specify list of data sources that will be created for use in the parameter space definition

* ``depends`` = <list of values> (Default: '')
    List of environment setup scripts that the jobs depend on

* ``events per job`` = <text> (Default: '0')
    This sets the variable MAX_EVENTS if no datasets are present

* ``gzip output`` = <boolean> (Default: True)
    Toggle the compression of the job log files for stdout and stderr

* ``input files`` = <list of paths> (Default: '')
    List of files that should be transferred to the landing zone of the job on the worker node. Only for small files - send large files via SE!

* ``instrumentation`` = <boolean> (Default: True)
    Toggle to control the instrumentation of CMSSW config files for running over data / initializing the RNG for MC production

* ``instrumentation fragment`` = <path> (Default: <grid-control cms package>/share/fragmentForCMSSW.py)
    Path to the instrumentation fragment that is appended to the CMSSW config file if instrumentation is enabled

* ``internal parameter factory`` = <plugin> (Default: 'BasicParameterFactory')
    Specify the parameter factory plugin that is used to generate the basic grid-control parameters

    List of available plugins:
     * BasicParameterFactory_ (alias: basic)
     * ModularParameterFactory_ (alias: modular)
     * SimpleParameterFactory_ (alias: simple)

* ``job name generator`` = <plugin> (Default: 'DefaultJobName')
    Specify the job name plugin that generates the job name that is given to the backend

    List of available plugins:
     * ConfigurableJobName_ (alias: config)
     * DefaultJobName_ (alias: default)

* ``landing zone space left`` = <integer> (Default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the landing zone directory while running

* ``landing zone space used`` = <integer> (Default: 100)
    Maximum amount of disk space (in MB) that the job is allowed to use in the landing zone directory while running

* ``memory`` = <integer> (Default: unspecified (-1))
    Requested memory in MB. Some batch farms have very low default memory limits in which case it is necessary to specify this option!

* ``nickname config`` = <lookup specifier> (Default: '')
    Allows to specify a dictionary with list of config files that will be sequentially processed by *cmsRun* calls. The dictionary key is the job dependent dataset nickname

* ``nickname config matcher`` = <plugin> (Default: 'RegExMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``nickname constants`` = <list of values> (Default: '')
    Allows to specify a list of nickname dependent variables. The value of the variables is specified separately in the form of a dictionary. (This option is deprecated, since *all* variables support this functionality now!)

* ``nickname lumi filter`` = <dictionary> (Default: {})
    Allows to specify a dictionary with nickname dependent lumi filter expressions. (This option is deprecated, since the normal option ``lumi filter`` already supports this!)

* ``node timeout`` = <duration hh[:mm[:ss]]> (Default: disabled (-1))
    Cancel job after some time on worker node

* ``output files`` = <list of values> (Default: '')
    List of files that should be transferred to the job output directory on the submission machine. Only for small files - send large files via SE!

* ``parameter adapter`` = <plugin> (Default: 'TrackedParameterAdapter')
    Specify the parameter adapter plugin that translates parameter point to job number

    List of available plugins:
     * BasicParameterAdapter_ (alias: basic)
     * TrackedParameterAdapter_ (alias: tracked)

* ``project area`` = <path> (Default: <depends on ``scram arch`` and ``scram project``>)
    Specify location of the CMSSW project area that should be send with the job. Instead of the CMSSW project area, it is possible to specify ``scram arch`` and ``scram project`` to use a fresh CMSSW project

* ``scram arch`` = <text> (Default: <depends on ``project area``>)
    Specify scram architecture that should be used by the job (eg. 'slc7_amd64_gcc777'). When using an existing CMSSW project area with ``project area``, this option uses the default value taken from the project area

* ``scram arch requirements`` = <boolean> (Default: True)
    Toggle the inclusion of the scram architecture in the job requirements

* ``scram project`` = <list of values> (Default: '')
    Specify scram project that should be used by the job (eg. 'CMSSW CMSSW_9_9_9')

* ``scram project requirements`` = <boolean> (Default: False)
    Toggle the inclusion of the scram project name in the job requirements

* ``scram project version requirements`` = <boolean> (Default: False)
    Toggle the inclusion of the scram project version in the job requirements

* ``scram version`` = <text> (Default: 'scramv1')
    Specify scram version that should be used by the job

* ``scratch space left`` = <integer> (Default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply

* ``scratch space used`` = <integer> (Default: 5000)
    Maximum amount of disk space (in MB) that the job is allowed to use in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply

* ``se min size`` = <integer> (Default: -1)
    TODO: DELETE

* ``se project area / se runtime`` = <boolean> (Default: True)
    Toggle to specify how the CMSSW project area should be transferred to the worker node

* ``subst files`` = <list of values> (Default: '')
    List of files that will be subjected to variable substituion

* ``task date`` = <text> (Default: <current date: YYYY-MM-DD>)
    Persistent date when the task was started

* ``task id`` = <text> (Default: 'GCxxxxxxxxxxxx')
    Persistent task identifier that is generated at the start of the task

* ``task name generator`` = <plugin> (Default: 'DefaultTaskName')
    Specify the task name plugin that generates the task name that is given to the backend

    List of available plugins:
     * DefaultTaskName_ (alias: default)

* ``task time`` = <text> (Default: <current time: HHMMSS>)
    Persistent time when the task was started

* ``vo software dir / cmssw dir`` = <text> (Default: '')
    This option allows to override of the VO_CMS_SW_DIR environment variable


.. _dataset:
dataset options
---------------

* ``<datasource>`` = <list of [<nickname> : [<provider> :]] <dataset specifier> > (Default: '')
    Specify list of datasets to process (including optional nickname and dataset provider information)

    List of available plugins:
     * ConfigDataProvider_ (alias: config)
     * DASProvider_ (alias: das)
     * DBS2Provider_ (alias: dbs2)
     * DBS3Provider_ (alias: dbs3, dbs)
     * DBSInfoProvider_ (alias: dbsinfo)
     * FileProvider_ (alias: file)
     * GCProvider_ (alias: gc)
     * ListProvider_ (alias: list)
     * ScanProvider_ (alias: scan)

* ``<datasource> manager`` = <plugin> (Default: ':ThreadedMultiDatasetProvider:')
    Specifiy compositor class to merge the different plugins given in ``<datasource>``

    List of available compositor plugins:
     * MultiDatasetProvider_ (alias: multi)
     * ThreadedMultiDatasetProvider_ (alias: threaded)

* ``<datasource> default query interval`` = <duration hh[:mm[:ss]]> (Default: 00:01:00)
    Specify the default limit for the dataset query interval

* ``<datasource> nickname source / nickname source`` = <plugin> (Default: 'SimpleNickNameProducer')
    Specify nickname plugin that determines the nickname for datasets

    List of available plugins:
     * EmptyDataProcessor_ (alias: empty)
     * EntriesConsistencyDataProcessor_ (alias: consistency)
     * EntriesCountDataProcessor_ (alias: events, EventsCountDataProcessor)
     * InlineNickNameProducer_ (alias: inline)
     * LocationDataProcessor_ (alias: location)
     * LumiDataProcessor_ (alias: lumi)
     * NickNameConsistencyProcessor_ (alias: nickconsistency)
     * NullDataProcessor_ (alias: null)
     * PartitionEstimator_ (alias: estimate, SplitSettingEstimator)
     * SimpleNickNameProducer_ (alias: simple)
     * SimpleStatsDataProcessor_ (alias: stats)
     * SortingDataProcessor_ (alias: sort)
     * URLCountDataProcessor_ (alias: files, FileCountDataProcessor)
     * URLDataProcessor_ (alias: ignore, FileDataProcessor)
     * UniqueDataProcessor_ (alias: unique)

* ``<datasource> partition processor / partition processor`` = <list of plugins> (Default: 'TFCPartitionProcessor LocationPartitionProcessor MetaPartitionProcessor BasicPartitionProcessor')
    Specify list of plugins that process partitions

    List of available plugins:
     * BasicPartitionProcessor_ (alias: basic)
     * CMSSWPartitionProcessor_ (alias: cmsswpart)
     * LFNPartitionProcessor_ (alias: lfnprefix)
     * LocationPartitionProcessor_ (alias: location)
     * LumiPartitionProcessor_ (alias: lumi)
     * MetaPartitionProcessor_ (alias: metadata)
     * RequirementsPartitionProcessor_ (alias: reqs)
     * TFCPartitionProcessor_ (alias: tfc)

* ``<datasource> partition processor manager`` = <plugin> (Default: 'MultiPartitionProcessor')
    Specifiy compositor class to merge the different plugins given in ``<datasource> partition processor``

    List of available compositor plugins:
     * MultiPartitionProcessor_ (alias: multi)

* ``<datasource> processor`` = <list of plugins> (Default: 'NickNameConsistencyProcessor EntriesConsistencyDataProcessor URLDataProcessor URLCountDataProcessor EntriesCountDataProcessor EmptyDataProcessor UniqueDataProcessor LocationDataProcessor')
    Specify list of plugins that process datasets before the partitioning

    List of available plugins:
     * EmptyDataProcessor_ (alias: empty)
     * EntriesConsistencyDataProcessor_ (alias: consistency)
     * EntriesCountDataProcessor_ (alias: events, EventsCountDataProcessor)
     * InlineNickNameProducer_ (alias: inline)
     * LocationDataProcessor_ (alias: location)
     * LumiDataProcessor_ (alias: lumi)
     * NickNameConsistencyProcessor_ (alias: nickconsistency)
     * NullDataProcessor_ (alias: null)
     * PartitionEstimator_ (alias: estimate, SplitSettingEstimator)
     * SimpleNickNameProducer_ (alias: simple)
     * SimpleStatsDataProcessor_ (alias: stats)
     * SortingDataProcessor_ (alias: sort)
     * URLCountDataProcessor_ (alias: files, FileCountDataProcessor)
     * URLDataProcessor_ (alias: ignore, FileDataProcessor)
     * UniqueDataProcessor_ (alias: unique)

* ``<datasource> processor manager`` = <plugin> (Default: 'MultiDataProcessor')
    Specifiy compositor class to merge the different plugins given in ``<datasource> processor``

    List of available compositor plugins:
     * MultiDataProcessor_ (alias: multi)

* ``<datasource> provider / default provider`` = <text> (Default: 'ListProvider')
    Specify the name of the default dataset provider

* ``<datasource> refresh`` = <duration hh[:mm[:ss]]> (Default: disabled (-1))
    Specify the interval to check for changes in the used datasets

* ``<datasource> splitter`` = <plugin> (Default: 'FileBoundarySplitter')
    Specify the dataset splitter plugin to partition the dataset

* ``resync jobs`` = <enum: APPEND|PRESERVE|FILLGAP|REORDER> (Default: APPEND)
    Specify how resynced jobs should be handled

* ``resync metadata`` = <list of values> (Default: '')
    List of metadata keys that have configuration options to specify how metadata changes are handled by a dataset resync

* ``resync mode <metadata key>`` = <enum: DISABLE|COMPLETE|IGNORE> (Default: COMPLETE)
    Specify how changes in the given metadata key affect partitions during resync

* ``resync mode added`` = <enum: COMPLETE|IGNORE> (Default: COMPLETE)
    Sets the resync mode for new files

* ``resync mode expand`` = <enum: DISABLE|COMPLETE|CHANGED|IGNORE> (Default: CHANGED)
    Sets the resync mode for expanded files

* ``resync mode removed`` = <enum: DISABLE|COMPLETE|IGNORE> (Default: COMPLETE)
    Sets the resync mode for removed files

* ``resync mode shrink`` = <enum: DISABLE|COMPLETE|CHANGED|IGNORE> (Default: CHANGED)
    Sets the resync mode for shrunken files


.. _CMS grid proxy:
CMS grid proxy options
----------------------

* ``new proxy lifetime`` = <duration hh[:mm[:ss]]> (Default: 03:12:00)
    Specify the new lifetime for a newly created grid proxy

* ``new proxy roles`` = <list of values> (Default: '')
    Specify the new roles for a newly created grid proxy (in addition to the cms role)

* ``new proxy timeout`` = <duration hh[:mm[:ss]]> (Default: 00:00:10)
    Specify the timeout for waiting to create a new grid proxy


.. _TaskExecutableWrapper:
TaskExecutableWrapper options
-----------------------------

* ``[<prefix>] arguments`` = <text> (Default: '')
    Specify arguments for the executable

* ``[<prefix>] executable`` = <text> (Default: <no default> or '')
    Path to the executable

* ``[<prefix>] send executable`` = <boolean> (Default: True)
    Toggle to control if the specified executable should be send together with the job


.. _interactive:
interactive options
-------------------

* ``<option name>`` = <boolean> (Default: True)
    Toggle to switch interactive questions on and off

* ``dataset name assignment`` = <boolean> (Default: True)
    Toggle interactive question about issues with the bijectivity of the dataset / block name assignments in the scan provider

* ``delete jobs`` = <boolean> (Default: True)
    Toggle interactivity of job deletion requests

* ``reset jobs`` = <boolean> (Default: True)
    Toggle interactivity of job reset requests


.. _logging:
logging options
---------------

* ``<logger name> file`` = <text>
    Log file used by file logger

* ``<logger name> <handler> code context / <logger name> code context`` = <integer> (Default: 2)
    Number of code context lines in shown exception logs

* ``<logger name> <handler> detail lower limit / <logger name> detail lower limit`` = <enum: LEVEL 0..50|NOTSET|DEBUG3...DEBUG|INFO3..INFO|DEFAULT|WARNING|ERROR|CRITICAL> (Default: DEBUG)
    Logging messages below this log level will use the long form output

* ``<logger name> <handler> detail upper limit / <logger name> detail upper limit`` = <enum: LEVEL 0..50|NOTSET|DEBUG3...DEBUG|INFO3..INFO|DEFAULT|WARNING|ERROR|CRITICAL> (Default: ERROR)
    Logging messages above this log level will use the long form output

* ``<logger name> <handler> file stack / <logger name> file stack`` = <integer> (Default: 1)
    Level of detail for file stack information shown in exception logs

* ``<logger name> <handler> thread stack / <logger name> thread stack`` = <integer> (Default: 1)
    Level of detail for thread stack information shown in exception logs

* ``<logger name> <handler> tree / <logger name> tree`` = <integer> (Default: 2)
    Level of detail for exception tree information shown in exception logs

* ``<logger name> <handler> variables / <logger name> variables`` = <integer> (Default: 200)
    Level of detail for variable information shown in exception logs

* ``<logger name> debug file`` = <list of paths> (Default: '"<gc dir>/debug.log" "/tmp/gc.debug.<uid>.<pid>" "~/gc.debug"')
    Logfile used by debug file logger. In case multiple paths are specified, the first usable path will be used

* ``<logger name> handler`` = <list of values> (Default: '')
    List of log handlers

* ``<logger name> level`` = <enum: LEVEL 0..50|NOTSET|DEBUG3...DEBUG|INFO3..INFO|DEFAULT|WARNING|ERROR|CRITICAL> (Default: <depends on the logger>)
    Logging level of log handlers

* ``<logger name> propagate`` = <boolean> (Default: <depends on the logger>)
    Toggle log propagation

* ``activity stream stderr / activity stream`` = <plugin> (Default: 'DefaultActivityMonitor')
    Specify activity stream class that displays the current activity tree on stderr

    List of available plugins:
     * DefaultActivityMonitor_ (alias: default_stream)
     * NullOutputStream_ (alias: null)
     * SingleActivityMonitor_ (alias: single_stream)
     * TimedActivityMonitor_ (alias: timed_stream)

* ``activity stream stdout / activity stream`` = <plugin> (Default: 'DefaultActivityMonitor')
    Specify activity stream class that displays the current activity tree on stdout

    List of available plugins:
     * DefaultActivityMonitor_ (alias: default_stream)
     * NullOutputStream_ (alias: null)
     * SingleActivityMonitor_ (alias: single_stream)
     * TimedActivityMonitor_ (alias: timed_stream)

* ``debug mode`` = <boolean> (Default: False)
    Toggle debug mode (detailed exception output on stdout)

* ``display logger`` = <boolean> (Default: False)
    Toggle display of logging structure


.. _parameters:
parameters options
------------------

* ``parameters`` = <text> (Default: '')
    Specify the parameter expression that defines the parameter space. The syntax depends on the used parameter factory


.. _ActivityMonitor:
ActivityMonitor options
-----------------------

* ``activity max length`` = <integer> (Default: 75)
    Specify maximum number of activities to display


.. _Matcher:
Matcher options
---------------

* ``<prefix> case sensitive`` = <boolean> (Default: True)
    Toggle case sensitivity for the matcher


.. _MultiActivityMonitor:
MultiActivityMonitor options
----------------------------

* ``activity fold fraction`` = <float> (Default: 0.5)
    Start folding activities when the number of activities reach this fraction of the display height

* ``activity max length`` = <integer> (Default: 75)
    Specify maximum number of activities to display


.. _TimedActivityMonitor:
TimedActivityMonitor options
----------------------------

* ``activity interval`` = <float> (Default: 5.0)
    Specify interval to display the

* ``activity max length`` = <integer> (Default: 75)
    Specify maximum number of activities to display


.. _GridEngineDiscoverNodes:
GridEngineDiscoverNodes options
-------------------------------

* ``discovery timeout`` = <duration hh[:mm[:ss]]> (Default: 00:00:30)
    Specify timeout of the process that is used to discover backend featues


.. _GridEngineDiscoverQueues:
GridEngineDiscoverQueues options
--------------------------------

* ``discovery timeout`` = <duration hh[:mm[:ss]]> (Default: 00:00:30)
    Specify timeout of the process that is used to discover backend featues


.. _PBSDiscoverNodes:
PBSDiscoverNodes options
------------------------

* ``discovery timeout`` = <duration hh[:mm[:ss]]> (Default: 00:00:30)
    Specify timeout of the process that is used to discover backend featues


.. _CheckJobsWithProcess:
CheckJobsWithProcess options
----------------------------

* ``check promiscuous`` = <boolean> (Default: False)
    Toggle the indiscriminate logging of the job status tool output

* ``check timeout`` = <duration hh[:mm[:ss]]> (Default: 00:01:00)
    Specify timeout of the process that is used to check the job status


.. _GridEngineCheckJobs:
GridEngineCheckJobs options
---------------------------

* ``check promiscuous`` = <boolean> (Default: False)
    Toggle the indiscriminate logging of the job status tool output

* ``check timeout`` = <duration hh[:mm[:ss]]> (Default: 00:01:00)
    Specify timeout of the process that is used to check the job status

* ``job status key`` = <list of values> (Default: 'JB_jobnum JB_jobnumber JB_job_number')
    List of property names that are used to determine the wms id of jobs


.. _EmptyDataProcessor:
EmptyDataProcessor options
--------------------------

* ``<datasource> remove empty blocks`` = <boolean> (Default: True)
    Toggle removal of empty blocks (without files) from the dataset

* ``<datasource> remove empty files`` = <boolean> (Default: True)
    Toggle removal of empty files (without entries) from the dataset


.. _EntriesCountDataProcessor:
EntriesCountDataProcessor options
---------------------------------

* ``<datasource> limit entries / <datasource> limit events`` = <integer> (Default: -1)
    Specify the number of events after which addition files in the dataset are discarded


.. _LocationDataProcessor:
LocationDataProcessor options
-----------------------------

* ``<datasource> location filter`` = <filter option> (Default: '')
    Specify dataset location filter. Dataset without locations have the filter whitelist applied

* ``<datasource> location filter plugin`` = <plugin> (Default: 'StrictListFilter')
    Specifiy plugin that is used to filter the list

    List of available filters:
     * MediumListFilter_ (alias: try_strict)
     * StrictListFilter_ (alias: strict, require)
     * WeakListFilter_ (alias: weak, prefer)

* ``<datasource> location filter matcher`` = <plugin> (Default: 'BlackWhiteMatcher')
    Specifiy matcher plugin that is used to match filter expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``<datasource> location filter order`` = <enum: SOURCE|MATCHER> (Default: SOURCE)
    Specifiy the order of the filtered list


.. _LumiDataProcessor:
LumiDataProcessor options
-------------------------

* ``<datasource> lumi filter / lumi filter`` = <lookup specifier> (Default: '')
    Specify lumi filter for the dataset (as nickname dependent dictionary)

* ``<datasource> lumi filter matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``<datasource> lumi filter strictness / lumi filter strictness`` = <enum: STRICT|WEAK> (Default: STRICT)
    Specify if the lumi filter requires the run and lumi information (strict) or just the run information (weak)

* ``<datasource> lumi keep / lumi keep`` = <enum: RUNLUMI|RUN|NONE> (Default: <Run/none depending on active/inactive lumi filter>)
    Specify which lumi metadata to retain


.. _MultiDataProcessor:
MultiDataProcessor options
--------------------------

* ``<datasource> processor prune`` = <boolean> (Default: True)
    Toggle the removal of unused dataset processors from the dataset processing pipeline


.. _PartitionEstimator:
PartitionEstimator options
--------------------------

* ``<datasource> target partitions / target partitions`` = <integer> (Default: -1)
    Specify the number of partitions the splitter should aim for

* ``<datasource> target partitions per nickname / target partitions per nickname`` = <integer> (Default: -1)
    Specify the number of partitions per nickname the splitter should aim for


.. _SortingDataProcessor:
SortingDataProcessor options
----------------------------

* ``<datasource> block sort`` = <boolean> (Default: False)
    Toggle sorting of dataset blocks

* ``<datasource> files sort`` = <boolean> (Default: False)
    Toggle sorting of dataset files

* ``<datasource> location sort`` = <boolean> (Default: False)
    Toggle sorting of dataset locations

* ``<datasource> sort`` = <boolean> (Default: False)
    Toggle sorting of datasets


.. _URLCountDataProcessor:
URLCountDataProcessor options
-----------------------------

* ``<datasource> limit urls / <datasource> limit files`` = <integer> (Default: -1)
    Specify the number of files after which addition files in the dataset are discarded

* ``<datasource> limit urls fraction / <datasource> limit files fraction`` = <float> (Default: -1.0)
    Specify the fraction of files in the dataset that should be used


.. _URLDataProcessor:
URLDataProcessor options
------------------------

* ``<datasource> ignore urls / <datasource> ignore files`` = <filter option> (Default: '')
    Specify list of url / data sources to remove from the dataset

* ``<datasource> ignore urls plugin`` = <plugin> (Default: 'WeakListFilter')
    Specifiy plugin that is used to filter the list

    List of available filters:
     * MediumListFilter_ (alias: try_strict)
     * StrictListFilter_ (alias: strict, require)
     * WeakListFilter_ (alias: weak, prefer)

* ``<datasource> ignore urls matcher`` = <plugin> (Default: 'BlackWhiteMatcher')
    Specifiy matcher plugin that is used to match filter expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``<datasource> ignore urls order`` = <enum: SOURCE|MATCHER> (Default: SOURCE)
    Specifiy the order of the filtered list


.. _EntriesConsistencyDataProcessor:
EntriesConsistencyDataProcessor options
---------------------------------------

* ``<datasource> check entry consistency`` = <enum: WARN|ABORT|IGNORE> (Default: ABORT)
    Toggle check for consistency between the number of events given in the block and and the files


.. _NickNameConsistencyProcessor:
NickNameConsistencyProcessor options
------------------------------------

* ``<datasource> check nickname collision`` = <enum: WARN|ABORT|IGNORE> (Default: ABORT)
    Toggle nickname collision checks between datasets

* ``<datasource> check nickname consistency`` = <enum: WARN|ABORT|IGNORE> (Default: ABORT)
    Toggle check for consistency of nicknames between blocks in the same dataset


.. _UniqueDataProcessor:
UniqueDataProcessor options
---------------------------

* ``<datasource> check unique block`` = <enum: WARN|ABORT|SKIP|IGNORE|RECORD> (Default: ABORT)
    Specify how to react to duplicated dataset and blockname combinations

* ``<datasource> check unique url`` = <enum: WARN|ABORT|SKIP|IGNORE|RECORD> (Default: ABORT)
    Specify how to react to duplicated urls in the dataset


.. _InlineNickNameProducer:
InlineNickNameProducer options
------------------------------

* ``<datasource> nickname expr / nickname expr`` = <text> (Default: 'current_nickname')
    Specify a python expression (using the variables dataset, block and oldnick) to generate the dataset nickname for the block


.. _SimpleNickNameProducer:
SimpleNickNameProducer options
------------------------------

* ``<datasource> nickname full name / nickname full name`` = <boolean> (Default: True)
    Toggle if the nickname should be constructed from the complete dataset name or from the first part


.. _CMSBaseProvider:
CMSBaseProvider options
-----------------------

* ``<datasource> lumi filter / lumi filter`` = <lookup specifier> (Default: '')
    Specify lumi filter for the dataset (as nickname dependent dictionary)

* ``<datasource> lumi filter matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``<datasource> lumi metadata / lumi metadata`` = <boolean> (Default: <True/False for active/inactive lumi filter>)
    Toggle the retrieval of lumi metadata

* ``dbs instance`` = <text> (Default: 'prod/global')
    Specify the default dbs instance (by url or instance identifier) to use for dataset queries

* ``location format`` = <enum: HOSTNAME|SITEDB|BOTH> (Default: HOSTNAME)
    Specify the format of the DBS location information

* ``only complete sites`` = <boolean> (Default: True)
    Toggle the inclusion of incomplete sites in the dataset location information

* ``only valid`` = <boolean> (Default: True)
    Toggle the inclusion of files marked as invalid dataset

* ``phedex sites`` = <filter option> (Default: '-* T1_*_Disk T2_* T3_*')
    Toggle the inclusion of files marked as invalid dataset

* ``phedex sites plugin`` = <plugin> (Default: 'StrictListFilter')
    Specifiy plugin that is used to filter the list

    List of available filters:
     * MediumListFilter_ (alias: try_strict)
     * StrictListFilter_ (alias: strict, require)
     * WeakListFilter_ (alias: weak, prefer)

* ``phedex sites matcher`` = <plugin> (Default: 'BlackWhiteMatcher')
    Specifiy matcher plugin that is used to match filter expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``phedex sites order`` = <enum: SOURCE|MATCHER> (Default: SOURCE)
    Specifiy the order of the filtered list


.. _ConfigDataProvider:
ConfigDataProvider options
--------------------------

* ``<dataset URL>`` = <int> [<metadata in JSON format>]
    The option name corresponds to the URL of the dataset file. The value consists of the number of entry and some optional file metadata

* ``events`` = <integer> (Default: automatic (-1))
    Specify total number of events in the dataset

* ``metadata`` = <text> (Default: '[]')
    List of metadata keys in the dataset

* ``metadata common`` = <text> (Default: '[]')
    Specify metadata values in JSON format that are common to all files in the dataset

* ``nickname`` = <text> (Default: <determined by dataset expression>)
    Specify the dataset nickname

* ``prefix`` = <text> (Default: '')
    Specify the common prefix of URLs in the dataset

* ``se list`` = <text> (Default: '')
    Specify list of locations where the dataset is available


.. _ScanProviderBase:
ScanProviderBase options
------------------------

* ``<prefix> guard override`` = <list of values> (Default: <taken from the selected info scanners>)
    Override the list of guard keys that are preventing files from being in the same datasets or block

* ``<prefix> hash keys`` = <list of values> (Default: '')
    Specify list of keys that are used to determine the datasets or block assigment of files

* ``<prefix> key select`` = <list of values> (Default: '')
    Specify list of dataset or block hashes that are selected for this data source

* ``<prefix> name pattern`` = <text> (Default: '')
    Specify the name pattern for the dataset or block (using variables that are common to all files in the dataset or block)

* ``scanner`` = <list of values> (Default: <depends on other configuration options>)
    Specify list of info scanner plugins to retrieve dataset informations


.. _DASProvider:
DASProvider options
-------------------

* ``<datasource> lumi filter / lumi filter`` = <lookup specifier> (Default: '')
    Specify lumi filter for the dataset (as nickname dependent dictionary)

* ``<datasource> lumi filter matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``<datasource> lumi metadata / lumi metadata`` = <boolean> (Default: <True/False for active/inactive lumi filter>)
    Toggle the retrieval of lumi metadata

* ``das instance`` = <text> (Default: 'https://cmsweb.cern.ch/das/cache')
    Specify url to the DAS instance that is used to query the datasets

* ``dbs instance`` = <text> (Default: 'prod/global')
    Specify the default dbs instance (by url or instance identifier) to use for dataset queries

* ``location format`` = <enum: HOSTNAME|SITEDB|BOTH> (Default: HOSTNAME)
    Specify the format of the DBS location information

* ``only complete sites`` = <boolean> (Default: True)
    Toggle the inclusion of incomplete sites in the dataset location information

* ``only valid`` = <boolean> (Default: True)
    Toggle the inclusion of files marked as invalid dataset

* ``phedex sites`` = <filter option> (Default: '-* T1_*_Disk T2_* T3_*')
    Toggle the inclusion of files marked as invalid dataset

* ``phedex sites plugin`` = <plugin> (Default: 'StrictListFilter')
    Specifiy plugin that is used to filter the list

    List of available filters:
     * MediumListFilter_ (alias: try_strict)
     * StrictListFilter_ (alias: strict, require)
     * WeakListFilter_ (alias: weak, prefer)

* ``phedex sites matcher`` = <plugin> (Default: 'BlackWhiteMatcher')
    Specifiy matcher plugin that is used to match filter expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``phedex sites order`` = <enum: SOURCE|MATCHER> (Default: SOURCE)
    Specifiy the order of the filtered list


.. _ThreadedMultiDatasetProvider:
ThreadedMultiDatasetProvider options
------------------------------------

* ``dataprovider thread max`` = <integer> (Default: 3)
    Specify the maximum number of threads used for dataset query

* ``dataprovider thread timeout`` = <duration hh[:mm[:ss]]> (Default: 00:15:00)
    Specify the timeout for the dataset query to fail


.. _DBSInfoProvider:
DBSInfoProvider options
-----------------------

* ``<prefix> guard override`` = <list of values> (Default: <taken from the selected info scanners>)
    Override the list of guard keys that are preventing files from being in the same datasets or block

* ``<prefix> hash keys`` = <list of values> (Default: '')
    Specify list of keys that are used to determine the datasets or block assigment of files

* ``<prefix> key select`` = <list of values> (Default: '')
    Specify list of dataset or block hashes that are selected for this data source

* ``<prefix> name pattern`` = <text> (Default: '')
    Specify the name pattern for the dataset or block (using variables that are common to all files in the dataset or block)

* ``discovery`` = <boolean> (Default: False)
    Toggle discovery only mode (without DBS consistency checks)

* ``scanner`` = <list of values> (Default: <depends on other configuration options>)
    Specify list of info scanner plugins to retrieve dataset informations


.. _EventBoundarySplitter:
EventBoundarySplitter options
-----------------------------

* ``<datasource> entries per job / <datasource> events per job / entries per job / events per job`` = <lookup specifier>
    Set granularity of dataset splitter

* ``<datasource> entries per job matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)


.. _FLSplitStacker:
FLSplitStacker options
----------------------

* ``<datasource> splitter stack / splitter stack`` = <list of plugins> (Default: 'BlockBoundarySplitter')
    Specify sequence of dataset splitters. All dataset splitters except for the last one have to be of type 'FileLevelSplitter', splitting only along file boundaries


.. _FileBoundarySplitter:
FileBoundarySplitter options
----------------------------

* ``<datasource> files per job / files per job`` = <lookup specifier>
    Set granularity of dataset splitter

* ``<datasource> files per job matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)


.. _HybridSplitter:
HybridSplitter options
----------------------

* ``<datasource> entries per job / <datasource> events per job / entries per job / events per job`` = <lookup specifier>
    Set guideline for the granularity of the dataset splitter

* ``<datasource> entries per job matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)


.. _RunSplitter:
RunSplitter options
-------------------

* ``<datasource> run range / run range`` = <lookup specifier> (Default: {None: 1})
    Specify number of sequential runs that are processed per job

* ``<datasource> run range matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)


.. _UserMetadataSplitter:
UserMetadataSplitter options
----------------------------

* ``split metadata`` = <lookup specifier> (Default: '')
    Specify the name of the metadata variable that is used to partition the dataset into equivalence classes

* ``split metadata matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)


.. _CompatEventHandlerManager:
CompatEventHandlerManager options
---------------------------------

* ``event handler / monitor`` = <list of values> (Default: 'scripts')
    Specify list of dual event handlers


.. _ANSIGUI:
ANSIGUI options
---------------

* ``gui element`` = <list of plugin[:name] ...> (Default: 'report activity log')
    Specify the GUI elements that form the GUI display

    List of available plugins:
     * ActivityGUIElement_ (alias: activity)
     * ReportGUIElement_ (alias: report)
     * SpanGUIElement_ (alias: span)
     * UserLogGUIElement_ (alias: log)

* ``gui element manager`` = <plugin> (Default: 'MultiGUIElement')
    Specifiy compositor class to merge the different plugins given in ``gui element``

    List of available compositor plugins:
     * MultiGUIElement_ (alias: multi)

* ``gui redraw delay`` = <float> (Default: 0.05)
    Specify the redraw delay for gui elements

* ``gui redraw interval`` = <float> (Default: 0.1)
    Specify the redraw interval for gui elements


.. _BasicConsoleGUI:
BasicConsoleGUI options
-----------------------

* ``report`` = <list of plugin[:name] ...> (Default: 'BasicTheme')
    Type of report to display during operations

    List of available plugins:
     * ANSIHeaderReport_ (alias: ansiheader)
     * ANSIReport_ (alias: ansireport)
     * ANSITheme_ (alias: ansi)
     * BackendReport_ (alias: backend)
     * BarReport_ (alias: bar)
     * BasicHeaderReport_ (alias: basicheader)
     * BasicReport_ (alias: basicreport)
     * BasicTheme_ (alias: basic)
     * ColorBarReport_ (alias: cbar)
     * LeanHeaderReport_ (alias: leanheader)
     * LeanReport_ (alias: leanreport)
     * LeanTheme_ (alias: lean)
     * LocationHistoryReport_ (alias: history)
     * LocationReport_ (alias: location)
     * MapReport_ (alias: map)
     * ModernReport_ (alias: modern)
     * ModuleReport_ (alias: module)
     * NullReport_ (alias: null)
     * PlotReport_ (alias: plot)
     * PluginReport_ (alias: plugin)
     * TimeReport_ (alias: time)
     * TrivialReport_ (alias: trivial)
     * VariablesReport_ (alias: variables, vars)

* ``report manager`` = <plugin> (Default: 'MultiReport')
    Specifiy compositor class to merge the different plugins given in ``report``

    List of available compositor plugins:
     * MultiReport_ (alias: multi)


.. _AddFilePrefix:
AddFilePrefix options
---------------------

* ``filename prefix`` = <text> (Default: '')
    Specify prefix that is prepended to the dataset file names


.. _DetermineEntries:
DetermineEntries options
------------------------

* ``entries command / events command`` = <text> (Default: '')
    Specify command that, given the file name as argument, returns with the number of entries in the file

* ``entries default / events default`` = <integer> (Default: -1)
    Specify the default number of entries in a dataset file

* ``entries key / events key`` = <text> (Default: '')
    Specify a variable from the available metadata that contains the number of entries in a dataset file

* ``entries per key value / events per key value`` = <float> (Default: 1.0)
    Specify the conversion factor between the number of entries in a dataset file and the metadata key


.. _FilesFromDataProvider:
FilesFromDataProvider options
-----------------------------

* ``source dataset path`` = <text>
    Specify path to dataset file that provides the input to the info scanner pipeline


.. _FilesFromLS:
FilesFromLS options
-------------------

* ``source directory`` = <text> (Default: '.')
    Specify source directory that is queried for dataset files

* ``source recurse`` = <boolean> (Default: False)
    Toggle recursion into directories. This is only possible for local source directories!

* ``source timeout`` = <integer> (Default: 120)
    Specify timeout for listing the source directory contents

* ``source trim local`` = <boolean> (Default: True)
    Remove file:// prefix from URLs


.. _LFNFromPath:
LFNFromPath options
-------------------

* ``lfn marker`` = <text> (Default: '/store/')
    Specifiy the string that marks the beginning of the LFN


.. _MatchDelimeter:
MatchDelimeter options
----------------------

* ``delimeter block key`` = <delimeter>:<start>:<end> (Default: '')
    Specify the the delimeter and range to derive a block key

* ``delimeter block modifier`` = <text> (Default: '')
    Specify a python expression to modify the delimeter block key - using the variable 'value'

* ``delimeter dataset key`` = <delimeter>:<start>:<end> (Default: '')
    Specify the the delimeter and range to derive a dataset key

* ``delimeter dataset modifier`` = <text> (Default: '')
    Specify a python expression to modify the delimeter dataset key - using the variable 'value'

* ``delimeter match`` = <delimeter>:<count> (Default: '')
    Specify the the delimeter and number of delimeters that have to be in the dataset file


.. _MatchOnFilename:
MatchOnFilename options
-----------------------

* ``filename filter`` = <filter option> (Default: '*.root')
    Specify filename filter to select files for the dataset

* ``filename filter matcher`` = <plugin> (Default: 'ShellStyleMatcher')
    Specifiy matcher plugin that is used to match filter expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``filename filter relative`` = <boolean> (Default: True)
    Toggle between using the absolute path or just the base path to match file names


.. _MetadataFromCMSSW:
MetadataFromCMSSW options
-------------------------

* ``include config infos`` = <boolean> (Default: False)
    Toggle the inclusion of config information in the dataset metadata


.. _MetadataFromTask:
MetadataFromTask options
------------------------

* ``ignore task vars`` = <list of values> (Default: <list of common task vars>)
    Specifiy the list of task variables that is not included in the dataset metadata


.. _ObjectsFromCMSSW:
ObjectsFromCMSSW options
------------------------

* ``include parent infos`` = <boolean> (Default: False)
    Toggle the inclusion of parentage information in the dataset metadata

* ``merge config infos`` = <boolean> (Default: True)
    Toggle the merging of config file information according to config file hashes instead of config file names


.. _OutputDirsFromConfig:
OutputDirsFromConfig options
----------------------------

* ``source config`` = <path>
    Specify source config file that contains the workflow whose output is queried for dataset files

* ``source job selector`` = <text> (Default: '')
    Specify job selector to apply to jobs in the task

* ``workflow`` = <plugin[:name]> (Default: 'Workflow:global')
    Specifies the workflow that is read from the config file

    List of available plugins:
     * Workflow


.. _OutputDirsFromWork:
OutputDirsFromWork options
--------------------------

* ``source directory`` = <path>
    Specify source directory that is queried for output directories of the task

* ``source job selector`` = <text> (Default: '')
    Specify job selector to apply to jobs in the task


.. _ParentLookup:
ParentLookup options
--------------------

* ``merge parents`` = <boolean> (Default: False)
    Toggle the merging of dataset blocks with different parent paths

* ``parent keys`` = <list of values> (Default: '')
    Specify the dataset metadata keys that contain parentage information

* ``parent match level`` = <integer> (Default: 1)
    Specify the number of path components that is used to match parent files from the parent dataset and the used parent LFN. (0 == full match)

* ``parent source`` = <text> (Default: '')
    Specify the dataset specifier from which the parent information is taken


.. _ConfigurableJobName:
ConfigurableJobName options
---------------------------

* ``job name`` = <text> (Default: '@GC_TASK_ID@.@GC_JOB_ID@')
    Specify the job name template for the job name given to the backend


.. _BlackWhiteMatcher:
BlackWhiteMatcher options
-------------------------

* ``<prefix> case sensitive`` = <boolean> (Default: True)
    Toggle case sensitivity for the matcher

* ``<prefix> mode`` = <plugin> (Default: 'start')
    Specify the matcher plugin that is used to match the subexpressions of the filter

    List of available plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)


.. _Broker:
Broker options
--------------

* ``<broker prefix> entries`` = <integer> (Default: 0)
    Specify the number of broker results to store in the job requirements (0: no limit)

* ``<broker prefix> randomize`` = <boolean> (Default: False)
    Toggle the randomization of broker results


.. _GUIElement:
GUIElement options
------------------

* ``gui height interval`` = <float> (Default: 10.0)
    Specify the interval for gui element height changes

* ``gui refresh interval`` = <float> (Default: 0.2)
    Specify the interval for gui element refresh cycles


.. _GridAccessToken:
GridAccessToken options
-----------------------

* ``ignore needed time / ignore walltime`` = <boolean> (Default: False)
    Toggle if the needed time influences the decision if the proxy allows job submission

* ``ignore warnings`` = <boolean> (Default: False)
    Toggle check for non-zero exit code from voms-proxy-info

* ``min lifetime`` = <duration hh[:mm[:ss]]> (Default: 00:05:00)
    Specify the minimal lifetime of the proxy that is required to enable job submission

* ``proxy path`` = <text> (Default: '')
    Specify the path to the proxy file that is used to check

* ``query time / min query time`` = <duration hh[:mm[:ss]]> (Default: 00:30:00)
    Specify the interval in which queries are performed

* ``urgent query time / max query time`` = <duration hh[:mm[:ss]]> (Default: 00:05:00)
    Specify the interval in which queries are performed when the time is running out


.. _AFSAccessToken:
AFSAccessToken options
----------------------

* ``access refresh`` = <duration hh[:mm[:ss]]> (Default: 01:00:00)
    Specify the lifetime threshold at which the access token is renewed

* ``ignore needed time / ignore walltime`` = <boolean> (Default: False)
    Toggle if the needed time influences the decision if the proxy allows job submission

* ``min lifetime`` = <duration hh[:mm[:ss]]> (Default: 00:05:00)
    Specify the minimal lifetime of the proxy that is required to enable job submission

* ``query time / min query time`` = <duration hh[:mm[:ss]]> (Default: 00:30:00)
    Specify the interval in which queries are performed

* ``tickets`` = <list of values> (Default: <all tickets: ''>)
    Specify the subset of kerberos tickets to check the access token lifetime

* ``urgent query time / max query time`` = <duration hh[:mm[:ss]]> (Default: 00:05:00)
    Specify the interval in which queries are performed when the time is running out


.. _CoverageBroker:
CoverageBroker options
----------------------

* ``<broker prefix>`` = <filter option> (Default: '')
    Specify broker requirement

* ``<broker prefix> plugin`` = <plugin> (Default: 'try_strict')
    Specifiy plugin that is used to filter the list

    List of available filters:
     * MediumListFilter_ (alias: try_strict)
     * StrictListFilter_ (alias: strict, require)
     * WeakListFilter_ (alias: weak, prefer)

* ``<broker prefix> matcher`` = <plugin> (Default: 'blackwhite')
    Specifiy matcher plugin that is used to match filter expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``<broker prefix> order`` = <enum: SOURCE|MATCHER> (Default: MATCHER)
    Specifiy the order of the filtered list

* ``<broker prefix> entries`` = <integer> (Default: 0)
    Specify the number of broker results to store in the job requirements (0: no limit)

* ``<broker prefix> randomize`` = <boolean> (Default: False)
    Toggle the randomization of broker results


.. _FilterBroker:
FilterBroker options
--------------------

* ``<broker prefix>`` = <filter option> (Default: '')
    Specify the filter expression to select entries given to the broker

* ``<broker prefix> plugin`` = <plugin> (Default: 'try_strict')
    Specifiy plugin that is used to filter the list

    List of available filters:
     * MediumListFilter_ (alias: try_strict)
     * StrictListFilter_ (alias: strict, require)
     * WeakListFilter_ (alias: weak, prefer)

* ``<broker prefix> matcher`` = <plugin> (Default: 'blackwhite')
    Specifiy matcher plugin that is used to match filter expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``<broker prefix> order`` = <enum: SOURCE|MATCHER> (Default: MATCHER)
    Specifiy the order of the filtered list

* ``<broker prefix> entries`` = <integer> (Default: 0)
    Specify the number of broker results to store in the job requirements (0: no limit)

* ``<broker prefix> randomize`` = <boolean> (Default: False)
    Toggle the randomization of broker results


.. _StorageBroker:
StorageBroker options
---------------------

* ``<broker prefix> entries`` = <integer> (Default: 0)
    Specify the number of broker results to store in the job requirements (0: no limit)

* ``<broker prefix> randomize`` = <boolean> (Default: False)
    Toggle the randomization of broker results

* ``<broker prefix> storage access`` = <lookup specifier> (Default: '')
    Specify the lookup dictionary that maps storage requirements into other kinds of requirements

* ``<broker prefix> storage access matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)


.. _UserBroker:
UserBroker options
------------------

* ``<broker prefix>`` = <list of values> (Default: '')
    Specify broker requirement

* ``<broker prefix> entries`` = <integer> (Default: 0)
    Specify the number of broker results to store in the job requirements (0: no limit)

* ``<broker prefix> randomize`` = <boolean> (Default: False)
    Toggle the randomization of broker results


.. _FrameGUIElement:
FrameGUIElement options
-----------------------

* ``gui dump stream`` = <boolean> (Default: True)
    Toggle dumping any buffered log streams recorded during GUI operations

* ``gui height interval`` = <float> (Default: 10.0)
    Specify the interval for gui element height changes

* ``gui refresh interval`` = <float> (Default: 0.2)
    Specify the interval for gui element refresh cycles


.. _UserLogGUIElement:
UserLogGUIElement options
-------------------------

* ``gui height interval`` = <float> (Default: 10.0)
    Specify the interval for gui element height changes

* ``gui refresh interval`` = <float> (Default: 0.2)
    Specify the interval for gui element refresh cycles

* ``log dump`` = <boolean> (Default: True)
    Toggle dump of the log history when grid-control is quitting

* ``log length`` = <integer> (Default: 200)
    Specify length of the log history

* ``log wrap`` = <boolean> (Default: True)
    Toggle wrapping of log entries


.. _ActivityGUIElement:
ActivityGUIElement options
--------------------------

* ``activity height max`` = <integer> (Default: 5)
    Specify the maximum height of the activity gui element

* ``activity height min`` = <integer> (Default: 1)
    Specify the minimal height of the activity gui element

* ``activity stream`` = <plugin> (Default: 'MultiActivityMonitor')
    Specify activity stream class that displays the current activity tree on the gui

    List of available plugins:
     * DefaultActivityMonitor_ (alias: default_stream)
     * NullOutputStream_ (alias: null)
     * SingleActivityMonitor_ (alias: single_stream)
     * TimedActivityMonitor_ (alias: timed_stream)

* ``gui height interval`` = <float> (Default: 10.0)
    Specify the interval for gui element height changes

* ``gui refresh interval`` = <float> (Default: 0.2)
    Specify the interval for gui element refresh cycles


.. _ReportGUIElement:
ReportGUIElement options
------------------------

* ``gui height interval`` = <float> (Default: 10.0)
    Specify the interval for gui element height changes

* ``gui refresh interval`` = <float> (Default: 0.2)
    Specify the interval for gui element refresh cycles

* ``report`` = <list of plugin[:name] ...> (Default: 'ANSITheme')
    Type of report to display during operations

    List of available plugins:
     * ANSIHeaderReport_ (alias: ansiheader)
     * ANSIReport_ (alias: ansireport)
     * ANSITheme_ (alias: ansi)
     * BackendReport_ (alias: backend)
     * BarReport_ (alias: bar)
     * BasicHeaderReport_ (alias: basicheader)
     * BasicReport_ (alias: basicreport)
     * BasicTheme_ (alias: basic)
     * ColorBarReport_ (alias: cbar)
     * LeanHeaderReport_ (alias: leanheader)
     * LeanReport_ (alias: leanreport)
     * LeanTheme_ (alias: lean)
     * LocationHistoryReport_ (alias: history)
     * LocationReport_ (alias: location)
     * MapReport_ (alias: map)
     * ModernReport_ (alias: modern)
     * ModuleReport_ (alias: module)
     * NullReport_ (alias: null)
     * PlotReport_ (alias: plot)
     * PluginReport_ (alias: plugin)
     * TimeReport_ (alias: time)
     * TrivialReport_ (alias: trivial)
     * VariablesReport_ (alias: variables, vars)

* ``report manager`` = <plugin> (Default: 'MultiReport')
    Specifiy compositor class to merge the different plugins given in ``report``

    List of available compositor plugins:
     * MultiReport_ (alias: multi)


.. _BasicLogEventHandler:
BasicLogEventHandler options
----------------------------

* ``event log show wms`` = <boolean> (Default: False)
    Toggle displaying the wms name during job state changes


.. _DashboardLocal:
DashboardLocal options
----------------------

* ``application`` = <text> (Default: 'shellscript')
    Specify the name of the application that is reported to dashboard

* ``dashboard timeout`` = <duration hh[:mm[:ss]]> (Default: 00:00:05)
    Specify the timeout for dashboard interactions

* ``task`` = <text> (Default: 'analysis')
    Specify the task type reported to dashboard

* ``task name`` = <text> (Default: '@GC_TASK_ID@_@DATASETNICK@')
    Specify the task name reported to dashboard


.. _JabberAlarm:
JabberAlarm options
-------------------

* ``source jid`` = <text>
    source account of the jabber messages

* ``source password file`` = <path>
    path to password file of the source account

* ``target jid`` = <text>
    target account of the jabber messages


.. _ScriptEventHandler:
ScriptEventHandler options
--------------------------

* ``on finish`` = <command or path> (Default: '')
    Specify script that is executed when grid-control is exited

* ``on finish type`` = <enum: EXECUTABLE|COMMAND> (Default: 'executable')
    Specifiy the type of command

* ``on output`` = <command or path> (Default: '')
    Specify script that is executed when the job output is retrieved

* ``on output type`` = <enum: EXECUTABLE|COMMAND> (Default: 'executable')
    Specifiy the type of command

* ``on status`` = <command or path> (Default: '')
    Specify script that is executed when the job status changes

* ``on status type`` = <enum: EXECUTABLE|COMMAND> (Default: 'executable')
    Specifiy the type of command

* ``on submit`` = <command or path> (Default: '')
    Specify script that is executed when a job is submitted

* ``on submit type`` = <enum: EXECUTABLE|COMMAND> (Default: 'executable')
    Specifiy the type of command

* ``script timeout`` = <duration hh[:mm[:ss]]> (Default: 00:00:20)
    Specify the maximal script runtime after which the script is aborted

* ``silent`` = <boolean> (Default: True)
    Do not show output of event scripts


.. _DashboardRemote:
DashboardRemote options
-----------------------

* ``application`` = <text> (Default: 'shellscript')
    Specify the name of the application that is reported to dashboard

* ``task name`` = <text> (Default: '@GC_TASK_ID@_@DATASETNICK@')
    Specify the task name reported to dashboard


.. _ColorBarReport:
ColorBarReport options
----------------------

* ``report bar show numbers`` = <boolean> (Default: False)
    Toggle displaying numeric information in the job progress bar


.. _ModernReport:
ModernReport options
--------------------

* ``report categories max`` = <integer> (Default: <20% of the console height>)
    Specify the maximum amount of categories that should be displayed


.. _TimeReport:
TimeReport options
------------------

* ``dollar per hour`` = <float> (Default: 0.013)
    Specify how much a cpu hour costs for the computing cost estimation


.. _BackendReport:
BackendReport options
---------------------

* ``report hierarchy`` = <list of values> (Default: 'wms')
    Specify the hierarchy of backend variables in the report table

* ``report history`` = <boolean> (Default: False)
    Toggle the inclusion of history job information in the report


.. _LocalSBStorageManager:
LocalSBStorageManager options
-----------------------------

* ``<storage type> path`` = <path> (Default: <call:config.get_work_path('sandbox')>)
    Specify the default transport URL(s) that are used to transfer files over this type of storage channel


.. _SEStorageManager:
SEStorageManager options
------------------------

* ``<storage channel> files`` = <list of values> (Default: '')
    Specify the files that are transferred over this storage channel

* ``<storage channel> force`` = <boolean> (Default: True)
    Specify the files that are transferred over this storage channel

* ``<storage channel> path / <storage type> path`` = <list of values> (Default: '')
    Specify the default transport URL(s) that are used to transfer files over this type of storage channel

* ``<storage channel> pattern`` = <text> (Default: '@X@')
    Specify the pattern that is used to translate local to remote file names

* ``<storage channel> timeout`` = <duration hh[:mm[:ss]]> (Default: 02:00:00)
    Specify the transfer timeout for files over this storage channel


.. _ROOTTask:
ROOTTask options
----------------

* ``executable`` = <text>
    Path to the executable

* ``wall time`` = <duration hh[:mm[:ss]]>
    Requested wall time also used for checking the proxy lifetime

* ``cpu time`` = <duration hh[:mm[:ss]]> (Default: <wall time>)
    Requested cpu time

* ``cpus`` = <integer> (Default: 1)
    Requested number of cpus per node

* ``datasource names`` = <list of values> (Default: 'dataset')
    Specify list of data sources that will be created for use in the parameter space definition

* ``depends`` = <list of values> (Default: '')
    List of environment setup scripts that the jobs depend on

* ``gzip output`` = <boolean> (Default: True)
    Toggle the compression of the job log files for stdout and stderr

* ``input files`` = <list of paths> (Default: '')
    List of files that should be transferred to the landing zone of the job on the worker node. Only for small files - send large files via SE!

* ``internal parameter factory`` = <plugin> (Default: 'BasicParameterFactory')
    Specify the parameter factory plugin that is used to generate the basic grid-control parameters

    List of available plugins:
     * BasicParameterFactory_ (alias: basic)
     * ModularParameterFactory_ (alias: modular)
     * SimpleParameterFactory_ (alias: simple)

* ``job name generator`` = <plugin> (Default: 'DefaultJobName')
    Specify the job name plugin that generates the job name that is given to the backend

    List of available plugins:
     * ConfigurableJobName_ (alias: config)
     * DefaultJobName_ (alias: default)

* ``landing zone space left`` = <integer> (Default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the landing zone directory while running

* ``landing zone space used`` = <integer> (Default: 100)
    Maximum amount of disk space (in MB) that the job is allowed to use in the landing zone directory while running

* ``memory`` = <integer> (Default: unspecified (-1))
    Requested memory in MB. Some batch farms have very low default memory limits in which case it is necessary to specify this option!

* ``node timeout`` = <duration hh[:mm[:ss]]> (Default: disabled (-1))
    Cancel job after some time on worker node

* ``output files`` = <list of values> (Default: '')
    List of files that should be transferred to the job output directory on the submission machine. Only for small files - send large files via SE!

* ``parameter adapter`` = <plugin> (Default: 'TrackedParameterAdapter')
    Specify the parameter adapter plugin that translates parameter point to job number

    List of available plugins:
     * BasicParameterAdapter_ (alias: basic)
     * TrackedParameterAdapter_ (alias: tracked)

* ``root path`` = <path> (Default: ${ROOTSYS})
    Path to the ROOT installation

* ``scratch space left`` = <integer> (Default: 1)
    Minimum amount of disk space (in MB) that the job has to leave in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply

* ``scratch space used`` = <integer> (Default: 5000)
    Maximum amount of disk space (in MB) that the job is allowed to use in the scratch directory while running. If the landing zone itself is the scratch space, the scratch thresholds apply

* ``se min size`` = <integer> (Default: -1)
    TODO: DELETE

* ``subst files`` = <list of values> (Default: '')
    List of files that will be subjected to variable substituion

* ``task date`` = <text> (Default: <current date: YYYY-MM-DD>)
    Persistent date when the task was started

* ``task id`` = <text> (Default: 'GCxxxxxxxxxxxx')
    Persistent task identifier that is generated at the start of the task

* ``task name generator`` = <plugin> (Default: 'DefaultTaskName')
    Specify the task name plugin that generates the task name that is given to the backend

    List of available plugins:
     * DefaultTaskName_ (alias: default)

* ``task time`` = <text> (Default: <current time: HHMMSS>)
    Persistent time when the task was started


.. _InactiveWMS:
InactiveWMS options
-------------------

* ``access token / proxy`` = <list of plugin[:name] ...> (Default: 'TrivialAccessToken')
    Specify access token plugins that are necessary for job submission

    List of available plugins:
     * AFSAccessToken_ (alias: afs, AFSProxy, KerberosAccessToken)
     * ARCAccessToken_ (alias: arc, arcproxy)
     * TrivialAccessToken_ (alias: trivial, TrivialProxy)
     * VomsAccessToken_ (alias: voms, VomsProxy)

* ``access token manager`` = <plugin> (Default: 'MultiAccessToken')
    Specifiy compositor class to merge the different plugins given in ``access token``

    List of available compositor plugins:
     * MultiAccessToken_ (alias: multi)

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle


.. _Local:
Local options
-------------

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``sandbox path`` = <path> (Default: <workdir>/sandbox)
    Specify the sandbox path

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle

* ``wms`` = <text> (Default: '')
    Override automatic discovery of local backend


.. _MultiWMS:
MultiWMS options
----------------

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle

* ``wms broker`` = <plugin[:name]> (Default: 'RandomBroker')
    Specify broker plugin to select the WMS for job submission.

    List of available plugins:
     * Broker


.. _Condor:
Condor options
--------------

* ``classad data / classaddata`` = <list of values> (Default: '')
    List of classAds to manually add to the job submission file

* ``email / notifyemail`` = <text> (Default: '')
    Specify the email address for job notifications

* ``jdl data / jdldata`` = <list of values> (Default: '')
    List of jdl lines to manually add to the job submission file

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``pool host list / poolhostlist`` = <list of values> (Default: '')
    Specify list of pool hosts

* ``poolargs query`` = <dictionary> (Default: {})
    Specify keys for condor pool ClassAds

* ``poolargs req`` = <dictionary> (Default: {})
    Specify keys for condor pool ClassAds

* ``remote dest`` = <text> (Default: '@')
    Specify remote destination

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``remote type`` = <enum: LOCAL|SPOOL|SSH|GSISSH> (Default: LOCAL)
    Specify the type of remote destination

* ``remote user`` = <text> (Default: '')
    Specify user at remote destination

* ``remote workdir`` = <text> (Default: '')
    Specify work directory at the remote destination

* ``sandbox path`` = <path> (Default: <workdir>/sandbox)
    Specify the sandbox path

* ``site broker`` = <plugin[:name]> (Default: 'UserBroker')
    Specify broker plugin to select the site for job submission

    List of available plugins:
     * Broker

* ``task id`` = <text> (Default: <md5 hash>)
    Persistent condor task identifier that is generated at the start of the task

* ``universe`` = <text> (Default: 'vanilla')
    Specify the name of the Condor universe

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle


.. _GridWMS:
GridWMS options
---------------

* ``ce`` = <text> (Default: '')
    Specify CE for job submission

* ``config`` = <path> (Default: '')
    Specify the config file with grid settings

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``site broker`` = <plugin[:name]> (Default: 'UserBroker')
    Specify broker plugin to select the site for job submission

    List of available plugins:
     * Broker

* ``vo`` = <text> (Default: <group from the access token>)
    Specify the VO used for job submission

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle

* ``warn sb size`` = <integer> (Default: 5)
    Warning threshold for large sandboxes (in MB)


.. _HTCondor:
HTCondor options
----------------

* ``append info`` = <list of values> (Default: '')
    List of classAds to manually add to the job submission file

* ``append opts`` = <list of values> (Default: '')
    List of jdl lines to manually add to the job submission file

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``poolconfig`` = <list of values> (Default: '')
    Specify the list of pool config files

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``sandbox path`` = <path> (Default: <workdir>/sandbox)
    Specify the sandbox path

* ``schedduri`` = <text> (Default: '')
    Specify URI of the schedd

* ``universe`` = <text> (Default: 'vanilla')
    Specify the name of the Condor universe

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle


.. _CreamWMS:
CreamWMS options
----------------

* ``ce`` = <text> (Default: '')
    Specify CE for job submission

* ``config`` = <path> (Default: '')
    Specify the config file with grid settings

* ``job chunk size`` = <integer> (Default: 10)
    Specify size of job submission chunks

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``site broker`` = <plugin[:name]> (Default: 'UserBroker')
    Specify broker plugin to select the site for job submission

    List of available plugins:
     * Broker

* ``try delegate`` = <boolean> (Default: True)
    Toggle the attempt to do proxy delegation to the WMS

* ``vo`` = <text> (Default: <group from the access token>)
    Specify the VO used for job submission

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle

* ``warn sb size`` = <integer> (Default: 5)
    Warning threshold for large sandboxes (in MB)


.. _GliteWMS:
GliteWMS options
----------------

* ``ce`` = <text> (Default: '')
    Specify CE for job submission

* ``config`` = <path> (Default: '')
    Specify the config file with grid settings

* ``discover sites`` = <boolean> (Default: False)
    Toggle the automatic discovery of matching CEs

* ``discover wms`` = <boolean> (Default: True)
    Toggle the automatic discovery of WMS endpoints

* ``force delegate`` = <boolean> (Default: False)
    Toggle the enforcement of proxy delegation to the WMS

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``site broker`` = <plugin[:name]> (Default: 'UserBroker')
    Specify broker plugin to select the site for job submission

    List of available plugins:
     * Broker

* ``try delegate`` = <boolean> (Default: True)
    Toggle the attempt to do proxy delegation to the WMS

* ``vo`` = <text> (Default: <group from the access token>)
    Specify the VO used for job submission

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle

* ``warn sb size`` = <integer> (Default: 5)
    Warning threshold for large sandboxes (in MB)

* ``wms discover full`` = <boolean> (Default: True)
    Toggle between full and lazy WMS endpoint discovery


.. _PBSGECommon:
PBSGECommon options
-------------------

* ``account`` = <text> (Default: '')
    Specify fairshare account

* ``delay output`` = <boolean> (Default: False)
    Toggle between direct output of stdout/stderr to the sandbox or indirect output to local tmp during job execution

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``memory`` = <integer> (Default: unspecified (-1))
    Requested memory in MB by the batch system

* ``queue broker`` = <plugin[:name]> (Default: 'UserBroker')
    Specify broker plugin to select the queue for job submission

    List of available plugins:
     * Broker

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``scratch path`` = <list of values> (Default: 'TMPDIR /tmp')
    Specify the list of scratch environment variables and paths to search for the scratch directory

* ``shell`` = <text> (Default: '')
    Specify the shell to use for job execution

* ``site broker`` = <plugin[:name]> (Default: 'UserBroker')
    Specify broker plugin to select the site for job submission

    List of available plugins:
     * Broker

* ``software requirement map`` = <lookup specifier> (Default: '')
    Specify a dictionary to map job requirements into submission options

* ``software requirement map matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``submit options`` = <text> (Default: '')
    Specify additional job submission options

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle


.. _GridEngine:
GridEngine options
------------------

* ``account`` = <text> (Default: '')
    Specify fairshare account

* ``delay output`` = <boolean> (Default: False)
    Toggle between direct output of stdout/stderr to the sandbox or indirect output to local tmp during job execution

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``memory`` = <integer> (Default: unspecified (-1))
    Requested memory in MB by the batch system

* ``project name`` = <text> (Default: '')
    Specify project name for batch fairshare

* ``queue broker`` = <plugin[:name]> (Default: 'UserBroker')
    Specify broker plugin to select the queue for job submission

    List of available plugins:
     * Broker

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``scratch path`` = <list of values> (Default: 'TMPDIR /tmp')
    Specify the list of scratch environment variables and paths to search for the scratch directory

* ``shell`` = <text> (Default: '')
    Specify the shell to use for job execution

* ``site broker`` = <plugin[:name]> (Default: 'UserBroker')
    Specify broker plugin to select the site for job submission

    List of available plugins:
     * Broker

* ``software requirement map`` = <lookup specifier> (Default: '')
    Specify a dictionary to map job requirements into submission options

* ``software requirement map matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``submit options`` = <text> (Default: '')
    Specify additional job submission options

* ``user`` = <text> (Default: <local user name>)
    Specify batch system user name

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle


.. _PBS:
PBS options
-----------

* ``account`` = <text> (Default: '')
    Specify fairshare account

* ``delay output`` = <boolean> (Default: False)
    Toggle between direct output of stdout/stderr to the sandbox or indirect output to local tmp during job execution

* ``job parser`` = <plugin> (Default: 'JobInfoProcessor')
    Specify plugin that checks the output sandbox of the job and returns with the job status

    List of available plugins:
     * CMSSWDebugJobInfoProcessor_ (alias: cmssw_debug)
     * DebugJobInfoProcessor_ (alias: debug)
     * FileInfoProcessor_ (alias: fileinfo)

* ``memory`` = <integer> (Default: unspecified (-1))
    Requested memory in MB by the batch system

* ``queue broker`` = <plugin[:name]> (Default: 'UserBroker')
    Specify broker plugin to select the queue for job submission

    List of available plugins:
     * Broker

* ``remote event handler / remote monitor`` = <list of plugin[:name] ...> (Default: '')
    Specify remote event handler plugins to track the task / job progress on the worker node

    List of available plugins:
     * DashboardRemote_ (alias: dashboard)

* ``remote event handler manager`` = <plugin> (Default: 'MultiRemoteEventHandler')
    Specifiy compositor class to merge the different plugins given in ``remote event handler``

    List of available compositor plugins:
     * MultiRemoteEventHandler_ (alias: multi)

* ``scratch path`` = <list of values> (Default: 'TMPDIR /tmp')
    Specify the list of scratch environment variables and paths to search for the scratch directory

* ``server`` = <text> (Default: '')
    Specify the PBS batch server

* ``shell`` = <text> (Default: '')
    Specify the shell to use for job execution

* ``site broker`` = <plugin[:name]> (Default: 'UserBroker')
    Specify broker plugin to select the site for job submission

    List of available plugins:
     * Broker

* ``software requirement map`` = <lookup specifier> (Default: '')
    Specify a dictionary to map job requirements into submission options

* ``software requirement map matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``submit options`` = <text> (Default: '')
    Specify additional job submission options

* ``wait idle`` = <integer> (Default: 60)
    Wait for the specified duration if the job cycle was idle

* ``wait work`` = <integer> (Default: 10)
    Wait for the specified duration during the work steps of the job cycle


.. _BasicParameterFactory:
BasicParameterFactory options
-----------------------------

* ``constants`` = <list of values> (Default: '')
    Specify the list of constant names that is queried for values

* ``nseeds`` = <integer> (Default: 10)
    Number of random seeds to generate

* ``parameter factory`` = <plugin> (Default: 'SimpleParameterFactory')
    Specify the parameter factory plugin that is used to generate the parameter space of the task

    List of available plugins:
     * BasicParameterFactory_ (alias: basic)
     * ModularParameterFactory_ (alias: modular)
     * SimpleParameterFactory_ (alias: simple)

* ``random variables`` = <list of values> (Default: 'JOB_RANDOM')
    Specify list of variable names that will contain random values on the worker node

* ``repeat`` = <integer> (Default: -1)
    Specify the number of jobs that each parameter space point spawns

* ``seeds`` = <list of values> (Default: Generate <nseeds> random seeds)
    Random seeds used in the job via @SEED_j@
    	@SEED_0@ = 32, 33, 34, ... for first, second, third job
    	@SEED_1@ = 51, 52, 53, ... for first, second, third job

* ``translate requirements`` = <boolean> (Default: True)
    Toggle the translation of the parameters WALLTIME, CPUTIME and MEMORY into job requirements


.. _BasicPartitionProcessor:
BasicPartitionProcessor options
-------------------------------

* ``<datasource> partition file names delimeter / partition file names delimeter`` = <text> (Default: '')
    Specify the delimeter used to concatenate the dataset file list

* ``<datasource> partition file names format / partition file names format`` = <text> (Default: '%s')
    Specify the format of the dataset files given to the job

* ``<datasource> partition variable file names / partition variable file names`` = <text> (Default: 'FILE_NAMES')
    Specify variable name containing the list of file names

* ``<datasource> partition variable max events / partition variable max events`` = <text> (Default: 'MAX_EVENTS')
    Specify variable name containing the number of events to process

* ``<datasource> partition variable prefix / partition variable prefix`` = <text> (Default: 'DATASET')
    Specify prefix for variables containing dataset information

* ``<datasource> partition variable skip events / partition variable skip events`` = <text> (Default: 'SKIP_EVENTS')
    Specify variable name containing the number of events to skip


.. _LFNPartitionProcessor:
LFNPartitionProcessor options
-----------------------------

* ``<datasource> partition lfn modifier / partition lfn modifier`` = <text> (Default: '')
    Specify a LFN prefix or prefix shortcut ('/': reduce to LFN)

* ``<datasource> partition lfn modifier dict / partition lfn modifier dict`` = <dictionary> (Default: {'<xrootd>': 'root://cms-xrd-global.cern.ch/', '<xrootd:eu>': 'root://xrootd-cms.infn.it/', '<xrootd:us>': 'root://cmsxrootd.fnal.gov/'})
    Specify a dictionary with lfn modifier shortcuts


.. _LocationPartitionProcessor:
LocationPartitionProcessor options
----------------------------------

* ``<datasource> partition location check / partition location check`` = <boolean> (Default: True)
    Toggle the deactivation of partitions without storage locations

* ``<datasource> partition location filter / partition location filter`` = <filter option> (Default: '')
    Specify filter for dataset locations

* ``<datasource> partition location filter plugin`` = <plugin> (Default: 'WeakListFilter')
    Specifiy plugin that is used to filter the list

    List of available filters:
     * MediumListFilter_ (alias: try_strict)
     * StrictListFilter_ (alias: strict, require)
     * WeakListFilter_ (alias: weak, prefer)

* ``<datasource> partition location filter matcher`` = <plugin> (Default: 'BlackWhiteMatcher')
    Specifiy matcher plugin that is used to match filter expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)

* ``<datasource> partition location filter order`` = <enum: SOURCE|MATCHER> (Default: SOURCE)
    Specifiy the order of the filtered list

* ``<datasource> partition location preference / partition location preference`` = <list of values> (Default: '')
    Specify dataset location preferences

* ``<datasource> partition location requirement / partition location requirement`` = <boolean> (Default: True)
    Add dataset location to job requirements


.. _LumiPartitionProcessor:
LumiPartitionProcessor options
------------------------------

* ``<datasource> lumi filter / lumi filter`` = <lookup specifier> (Default: '')
    Specify lumi filter for the dataset (as nickname dependent dictionary)

* ``<datasource> lumi filter matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)


.. _MetaPartitionProcessor:
MetaPartitionProcessor options
------------------------------

* ``<datasource> partition metadata / partition metadata`` = <list of values> (Default: '')
    Specify list of dataset metadata to forward to the job environment


.. _MultiPartitionProcessor:
MultiPartitionProcessor options
-------------------------------

* ``<datasource> partition processor prune / partition processor prune`` = <boolean> (Default: True)
    Toggle the removal of unused partition processors from the partition processing pipeline


.. _RequirementsPartitionProcessor:
RequirementsPartitionProcessor options
--------------------------------------

* ``<datasource> partition cputime factor / partition cputime factor`` = <float> (Default: 0.0)
    Specify how the requested cpu time scales with the number of entries in the partition

* ``<datasource> partition cputime offset / partition cputime offset`` = <float> (Default: 0.0)
    Specify the offset of the requested cpu time

* ``<datasource> partition memory factor / partition memory factor`` = <float> (Default: 0.0)
    Specify how the requested memory scales with the number of entries in the partition

* ``<datasource> partition memory offset / partition memory offset`` = <float> (Default: 0.0)
    Specify the offset of the requested memory

* ``<datasource> partition walltime factor / partition walltime factor`` = <float> (Default: 0.0)
    Specify how the requested wall time scales with the number of entries in the partition

* ``<datasource> partition walltime offset / partition walltime offset`` = <float> (Default: 0.0)
    Specify the offset of the requested wall time


.. _TFCPartitionProcessor:
TFCPartitionProcessor options
-----------------------------

* ``<datasource> partition tfc / partition tfc`` = <lookup specifier> (Default: '')
    Specify a dataset location dependent trivial file catalogue with file name prefixes

* ``<datasource> partition tfc matcher`` = <plugin> (Default: 'StartMatcher')
    Specifiy matcher plugin that is used to match the lookup expressions

    List of available matcher plugins:
     * AlwaysMatcher_ (alias: always)
     * BlackWhiteMatcher_ (alias: blackwhite)
     * EndMatcher_ (alias: end)
     * EqualMatcher_ (alias: equal)
     * ExprMatcher_ (alias: expr, eval)
     * RegExMatcher_ (alias: regex)
     * ShellStyleMatcher_ (alias: shell)
     * StartMatcher_ (alias: start)


Unused: 'nodes broker' {'disable_dupe_check': True, 'user_text': 'Specify broker plugin to select the queue for job submission', 'broker_desc': 'Specify worker nodes for job submission'}

Unused: '<name:storage_channel> path:LocalSBStorageManager' {'user_text': 'Specify the default transport URL(s) that are used to transfer files over this type of storage channel', 'default_map': {"<call:config.get_work_path('sandbox')>": '<workdir>/sandbox'}}

Unused: 'memory:LocalMemoryBroker' {'user_text': 'Requested memory in MB by the batch system', 'default_map': {'-1': 'unspecified (%(default_raw)s)'}}

Unused: '<name:datasource_name> location merge mode' {'user_text': 'Specify how the location information should be processed by the dataset block merge procedure'}

Unused: '<name:datasource_name> metadata merge mode' {'user_text': 'Specify how the metadata information should be processed by the dataset block merge procedure'}

Unused: 'enable chunk' {'user_text': 'Toggle chunked processing of jobs by the backend'}

Unused: 'submit timeout' {'user_text': 'Specify timeout of the process that is used to submit jobs'}

Unused: '<name:storage_channel> min size' {'user_text': 'output files below this file size (in MB) trigger a job failure', 'default_map': {'-1': 'disabled (%(default_raw)s)'}}

Unused: '<name:broker_prefix> broker prune' {'user_text': 'Toggle the removal of unused brokers from the broker pipeline'}

Unused: '<name:storage_channel> retry' {'user_text': 'Specify number of transfer retries'}

