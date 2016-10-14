# from grid_control_settings import Settings
#   This import is only needed to execute the script standalone.
#   It is not needed when running it with go.py
#   (<GCDIR>/packages needs to be in your PYTHONPATH - or grid-control was properly installed)

import sys, time
sys.stdout.write(str(time.time()) + '\n')

cfg = Settings()
cfg.Global.backend = 'Host'
cfg.workflow.task = 'UserTask'

cfg.jobs.wall_time = '1:00'
cfg.section('jobs').jobs = 2

cfg.usertask.executable = 'Example02_local.sh'
cfg.usertask.set('arguments', '0 arg1 arg2 arg3')
cfg.usertask.dataset = ['Example05_dataset.dbs', ':file:/bin/sh|3', ':file:/bin/bash|3']
cfg.usertask.set('files per job', 2)

sys.stdout.write(str(cfg) + '\n')
sys.stdout.write('=' * 20 + '\n')
