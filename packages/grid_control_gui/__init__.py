from grid_control import GUI, Monitoring, Report

Monitoring.moduleMap['jabber'] = 'jabber.JabberAlarm'
GUI.moduleMap['ANSIConsole'] = 'ansi_console.ANSIConsole'
GUI.moduleMap['CPWebserver'] = 'webserver.CPWebserver'
Report.moduleMap['BasicBarReport'] = 'report_basic.BasicBarReport'
Report.moduleMap['GUIReport'] = 'report_gui.GUIReport'
