#!/usr/bin/env python

# include XML-RPC client library
# RR API uses XML-RPC webservices interface for data access
import xmlrpclib

# get handler to RR XML-RPC server
server = xmlrpclib.ServerProxy('http://pccmsdqm04.cern.ch/runregistry/xmlrpc')
#print server

# Get a list of available tables (tables) to query
tables = server.DataExporter.tableReference()
#print tables
#print

# Get a list of available workspaces
workspaces = server.DataExporter.workspaceReference()
#print workspaces
#print

# Get a list of data export format types
# parameters:
#    table    - table to query (see DataExporter.tableReference)
#for table in tables:
#  formatTypes = server.DataExporter.typeReference(table)
#  print table, formatTypes
#print

# Get a map of data fields and field types that can be queried in the selected
# workspace for the selected table (table)
# parameters:
#    table    - table to query (see DataExporter.tableReference)
#    workspace - upper-case workspace name (see DataExporter.workspaceReference)
#for table in tables:
#  for workspace in workspaces:
#    try:
#      fields = server.DataExporter.fieldReference(table, workspace)
#      print table, workspace, fields
#    except:
#      pass
#print

# Query RR run/dataset data and get results in appropriate format
# parameters:
#    table    - table to query (see DataExporter.tableReference)
#    workspace - upper-case workspace name (see DataExporter.workspaceReference)
#    format    - result format. To get the list of possible values (formats, see DataExporter.typeReference)
#    filter       - map of field + filter. To get the list of possible field
#    and types, see DataExporter.fieldReference
#    query    - advanced query (string). List of fields are available in GUI under Filter->Advanced query
#
# note: please test your queries and results in RR GUI (same filter and advanced query syntax)
#data = server.DataExporter.export('RUN', 'GLOBAL', 'xml_datasets', {'runStartTime': '>= 2009-09-01'}, '{events} > 10000')
#print data
#data = server.DataExporter.export('RUN', 'GLOBAL', 'xml_datasets', {'groupName': 'Collisions10'})
#print data
#print

# There exist short version of above function that includes only either filter,
# either advanced query, i.e.
# the same query as above example could be rewritten in filter only version data = server.DataExporter.export('RUN', 'GLOBAL', 'csv_runs', {'runStartTime': '>= 2009-09-01', 'events': '> 10000'})
# or adv. query only version
#data = server.DataExporter.export('RUN', 'GLOBAL', 'csv_runs', "{runStartTime} >= TO_DATE('2009-09-01','YYYY-MM-DD') and {events} > 10000")
#print data
#print

# Get lumi section data in CRAB JSON format for the given run range where
# Physics bit is on.
data = server.DataExporter.export('RUNLUMISECTION', 'GLOBAL', 'json', {'groupName': 'Collisions10'})
all = []
runs = eval(data)
for run in runs:
	print run
	print "------------"
	lumis = runs[run]
	lumis.sort()
	print lumis
	for i in range(len(lumis) - 1):
		if (lumis[i][1] == lumis[i+1][0] - 1):
			lumis[i][1] = lumis[i+1][1]
			lumis[i+1] = None
			lumis.sort()
	print lumis
	lumis = filter(lambda x: x, lumis)
	lumis = map(lambda x: "%d:%d-%d:%d" % (int(run), x[0], int(run), x[1]), lumis)
	lumis = str.join(", ", lumis)
	all.append(lumis)
	print lumis
	print
print
print "lumi filter ="

def lenSplit(list, maxlen):
	clen = 0
	tmp = []
	for item in list:
		if clen + len(item) < maxlen:
			tmp.append(item)
			clen += len(item)
		else:
			tmp.append('')
			yield tmp
			tmp = [item]
			clen = len(item)
	yield tmp

for line in map(lambda x: str.join(", ", x), lenSplit(all, 60)):
	print "\t", line




#print
