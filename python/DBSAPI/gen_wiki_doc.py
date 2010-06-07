header="""*DBS Client API is developed in Python. The class name for api is dbsApi.*

CURRENT VERSION is DBS_1_0_8

<verbatim>
  DbsApi class, provides access to DBS Server, 
  all clients must use this interface 
</verbatim>

<hr>
%TOC{title="Contents:"}%
<hr>

---++++ Previous Version(s)
[[DBS_API_DBS_1_0_1][DBS API 1_0_1 Documentation]]
[[DBS_API_DBS_1_0_5][DBS API 1_0_5 Documentation]]


"""
lines=[]
from glob import glob
import string

files=glob("dbsApi*.py")
for afile in files:
	if afile == "dbsApi.py":
		continue
	if afile == "dbsApiException.py":
                continue
	fp=open(afile, "r")
	flines=fp.readlines()
	fp.close()
	lines.extend(flines)


print header
#lines=open("dbsApi.py", "r").readlines()
#lines=open("for_doc.tmp", "r").readlines()

stop = 0

for aline in lines:
	if aline.strip().startswith('from'):
		continue
	if aline.strip().startswith('import'):
                continue
	if aline.strip().startswith("def") and aline.find('self') != -1 :
		if aline.find('startElement') != -1:
			continue
		if aline.find('endElement') != -1:
			continue
		aline=aline.strip()
		aline=aline.strip(':')
		print "---++++ def %s%s" %(string.lower(aline[aline.find('dbsApiImpl')+len('dbsApiImpl')]), \
							aline[aline.find('dbsApiImpl')+len('dbsApiImpl')+1:])
		#print '---++++ '+aline
	if aline.strip().startswith('"""'):
		if (stop==0): 
			print "\n<verbatim>"
			stop = 1
		else: 	
			print "</verbatim>\n"
			stop = 0
		continue
	if(stop):

		if aline.strip() == "" :
			continue
		if aline.endswith("\n"):
			print aline.split('\n')[0]
		else: 
			print aline
	

footer="""---++++ Methods inherited from dbsConfig.DbsConfig:
dbname(self)
dbsDB(self)
dbshome(self)
dbtype(self)
host(self)
javahome(self)
log(self)
loglevel(self)
mode(self)
password(self)
port(self)
servlet(self)
url(self)
user(self)
verbose(self)
version(self)

---++++ Data and other attributes inherited from dbsConfig.DbsConfig:

__dict__ = <dictproxy object>
    dictionary for instance variables (if defined)

__weakref__ = <attribute '__weakref__' of 'DbsConfig' objects>
    list of weak references to the object (if defined)

"""

print footer
import time
print "*Last Updated by ANZAR AFAQ %s*" %str(time.strftime("%a, %d %b %Y", time.gmtime()))

