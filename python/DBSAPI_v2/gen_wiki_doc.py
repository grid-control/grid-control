header="""*DBS Client API is developed in Python. The class name for api is dbsApi.*

<hr>
%TOC{title="Contents:"}%
<hr>
"""

print header
lines=open("dbsApi.py", "r").readlines()
stop = 0

for aline in lines:
	if aline.strip().startswith("def") and aline.find('self') != -1 :
		if aline.find('startElement') != -1:
			continue
		if aline.find('endElement') != -1:
			continue
		print '---++++ '+aline
	if aline.strip().startswith('"""'):
		if (stop==0): 
			print "\n<verbatim>"
			stop = 1
		else: 	
			print "</verbatim>\n"
			stop = 0
		continue
	if(stop):
		import pdb

		if aline.strip() == "" :
		        #pdb.set_trace()
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


-- Main.afaq - 07 Mar 2007
"""

print footer


