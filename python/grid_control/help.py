from python_compat import *
import os, utils

class Help(object):
	def listVars(self, module):
		print "\nIn these files:\n\t",
		print str.join(', ', map(os.path.basename, module.getSubstFiles()))
		print "the following expressions will be substituted:\n"
		print "Variable".rjust(25), ":", "Value"
		print "%s=%s" % ("=" * 26, "=" * 26)

		taskcfg = module.getTaskConfig()
		try:
			job0cfg = module.getJobConfig(0)
		except:
			job0cfg = {}
		try:
			job3cfg = module.getJobConfig(3)
		except:
			job3cfg = {}

		varList = module.getVarMapping().items() + [('RANDOM', 'RANDOM')]
		for (keyword, variable) in sorted(varList):
			print ("__%s__" % keyword).rjust(25), ":",
			if variable in taskcfg:
				print taskcfg[variable]
			elif variable in job0cfg:
				print "<example for job 0: %s>" % job0cfg[variable]
				if variable in job3cfg:
					print " "*25, " ", "<example for job 3: %s>" % job3cfg[variable]
			else:
				tmp = module.substVars("@%s@" % variable, 0, module.getTransientVars())
				if "@" not in tmp:
					print '<example: %s>' % tmp
				else:
					print '<not determinable>'
