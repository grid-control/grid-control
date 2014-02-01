from python_compat import sorted
import os, utils

class Help(object):
	def listVars(self, task):
		print "\nIn these files:\n\t",
		print str.join(', ', map(os.path.basename, task.getSubstFiles()))
		print "the following expressions will be substituted:\n"
		print "Variable".rjust(25), ":", "Value"
		print "%s=%s" % ("=" * 26, "=" * 26)

		taskcfg = task.getTaskConfig()
		try:
			job0cfg = task.getJobConfig(0)
		except:
			job0cfg = {}
		try:
			job3cfg = task.getJobConfig(3)
		except:
			job3cfg = {}

		varList = task.getVarMapping().items() + [('RANDOM', 'RANDOM')]
		for (keyword, variable) in sorted(varList):
			print ("@%s@" % keyword).rjust(25), ":",
			if variable in taskcfg:
				print taskcfg[variable]
			elif variable in job0cfg:
				print "<example for job 0: %s>" % job0cfg[variable]
				if variable in job3cfg:
					print " "*25, " ", "<example for job 3: %s>" % job3cfg[variable]
			else:
				tmp = task.substVars("@%s@" % variable, 0, task.getTransientVars())
				if "@" not in tmp:
					print '<example: %s>' % tmp
				else:
					print '<not determinable>'
