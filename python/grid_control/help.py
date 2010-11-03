from python_compat import *
import os, time, random

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


	def getConfig(self, config, printDefault):
		print
		if printDefault:
			print "These are all used config options:"
		else:
			print "This is the minimal set of config options necessary:"
		print
		print ";", "="*60
		print ";", "grid-control", QM(printDefault, "complete", "reduced"), "config file"
		print ";", "="*60
		print
		for section in config.protocol:
			(header, prevNL) = (False, False)
			for (key, (value, default, volatile)) in config.protocol[section].iteritems():
				if (not printDefault and (str(value) != str(default))) or printDefault:
					if not header:
						print "[%s]" % section
						header = True
					value = str(value).replace("\n", "\n\t")
					print "%s = %s" % (str(key), str(value))
					prevNL = False
					if default != None and not printDefault:
						print "; Default setting: %s = %s\n" % (key, default)
						prevNL = True
			if header and not prevNL:
				print
