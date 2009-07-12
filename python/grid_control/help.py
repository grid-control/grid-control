import os, time, random

class Help(object):
	def listVars(self, module):
		print "\nIn these files:\n\t",
		print str.join(', ', map(os.path.basename, module.getSubstFiles()))
		print "\nthe following expressions will be substituted:\n"
		print "Variable".rjust(25), ":", "Value"
		print "%s=%s" % ("=" * 26, "=" * 26)

		vars = module.getVarMapping()
		vars += [('RANDOM', 'RANDOM')]
		vars.sort()
		try:
			job0cfg = module.getJobConfig(0)
		except:
			job0cfg = {}
		try:
			job3cfg = module.getJobConfig(3)
		except:
			job3cfg = {}
		for (keyword, variable) in vars:
			print ("__%s__" % keyword).rjust(25), ":",
			try:
				print module.getTaskConfig()[variable]
			except:
				try:
					print "<example for job 0: %s>" % job0cfg[variable]
				except:
					if keyword == 'DATE':
						print '<example: %s>' % time.strftime("%F")
					elif keyword == 'TIMESTAMP':
						print '<example: %s>' % time.strftime("%s")
					elif keyword == 'RANDOM':
						print '<example: %d>' % random.randrange(0, 900000000)
					else:
						print '<not yet determinable>'
				try:
					job1 = job3cfg[variable]
					print " "*25, " ", "<example for job 3: %s>" % job1
				except:
					pass


	def getConfig(self, config, printDefault):
		print
		if printDefault:
			print "These are all used config options:"
		else:
			print "This is the minimal set of config options necessary:"
		print
		print ";", "="*60
		print ";", "grid-control",
		if printDefault:
			print "complete",
		else:
			print "reduced",
		print "config file"
		print ";", "="*60
		print
		for section in config.protocol:
			header = False
			for (key, (value, default)) in config.protocol[section].iteritems():
				if (not printDefault and (str(value) != str(default))) or printDefault:
					if value == 'DEPRECATED':
						continue
					if not header:
						print "[%s]" % section
						header = True
					value = str(value).replace("\n", "\n\t")
					print "%s = %s" % (key, value)
			if header:
				print
