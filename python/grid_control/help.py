class Help(object):
	def listVars(self, module):
		print "\nIn these files:\n\t",
		print str.join(', ', module.getSubstFiles())
		print "\nthe following expressions will be substituted:\n"
		print "Variable".rjust(25), ":", "Value"
		print "%s=%s" % ("=" * 26, "=" * 26)

		vars = module.getVarMapping()
		vars += [('RANDOM', 'RANDOM')]
		vars.sort()
		job0cfg = module.getJobConfig(0)
		job3cfg = module.getJobConfig(3)
		for var in vars:
			print ("__%s__" % var[0]).rjust(25), ":",
			try:
				print module.getTaskConfig()[var[1]]
			except:
				try:
					print "<example for job 0: %s> " % job0cfg[var[1]]
				except:
					print '<not yet determinable>'
				try:
					job1 = job3cfg[var[1]]
					print " "*25, " ", "<example for job 3: %s> " % job1
				except:
					pass
